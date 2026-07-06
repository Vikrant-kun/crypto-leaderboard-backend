package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type PlayerRankDetails struct {
	UserID string  `json:"user_id"`
	Score  float64 `json:"score"`
	Rank   int64   `json:"rank"`
}

func main() {
	ctx := context.Background()

	// 1. Database Connections
	pgConnStr := "host=localhost port=5432 user=postgres password=local_password dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Failed to initialize Postgres driver connection: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	// 2. Initialize Schema
	if err := createTableIfNotExists(db); err != nil {
		log.Fatalf("Failed to create postgres tables: %v", err)
	}
	fmt.Println("Databases connected and schema ready.")

	// 3. Register our real-time WebSocket route
	http.HandleFunc("/ws/leaderboard", HandleLeaderboardWS)

	// 4. Background Concurrency Simulator:
	// Simulates live crypto rank variations every 3 seconds and triggers a broadcast update
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		players := []struct {
			id   string
			name string
		}{
			{"user_01", "Alice_BTC"},
			{"user_02", "Bob_ETH"},
			{"user_03", "Charlie_SOL"},
		}

		// Initial mock state populator
		_ = updatePlayerScore(ctx, db, rdb, "user_01", "Alice_BTC", 100)
		_ = updatePlayerScore(ctx, db, rdb, "user_02", "Bob_ETH", 120)
		_ = updatePlayerScore(ctx, db, rdb, "user_03", "Charlie_SOL", 110)

		fmt.Println("Live stream simulation engine started...")
		
		// Infinite loop picking a random player to receive an entry update every tick
		iteration := 0
		for range ticker.C {
			// Alternating score point adjustments to force ranking shifts
			targetPlayer := players[iteration%3]
			scoreGain := int64(40) // Static step additions
			
			err := updatePlayerScore(ctx, db, rdb, targetPlayer.id, targetPlayer.name, scoreGain)
			if err == nil {
				log.Printf("[Simulator Update] Added %d points to %s", scoreGain, targetPlayer.name)
				// Core action trigger: Broadcast latest data down the open connections!
				BroadcastLeaderboardUpdate(ctx, rdb)
			}
			iteration++
		}
	}()

	// 5. Start the API Server process
	log.Println("Backend Server running live on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server shutdown unexpectedly: %v", err)
	}
}

func createTableIfNotExists(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS users (
		user_id VARCHAR(50) PRIMARY KEY,
		username VARCHAR(50) UNIQUE NOT NULL,
		score BIGINT DEFAULT 0 NOT NULL,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := db.Exec(query)
	return err
}

func updatePlayerScore(ctx context.Context, db *sql.DB, rdb *redis.Client, userID string, username string, scoreDelta int64) error {
	pgQuery := `
		INSERT INTO users (user_id, username, score, updated_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (user_id)
		DO UPDATE SET score = users.score + EXCLUDED.score, updated_at = CURRENT_TIMESTAMP;
	`
	_, err := db.Exec(pgQuery, userID, username, scoreDelta)
	if err != nil {
		return err
	}

	return rdb.ZIncrBy(ctx, "leaderboard:crypto", float64(scoreDelta), userID).Err()
}

func GetTopPlayers(ctx context.Context, rdb *redis.Client, n int64) ([]redis.Z, error) {
	return rdb.ZRevRangeWithScores(ctx, "leaderboard:crypto", 0, n-1).Result()
}

func GetSingleUserRank(ctx context.Context, rdb *redis.Client, userID string) (*PlayerRankDetails, error) {
	zeroIndexedRank, err := rdb.ZRevRank(ctx, "leaderboard:crypto", userID).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("player %s not found on the leaderboard", userID)
		}
		return nil, fmt.Errorf("failed to fetch user rank: %w", err)
	}

	score, err := rdb.ZScore(ctx, "leaderboard:crypto", userID).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user score: %w", err)
	}

	return &PlayerRankDetails{
		UserID: userID,
		Score:  score,
		Rank:   zeroIndexedRank + 1,
	}, nil
}

// Upgrader configurations to elevate incoming standard HTTP requests into WebSocket connections
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// For local development testing, we allow all origins. 
	// In production, you would restrict this to your specific frontend URL.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Client represents a single active connected frontend user socket
type Client struct {
	Conn *websocket.Conn
}

// Global active client registry mapping connection pointers
var activeClients = make(map[*websocket.Conn]bool)

// HandleLeaderboardWS upgrades connections and registers active listeners
func HandleLeaderboardWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade socket connection: %v", err)
		return
	}
	
	// Register client connection to map
	activeClients[conn] = true
	log.Printf("New frontend client connected. Active clients: %d", len(activeClients))

	// Keep connection alive and listen for disconnects
	go func() {
		defer func() {
			conn.Close()
			delete(activeClients, conn)
			log.Printf("Client disconnected. Active clients: %d", len(activeClients))
		}()

		for {
			// We only expect server-to-client streaming, but we must read incoming control
			// frames (like PING/PONG or close signals) to know if the client disconnected.
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}

// BroadcastLeaderboardUpdate pulls the latest ranks from Redis and pushes them to all open sockets
func BroadcastLeaderboardUpdate(ctx context.Context, rdb *redis.Client) {
	// 1. Fetch top 3 players from Redis cache
	topPlayers, err := GetTopPlayers(ctx, rdb, 3)
	if err != nil {
		log.Printf("Error fetching ranks for broadcast: %v", err)
		return
	}

	// 2. Format data payload into clean human-readable JSON structures matching our frontend requirements
	type UIPlayer struct {
		ID    string  `json:"id"`
		Score float64 `json:"score"`
		Rank  int     `json:"rank"`
	}

	var payload []UIPlayer
	for idx, p := range topPlayers {
		payload = append(payload, UIPlayer{
			ID:    fmt.Sprintf("%v", p.Member),
			Score: p.Score,
			Rank:  idx + 1,
		})
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to serialize broadcast payload: %v", err)
		return
	}

	// 3. Loop over all registered clients and push the data down the socket
	for clientConn := range activeClients {
		err := clientConn.WriteMessage(websocket.TextMessage, jsonBytes)
		if err != nil {
			log.Printf("Failed sending message to client, dropping connection: %v", err)
			clientConn.Close()
			delete(activeClients, clientConn)
		}
	}
}