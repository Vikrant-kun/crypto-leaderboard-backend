package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
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

// 1. CONCURRENCY FIX: Protect our shared map with a Read-Write Mutex
var (
	activeClients   = make(map[*websocket.Conn]bool)
	clientsMu       sync.RWMutex
	upgrader        = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
)

func main() {
	ctx := context.Background()

	pgConnStr := "host=localhost port=5432 user=postgres password=local_password dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Failed to initialize Postgres driver connection: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	if err := createTableIfNotExists(db); err != nil {
		log.Fatalf("Failed to create postgres tables: %v", err)
	}
	fmt.Println("Databases connected and schema ready.")

	// HTTP Routes
	http.HandleFunc("/ws/leaderboard", HandleLeaderboardWS)
	http.HandleFunc("/api/leaderboard", func(w http.ResponseWriter, r *http.Request) {
		// Enable CORS so our frontend can read it safely
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		
		topPlayers, err := GetTopPlayers(ctx, rdb, 10)
		if err != nil {
			http.Error(w, "Failed to fetch leaderboard", http.StatusInternalServerError)
			return
		}

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
		json.NewEncoder(w).Encode(payload)
	})

	// Background Simulator
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

		for range ticker.C {
			targetPlayer := players[time.Now().UnixNano()%3]
			scoreGain := int64(40)
			
			err := updatePlayerScore(ctx, db, rdb, targetPlayer.id, targetPlayer.name, scoreGain)
			if err != nil {
				log.Printf("[ERROR] Ingestion write failure: %v", err)
				continue // Skip broadcast if writes failed
			}
			
			log.Printf("[Simulator] Added %d points to %s", scoreGain, targetPlayer.name)
			BroadcastLeaderboardUpdate(ctx, rdb)
		}
	}()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt)

	go func() {
		log.Println("Backend Server running live on http://localhost:8080")
		if err := http.ListenAndServe(":8080", nil); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server shutdown unexpectedly: %v", err)
		}
	}()

	<-shutdownChan
	fmt.Println("\nShutdown signal detected! Starting graceful drainage sequence...")

	// Thread-safe map clearing during shutdown
	clientsMu.Lock()
	for clientConn := range activeClients {
		_ = clientConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Server shutting down cleanly"))
		clientConn.Close()
	}
	clientsMu.Unlock()

	db.Close()
	rdb.Close()
	fmt.Println("Graceful shutdown sequence finished.")
}

func HandleLeaderboardWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade socket connection: %v", err)
		return
	}
	
	// Safe Write Lock execution
	clientsMu.Lock()
	activeClients[conn] = true
	clientsMu.Unlock()
	
	log.Printf("New client connected.")

	go func() {
		defer func() {
			conn.Close()
			clientsMu.Lock()
			delete(activeClients, conn)
			clientsMu.Unlock()
		}()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}

func BroadcastLeaderboardUpdate(ctx context.Context, rdb *redis.Client) {
	topPlayers, err := GetTopPlayers(ctx, rdb, 10)
	if err != nil {
		log.Printf("Error fetching ranks for broadcast: %v", err)
		return
	}

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
		return
	}

	// Safe Read Lock execution during iteration
	clientsMu.RLock()
	defer clientsMu.RUnlock()
	for clientConn := range activeClients {
		err := clientConn.WriteMessage(websocket.TextMessage, jsonBytes)
		if err != nil {
			clientConn.Close()
		}
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
		return fmt.Errorf("postgres fail: %w", err)
	}
	return rdb.ZIncrBy(ctx, "leaderboard:crypto", float64(scoreDelta), userID).Err()
}

func GetTopPlayers(ctx context.Context, rdb *redis.Client, n int64) ([]redis.Z, error) {
	return rdb.ZRevRangeWithScores(ctx, "leaderboard:crypto", 0, n-1).Result()
}