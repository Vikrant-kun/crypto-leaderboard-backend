package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

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

	pgConnStr := "host=localhost port=5432 user=postgres password=local_password dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Failed to initialize Postgres driver connection: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := createTableIfNotExists(db); err != nil {
		log.Fatalf("Failed to create postgres tables: %v", err)
	}

	fmt.Println("Updating data records...")
	_ = updatePlayerScore(ctx, db, rdb, "user_01", "Alice_BTC", 150)
	_ = updatePlayerScore(ctx, db, rdb, "user_02", "Bob_ETH", 500)
	_ = updatePlayerScore(ctx, db, rdb, "user_03", "Charlie_SOL", 250)

	fmt.Println("\n--- GLOBAL TOP RANKINGS (Redis ZRevRange) ---")
	topPlayers, err := GetTopPlayers(ctx, rdb, 3)
	if err != nil {
		log.Fatalf("Error reading top players: %v", err)
	}

	for idx, player := range topPlayers {
		fmt.Printf("Rank #%d: Player ID: %s | Total Score: %.0f pts\n", idx+1, player.Member, player.Score)
	}

	fmt.Println("\n--- INDIVIDUAL TARGET STANDING (Redis ZRevRank) ---")
	aliceDetails, err := GetSingleUserRank(ctx, rdb, "user_01")
	if err != nil {
		log.Printf("Error reading individual standing: %v", err)
	} else {
		fmt.Printf("Verification -> User: %s is Rank: #%d with score: %.0f pts\n", aliceDetails.UserID, aliceDetails.Rank, aliceDetails.Score)
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