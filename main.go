package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	pgConnStr := "host=localhost port=5432 user=postgres password=local_password dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Failed to initialize Postgres driver connection: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("Could not ping Postgres database: %v", err)
	}
	fmt.Println("Successfully connected to PostgreSQL inside Docker!")

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err = rdb.Ping(pingCtx).Result(); err != nil {
		log.Fatalf("Could not ping Redis instance: %v", err)
	}
	fmt.Println("Successfully connected to Redis inside Docker!")

	if err := createTableIfNotExists(db); err != nil {
		log.Fatalf("Failed to create postgres tables: %v", err)
	}
	fmt.Println("Postgres schema is ready.")

	fmt.Println("Simulating live data ingestion...")

	err = updatePlayerScore(ctx, db, rdb, "user_01", "Alice_BTC", 150)
	if err != nil {
		log.Printf("Error updating player: %v", err)
	}

	err = updatePlayerScore(ctx, db, rdb, "user_02", "Bob_ETH", 300)
	if err != nil {
		log.Printf("Error updating player: %v", err)
	}

	err = updatePlayerScore(ctx, db, rdb, "user_01", "Alice_BTC", 200)
	if err != nil {
		log.Printf("Error updating player: %v", err)
	}

	fmt.Println("Data ingestion test completed successfully!")
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
		return fmt.Errorf("failed to update postgres: %w", err)
	}

	err = rdb.ZIncrBy(ctx, "leaderboard:crypto", float64(scoreDelta), userID).Err()
	if err != nil {
		return fmt.Errorf("failed to update redis ZSET: %w", err)
	}

	return nil
}