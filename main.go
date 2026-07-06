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

	err = db.Ping()
	if err != nil {
		log.Fatalf("Could not ping Postgres database: %v", err)
	}
	fmt.Println("Successfully connected to PostgreSQL inside Docker!")

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", 
		DB:       0,  
	})
	defer rdb.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err = rdb.Ping(pingCtx).Result()
	if err != nil {
		log.Fatalf("Could not ping Redis instance: %v", err)
	}
	fmt.Println("Successfully connected to Redis inside Docker!")
}