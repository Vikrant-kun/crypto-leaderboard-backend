# Crypto Leaderboard Backend

A high-performance real-time leaderboard backend written in Go.

The application demonstrates a distributed leaderboard architecture using Redis Sorted Sets for ranking, PostgreSQL for persistent storage, Gorilla WebSocket for real-time communication, and concurrent Go routines for continuous score simulation.

The primary objective of this project is to showcase low-latency leaderboard updates using Redis while maintaining durable persistence inside PostgreSQL.

---

# Architecture

```
                          +---------------------+
                          |     Next.js UI      |
                          +----------+----------+
                                     |
                       REST + WebSocket
                                     |
                                     v
+------------------------------------------------------------+
|                    Go Backend Server                       |
|------------------------------------------------------------|
|                                                            |
|  HTTP API        WebSocket Server       Score Simulator    |
|                                                            |
|        \               |                    /              |
|         \              |                   /               |
|          +-------------+------------------+                |
|                        |                                   |
|                Leaderboard Service                         |
|                        |                                   |
+------------------------+-----------------------------------+
                         |
          +--------------+--------------+
          |                             |
          |                             |
          v                             v
   PostgreSQL                     Redis Sorted Set
Persistent Storage             Real-Time Ranking Engine
```

---

# Features

- Real-time leaderboard updates
- WebSocket streaming
- REST API for initial synchronization
- Redis Sorted Set ranking engine
- PostgreSQL persistent storage
- Concurrent score simulation
- Thread-safe WebSocket client management
- Graceful shutdown
- Automatic schema initialization
- Rank calculation using Redis ZSET

---

# Technology Stack

| Component | Technology |
|------------|------------|
| Language | Go |
| Database | PostgreSQL |
| Cache / Ranking Engine | Redis |
| WebSocket | Gorilla WebSocket |
| Driver | lib/pq |
| Redis Client | go-redis/v9 |

---

# System Design

The backend separates responsibilities into two storage systems.

## PostgreSQL

Used as the persistent source of truth.

Stores:

- User ID
- Username
- Total Score
- Updated Timestamp

Schema

```sql
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    score BIGINT NOT NULL,
    updated_at TIMESTAMP
);
```

---

## Redis

Redis is used exclusively for ranking.

The leaderboard is maintained using a Sorted Set.

```
leaderboard:crypto
```

Member

```
user_id
```

Score

```
player score
```

Operations

```
ZINCRBY
```

Updates player score.

```
ZREVRANGE WITHSCORES
```

Returns ranked leaderboard.

Redis allows leaderboard lookups in logarithmic time while avoiding expensive SQL sorting operations.

---

# Request Flow

```
               Score Update

                    │
                    ▼

          updatePlayerScore()

                    │

        +-----------+-----------+

        │                       │

        ▼                       ▼

 PostgreSQL UPDATE      Redis ZINCRBY

        │                       │

        +-----------+-----------+

                    │

                    ▼

 BroadcastLeaderboardUpdate()

                    │

                    ▼

           Connected Clients
```

---

# Real-Time Pipeline

Every three seconds the simulator

1. Selects a random player.
2. Increments the score.
3. Updates PostgreSQL.
4. Updates Redis.
5. Broadcasts leaderboard snapshot.
6. Broadcasts activity event.

```
Simulator

↓

Database Update

↓

Redis Ranking Update

↓

Leaderboard Broadcast

↓

Activity Broadcast

↓

Frontend Update
```

---

# WebSocket Protocol

Clients connect to

```
ws://localhost:8080/ws/leaderboard
```

The backend sends two message types.

---

## Leaderboard

```json
{
    "type": "leaderboard",
    "data": [
        {
            "id": "user_01",
            "username": "Alice_BTC",
            "score": 24580,
            "rank": 1
        }
    ]
}
```

---

## Activity

```json
{
    "type": "activity",
    "data": {
        "username": "Alice_BTC",
        "gain": 50,
        "time": "Just now"
    }
}
```

---

# REST API

## GET /api/leaderboard

Returns the current leaderboard.

Response

```json
[
    {
        "id": "user_01",
        "username": "Alice_BTC",
        "score": 24580,
        "rank": 1
    }
]
```

---

# Concurrency Model

The application uses Go's concurrency primitives extensively.

## Goroutines

- HTTP server
- Score simulator
- Individual WebSocket connections

---

## Mutex

Connected clients are protected using

```
sync.RWMutex
```

Read Lock

```
clientsMu.RLock()
```

Used while broadcasting.

Write Lock

```
clientsMu.Lock()
```

Used while adding or removing clients.

This prevents concurrent map access and race conditions.

---

# Project Structure

```
backend/

main.go

go.mod

go.sum
```

---

# Important Functions

## updatePlayerScore()

Responsible for

- PostgreSQL persistence
- Redis score increment

---

## GetTopPlayers()

Fetches ranked players from Redis.

Internally uses

```
ZREVRANGE WITHSCORES
```

---

## BroadcastLeaderboardUpdate()

Creates a leaderboard snapshot.

Wraps the payload into a WebSocket message.

Broadcasts to all active clients.

---

## BroadcastActivity()

Creates an activity event whenever a player's score changes.

---

## HandleLeaderboardWS()

Handles

- WebSocket upgrade
- Client registration
- Client removal
- Connection cleanup

---

# Graceful Shutdown

The backend listens for interrupt signals.

Shutdown procedure

1. Stop accepting new work.
2. Close all WebSocket clients.
3. Close PostgreSQL connection.
4. Close Redis connection.
5. Exit cleanly.

---

# Running Locally

## Clone

```bash
git clone https://github.com/<username>/crypto-leaderboard-backend.git
```

---

## Install Dependencies

```bash
go mod tidy
```

---

## Start PostgreSQL

```
localhost:5432
```

---

## Start Redis

```
localhost:6379
```

---

## Run

```bash
go run main.go
```

Server

```
http://localhost:8080
```

---

# Dependencies

```
github.com/gorilla/websocket

github.com/lib/pq

github.com/redis/go-redis/v9
```

---

# Performance Characteristics

| Operation | Complexity |
|------------|------------|
| Redis Score Update | O(log N) |
| Redis Rank Lookup | O(log N) |
| Top N Query | O(log N + M) |
| PostgreSQL Update | O(1) |
| WebSocket Broadcast | O(C) |

Where

- N = number of players
- M = requested leaderboard size
- C = connected clients

---

# Future Improvements

- Redis Pub/Sub for horizontal scaling
- Worker pools for ingestion
- Authentication
- JWT authorization
- Docker Compose deployment
- Configuration via environment variables
- Structured logging
- Metrics using Prometheus
- Health check endpoint
- Unit and integration tests
- Rate limiting
- Leaderboard pagination
- Multiple game modes
- Kubernetes deployment

---

# License

MIT License
