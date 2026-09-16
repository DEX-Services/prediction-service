// Package history persists each window's price/outcome ticks in Redis so a
// browser opening a round mid-way through sees the chart from the round's
// actual start, not just from the moment it connected. Redis (not Postgres)
// is deliberate: this is high-frequency (1 write/sec x 6 rounds), read-mostly
// recent data with no durability requirement — losing it on a Redis restart
// just means one round's chart rebuilds from "now" again, exactly today's
// behavior, never a correctness or balance issue. A TTL matching the round's
// own lifetime cleans keys up automatically with no cron/pruning job.
package history

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Point is one recorded tick for a window's chart.
type Point struct {
	TimestampMs  int64  `json:"t"`
	CurrentPrice string `json:"p"`
	YesPrice     string `json:"y"`
}

// Store appends/reads per-window price history in Redis.
type Store struct {
	rdb *redis.Client
}

// New connects to Redis using uri (rediss:// for TLS) — its own connection,
// separate from internal/index's Reader, since that type is scoped to
// reading the shared price feed rather than general-purpose storage.
func New(ctx context.Context, uri string) (*Store, error) {
	if uri == "" {
		return nil, fmt.Errorf("REDIS_SERVICE_URI is not set")
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("parse redis URI: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Store{rdb: rdb}, nil
}

func key(windowID int64) string {
	return fmt.Sprintf("prediction:history:%d", windowID)
}

// maxPoints caps a window's stored history — a 15-minute round at 1
// point/sec is 900 points; this leaves headroom without growing unbounded
// if a round somehow runs long.
const maxPoints = 1200

// Append records one tick and refreshes the key's TTL to ttl (the caller
// passes however long is left until the round's natural end, plus a small
// buffer for the post-lock "round closed" screen — see round.Manager).
func (s *Store) Append(ctx context.Context, windowID int64, point Point, ttl time.Duration) error {
	data, err := json.Marshal(point)
	if err != nil {
		return err
	}
	k := key(windowID)
	pipe := s.rdb.Pipeline()
	pipe.RPush(ctx, k, data)
	pipe.LTrim(ctx, k, -maxPoints, -1)
	pipe.Expire(ctx, k, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// Get returns every recorded point for a window, oldest first.
func (s *Store) Get(ctx context.Context, windowID int64) ([]Point, error) {
	raw, err := s.rdb.LRange(ctx, key(windowID), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	points := make([]Point, 0, len(raw))
	for _, item := range raw {
		var p Point
		if err := json.Unmarshal([]byte(item), &p); err != nil {
			continue // skip a malformed entry rather than fail the whole read
		}
		points = append(points, p)
	}
	return points, nil
}

// Close releases the Redis connection.
func (s *Store) Close() error { return s.rdb.Close() }
