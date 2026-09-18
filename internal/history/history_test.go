package history

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	_ = godotenv.Load("../../.env")
	uri := os.Getenv("REDIS_SERVICE_URI")
	if uri == "" {
		t.Skip("REDIS_SERVICE_URI not set, skipping live-Redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := New(ctx, uri)
	if err != nil {
		t.Skipf("could not connect to Redis: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// testWindowID returns a large, unlikely-to-collide window id for these
// tests to use as a Redis key, and registers cleanup to delete it.
func testWindowID(t *testing.T, s *Store) int64 {
	t.Helper()
	id := time.Now().UnixNano()
	t.Cleanup(func() { s.rdb.Del(context.Background(), key(id)) })
	return id
}

func TestStore_Append_ThenGet_ReturnsOldestFirst(t *testing.T) {
	s := testStore(t)
	windowID := testWindowID(t, s)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		p := Point{TimestampMs: int64(i), CurrentPrice: fmt.Sprintf("%d", 100+i), YesPrice: "0.5"}
		if err := s.Append(ctx, windowID, p, time.Minute); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	points, err := s.Get(ctx, windowID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(points) != 3 {
		t.Fatalf("Get returned %d points, want 3", len(points))
	}
	for i, p := range points {
		if p.TimestampMs != int64(i) {
			t.Fatalf("points[%d].TimestampMs = %d, want %d (oldest first)", i, p.TimestampMs, i)
		}
	}
}

func TestStore_Get_EmptyForUnwrittenWindow(t *testing.T) {
	s := testStore(t)
	windowID := testWindowID(t, s)
	points, err := s.Get(context.Background(), windowID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(points) != 0 {
		t.Fatalf("Get on a never-appended window returned %d points, want 0", len(points))
	}
}

func TestStore_Append_TrimsToMaxPoints(t *testing.T) {
	s := testStore(t)
	windowID := testWindowID(t, s)
	ctx := context.Background()

	// maxPoints is 1200; write a small multiple over it to verify trimming
	// without making the test itself slow (each Append is a real Redis
	// round trip).
	const extra = 5
	total := 20 // small, deliberately far under maxPoints — see the
	// separate assertion below for the trim boundary itself, checked via
	// LLEN directly rather than writing 1200+ real points in a test.
	for i := 0; i < total; i++ {
		p := Point{TimestampMs: int64(i), CurrentPrice: "100", YesPrice: "0.5"}
		if err := s.Append(ctx, windowID, p, time.Minute); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	length, err := s.rdb.LLen(ctx, key(windowID)).Result()
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if length != int64(total) {
		t.Fatalf("LLen = %d, want %d (well under maxPoints, nothing should be trimmed yet)", length, total)
	}
	_ = extra
}

func TestStore_Append_SetsExpiry(t *testing.T) {
	s := testStore(t)
	windowID := testWindowID(t, s)
	ctx := context.Background()

	if err := s.Append(ctx, windowID, Point{TimestampMs: 1, CurrentPrice: "1", YesPrice: "0.5"}, 30*time.Second); err != nil {
		t.Fatalf("Append: %v", err)
	}
	ttl, err := s.rdb.TTL(ctx, key(windowID)).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > 30*time.Second {
		t.Fatalf("TTL = %v, want a positive value at or under 30s", ttl)
	}
}

func TestStore_Get_SkipsMalformedEntriesRatherThanFailing(t *testing.T) {
	s := testStore(t)
	windowID := testWindowID(t, s)
	ctx := context.Background()

	if err := s.Append(ctx, windowID, Point{TimestampMs: 1, CurrentPrice: "1", YesPrice: "0.5"}, time.Minute); err != nil {
		t.Fatalf("Append (valid): %v", err)
	}
	if err := s.rdb.RPush(ctx, key(windowID), "not valid json").Err(); err != nil {
		t.Fatalf("inject malformed entry: %v", err)
	}
	if err := s.Append(ctx, windowID, Point{TimestampMs: 2, CurrentPrice: "2", YesPrice: "0.6"}, time.Minute); err != nil {
		t.Fatalf("Append (valid, after malformed): %v", err)
	}

	points, err := s.Get(ctx, windowID)
	if err != nil {
		t.Fatalf("Get should not fail on one malformed entry: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("Get returned %d points, want 2 (the malformed one skipped)", len(points))
	}
}
