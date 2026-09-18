package api

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// This service had no rate limiting anywhere. No local login endpoint here
// either (auth is a JWT the frontend already holds from Dex-Backend), so one
// general-purpose per-IP tier covers order placement, cancels, and the
// public window/history endpoints.
type limiterStore struct {
	mu       sync.Mutex
	limiters map[string]*rateEntry
	r        rate.Limit
	b        int
}

type rateEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newLimiterStore(r rate.Limit, b int) *limiterStore {
	s := &limiterStore{limiters: make(map[string]*rateEntry), r: r, b: b}
	go s.reapLoop()
	return s
}

func (s *limiterStore) allow(key string) bool {
	s.mu.Lock()
	entry, ok := s.limiters[key]
	if !ok {
		entry = &rateEntry{limiter: rate.NewLimiter(s.r, s.b)}
		s.limiters[key] = entry
	}
	entry.lastSeen = time.Now()
	limiter := entry.limiter
	s.mu.Unlock()
	return limiter.Allow()
}

// reapLoop bounds memory to roughly the number of distinct IPs seen in the
// last 10 minutes, not the lifetime of the process.
func (s *limiterStore) reapLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		s.mu.Lock()
		for k, e := range s.limiters {
			if e.lastSeen.Before(cutoff) {
				delete(s.limiters, k)
			}
		}
		s.mu.Unlock()
	}
}

// RateLimit wraps next with a per-client-IP token bucket: 20 req/sec
// sustained, burst of 40 — the trade ticket's fast polling during an active
// round stays well under this; a scripted order-spam loop does not. /ws is
// exempt for the same reason it is on the matching engine: a long-lived
// streaming connection is not a repeated HTTP request.
func RateLimit(next http.Handler) http.Handler {
	store := newLimiterStore(20, 40)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/prediction/ws" {
			next.ServeHTTP(w, r)
			return
		}
		key := r.RemoteAddr
		if host, _, err := net.SplitHostPort(key); err == nil {
			key = host
		}
		if !store.allow(key) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
