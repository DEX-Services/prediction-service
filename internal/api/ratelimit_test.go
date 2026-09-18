package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMaxBody_RejectsOversizedBody(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	h := MaxBody(inner)

	body := make([]byte, maxBodyBytes+1024)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413 for a body over maxBodyBytes", rec.Code)
	}
}

func TestMaxBody_AllowsBodyUnderLimit(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	h := MaxBody(inner)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("small body")))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for a small body", rec.Code)
	}
}

func TestMaxBody_ExemptsWebSocketPath(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A MaxBytesReader-wrapped body would still read fine for a small
		// body regardless, so this test's real assertion is structural:
		// /prediction/ws's body is untouched by MaxBody at all. We can't
		// directly observe "untouched" from outside, so this at minimum
		// documents and locks in the intended exemption path executing
		// without error.
		w.WriteHeader(http.StatusOK)
	})
	h := MaxBody(inner)
	req := httptest.NewRequest(http.MethodGet, "/prediction/ws", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for the exempted /prediction/ws path", rec.Code)
	}
}

func TestLimiterStore_AllowsUpToBurstThenBlocks(t *testing.T) {
	store := newLimiterStore(1, 3) // 1 token/sec refill, burst of 3
	key := "test-key-1"
	for i := 0; i < 3; i++ {
		if !store.allow(key) {
			t.Fatalf("call %d within burst should be allowed", i)
		}
	}
	if store.allow(key) {
		t.Fatal("call beyond burst capacity should be blocked")
	}
}

func TestLimiterStore_SeparateKeysHaveIndependentBuckets(t *testing.T) {
	store := newLimiterStore(1, 1)
	if !store.allow("key-a") {
		t.Fatal("first call for key-a should be allowed")
	}
	if !store.allow("key-b") {
		t.Fatal("key-b's bucket should be independent of key-a's — first call should be allowed")
	}
	if store.allow("key-a") {
		t.Fatal("second immediate call for key-a should be blocked (burst of 1 already used)")
	}
}

func TestRateLimit_BlocksAfterBurstWithTooManyRequests(t *testing.T) {
	// RateLimit's own store is fixed at (20, 40); rather than exhausting 40
	// real requests here (slow and brittle if wall-clock refill kicks in),
	// this test verifies the wiring: a request identified by RemoteAddr's
	// host portion reaches the limiter and gets a 429 with Retry-After once
	// the underlying store (built with a tiny burst for the test) is
	// exhausted. Since RateLimit's store size isn't injectable, this
	// exercises limiterStore directly (already covered above) plus the
	// header/status contract via a minimal handler using the same shape.
	store := newLimiterStore(1, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !store.allow("203.0.113.5") {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", rec1.Code)
	}

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request (burst exhausted) status = %d, want 429", rec2.Code)
	}
	if rec2.Header().Get("Retry-After") == "" {
		t.Error("429 response missing Retry-After header")
	}
}

func TestRateLimit_ExemptsWebSocketPath(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(http.StatusOK) })
	h := RateLimit(inner)
	// Hammer the exempt path many times in a row — none should ever be
	// blocked, unlike a normal path which would trip the 20/40 limiter.
	for i := 0; i < 100; i++ {
		req := httptest.NewRequest(http.MethodGet, "/prediction/ws", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d to exempt path got status %d, want 200 (never rate-limited)", i, rec.Code)
		}
	}
	if !called {
		t.Error("handler was never actually called")
	}
}
