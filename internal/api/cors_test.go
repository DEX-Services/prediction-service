package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func passthroughHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
}

func TestCORS_EchoesAllowedOrigin(t *testing.T) {
	h := CORS("http://localhost:5173,http://localhost:8080", passthroughHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Errorf("Access-Control-Allow-Origin = %q, want the matching allowed origin", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Access-Control-Allow-Credentials = %q, want true", got)
	}
}

func TestCORS_DoesNotEchoDisallowedOrigin(t *testing.T) {
	h := CORS("http://localhost:5173", passthroughHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://evil.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty for a disallowed origin", got)
	}
}

func TestCORS_OptionsRequestShortCircuitsWithNoContent(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	h := CORS("http://localhost:5173", inner)
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204 for a preflight OPTIONS request", rec.Code)
	}
	if called {
		t.Error("CORS should short-circuit an OPTIONS preflight instead of calling the wrapped handler")
	}
}

func TestCORS_NonOptionsRequestReachesHandler(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	h := CORS("http://localhost:5173", inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Error("CORS did not call through to the wrapped handler for a normal GET request")
	}
}

func TestCORS_EmptyOriginsAllowlistAllowsNothing(t *testing.T) {
	h := CORS("", passthroughHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty when the allowlist is empty", got)
	}
}
