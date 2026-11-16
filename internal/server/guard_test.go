package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/health"
)

func guardedServer(token string) *Server {
	cfg := testConfig()
	cfg.APIToken = token
	return New(cfg, testLogger(), Deps{Ready: func(context.Context) map[string]health.Result { return nil }})
}

func serve(s *Server, method, path, host string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader("{}"))
	req.Host = host
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// The guard must sit in front of every real route, not just a test mux.
func TestGuardProtectsEveryRoute(t *testing.T) {
	s := guardedServer("")
	routes := [][2]string{
		{"GET", "/"}, {"GET", "/healthz"}, {"GET", "/readyz"}, {"GET", "/api/v1/info"},
		{"GET", "/api/v1/datasets"}, {"POST", "/api/v1/datasets"}, {"GET", "/api/v1/ingestions"},
		{"POST", "/api/v1/ingestions"}, {"POST", "/api/v1/queries"}, {"GET", "/api/v1/queries"},
		{"POST", "/api/v1/queries/x/cancel"}, {"GET", "/api/v1/queries/x/export"}, {"GET", "/metrics"},
	}
	for _, r := range routes {
		if rec := serve(s, r[0], r[1], "rebind.attacker.example", nil); rec.Code != 403 {
			t.Errorf("%s %s bad host: %d", r[0], r[1], rec.Code)
		}
		if rec := serve(s, r[0], r[1], "example.com", map[string]string{"Origin": "https://evil.example"}); rec.Code != 403 {
			t.Errorf("%s %s bad origin: %d", r[0], r[1], rec.Code)
		}
		if r[0] == "POST" {
			if rec := serve(s, r[0], r[1], "example.com", nil); rec.Code != 403 {
				t.Errorf("%s %s without CSRF header: %d", r[0], r[1], rec.Code)
			}
		}
	}
}

func TestGuardedRoutesStillWorkForTheDashboard(t *testing.T) {
	s := guardedServer("")
	if rec := serve(s, "GET", "/healthz", "example.com", map[string]string{"Sec-Fetch-Site": "same-origin"}); rec.Code != 200 {
		t.Fatalf("healthz: %d", rec.Code)
	}
	// With the proof header a mutation reaches the (unconfigured) handler.
	rec := serve(s, "POST", "/api/v1/queries", "example.com", map[string]string{"X-Lakehouse-CSRF": "1", "Origin": "http://example.com"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected the handler's 503, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestTokenModeRequiresBearerButKeepsLivenessOpen(t *testing.T) {
	s := guardedServer("a-long-enough-token-1")
	if rec := serve(s, "GET", "/api/v1/info", "example.com", nil); rec.Code != 401 {
		t.Fatalf("info without token: %d", rec.Code)
	}
	if rec := serve(s, "GET", "/api/v1/info", "example.com", map[string]string{"Authorization": "Bearer a-long-enough-token-1"}); rec.Code != 200 {
		t.Fatalf("info with token: %d", rec.Code)
	}
	if rec := serve(s, "GET", "/healthz", "example.com", nil); rec.Code != 200 {
		t.Fatalf("healthz: %d", rec.Code)
	}
}
