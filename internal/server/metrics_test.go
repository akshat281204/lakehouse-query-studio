package server

import (
	"context"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/health"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

func metricsServer() (*Server, *catalog.MemoryQueryStore) {
	store := catalog.NewMemoryQueryStore()
	store.IngestQueued = 3
	s := New(testConfig(), testLogger(), Deps{
		Metrics: telemetry.New("test"),
		Queries: store,
		Ready: func(context.Context) map[string]health.Result {
			return map[string]health.Result{"postgres": {OK: true}, "minio": {OK: false, Message: "down"}}
		},
	})
	return s, store
}

func TestMetricsEndpointExposesQueueDepthAndDependencies(t *testing.T) {
	s, store := metricsServer()
	id, _ := catalog.NewUUID()
	_, _ = store.Submit(context.Background(), catalog.SubmitQuery{ID: id, Statement: "SELECT 'sentinel-literal'", StatementHash: "h"})

	rec := serve(s, "GET", "/metrics", "example.com", nil)
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("%d %v", rec.Code, rec.Header())
	}
	body := rec.Body.String()
	for _, want := range []string{
		`lqs_queue_depth{kind="query",state="queued"} 1`,
		`lqs_queue_depth{kind="ingest",state="queued"} 3`,
		`lqs_dependency_up{dependency="postgres"} 1`,
		`lqs_dependency_up{dependency="minio"} 0`,
		`lqs_build_info{version="test"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	if strings.Contains(body, "sentinel-literal") || strings.Contains(body, id) {
		t.Fatalf("metrics leaked statement text or a job id:\n%s", body)
	}
}

func TestMetricsRequireTheGuard(t *testing.T) {
	s, _ := metricsServer()
	if rec := serve(s, "GET", "/metrics", "rebind.attacker.example", nil); rec.Code != 403 {
		t.Fatalf("bad host: %d", rec.Code)
	}
	cfg := testConfig()
	cfg.APIToken = "a-long-enough-token-1"
	tok := New(cfg, testLogger(), Deps{Metrics: telemetry.New("t")})
	if rec := serve(tok, "GET", "/metrics", "example.com", nil); rec.Code != 401 {
		t.Fatalf("no token: %d", rec.Code)
	}
}

// Every route the server registers must map to a declared route label, so no
// real traffic is ever recorded under "other".
func TestEveryRegisteredRouteHasADeclaredLabel(t *testing.T) {
	s, _ := metricsServer()
	paths := [][2]string{
		{"GET", "/"}, {"GET", "/healthz"}, {"GET", "/readyz"}, {"GET", "/metrics"}, {"GET", "/api/v1/info"},
		{"GET", "/api/v1/datasets"}, {"GET", "/api/v1/datasets/abc"}, {"GET", "/api/v1/datasets/abc/schema"},
		{"GET", "/api/v1/datasets/abc/preview"}, {"POST", "/api/v1/datasets"},
		{"GET", "/api/v1/ingestions"}, {"GET", "/api/v1/ingestions/abc"}, {"POST", "/api/v1/ingestions"},
		{"POST", "/api/v1/ingestions/inspect"}, {"POST", "/api/v1/ingestions/abc/retry"},
		{"GET", "/api/v1/queries"}, {"POST", "/api/v1/queries"}, {"GET", "/api/v1/queries/abc"},
		{"POST", "/api/v1/queries/abc/cancel"}, {"GET", "/api/v1/queries/abc/results"},
		{"GET", "/api/v1/queries/abc/export"}, {"GET", "/no/such/path/with-secret-sentinel"},
	}
	for _, p := range paths {
		serve(s, p[0], p[1], "example.com", map[string]string{"X-Lakehouse-CSRF": "1"})
	}
	body := serve(s, "GET", "/metrics", "example.com", nil).Body.String()
	if strings.Contains(body, `="other"`) {
		t.Fatalf("a registered route fell outside the declared label set:\n%s", body)
	}
	if strings.Contains(body, "sentinel") || strings.Contains(body, "/abc") {
		t.Fatalf("raw path leaked into a label:\n%s", body)
	}
}
