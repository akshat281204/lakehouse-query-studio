package server

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akshat281204/lakehouse-query-studio/internal/health"
	"github.com/akshat281204/lakehouse-query-studio/internal/logging"
)

func TestAccessLogHasRouteAndRequestIDButNeverPathOrQuery(t *testing.T) {
	var buf bytes.Buffer
	log, _ := logging.New(&buf, "info", "json")
	s := New(testConfig(), log, Deps{Ready: func(context.Context) map[string]health.Result { return nil }})

	req := httptest.NewRequest("GET", "/api/v1/datasets/secret-dataset-id/schema?q=sentinel-literal&token=sentinel-token", nil)
	req.Header.Set("Authorization", "Bearer sentinel-bearer")
	req.Header.Set("X-Request-Id", "client-req-12345")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	out := buf.String()
	for _, bad := range []string{"secret-dataset-id", "sentinel-literal", "sentinel-token", "sentinel-bearer", "?q="} {
		if strings.Contains(out, bad) {
			t.Fatalf("access log leaked %q: %s", bad, out)
		}
	}
	for _, want := range []string{`"request_id":"client-req-12345"`, `"route":"GET /api/v1/datasets/{id}/schema"`, `"status":`, `"duration_ms":`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in %s", want, out)
		}
	}
	if rec.Header().Get("X-Request-Id") != "client-req-12345" {
		t.Errorf("request id not echoed: %v", rec.Header())
	}
}

func TestUnsafeInboundRequestIDIsReplaced(t *testing.T) {
	var buf bytes.Buffer
	log, _ := logging.New(&buf, "info", "json")
	s := New(testConfig(), log, Deps{Ready: func(context.Context) map[string]health.Result { return nil }})
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("X-Request-Id", `abc"}{"level":"ERROR","msg":"forged`)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	id := rec.Header().Get("X-Request-Id")
	if strings.ContainsAny(id, `"{} `) || len(id) < 8 {
		t.Fatalf("unsafe id accepted: %q", id)
	}
	if strings.Contains(buf.String(), "forged") {
		t.Fatalf("log injection: %s", buf.String())
	}
}
