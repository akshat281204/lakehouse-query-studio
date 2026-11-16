package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/config"
	"github.com/akshat281204/lakehouse-query-studio/internal/health"
)

func testConfig() *config.Config {
	return &config.Config{
		HTTPAddr:        ":0",
		ShutdownTimeout: time.Second,
		// httptest requests carry Host example.com.
		AllowedHosts:   []string{"example.com"},
		AllowedOrigins: []string{"http://example.com"},
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHealthz(t *testing.T) {
	srv := New(testConfig(), testLogger(), Deps{Ready: func(context.Context) map[string]health.Result { return nil }})

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %q, want ok", body["status"])
	}
}

func TestReadyzOK(t *testing.T) {
	ready := func(context.Context) map[string]health.Result {
		return map[string]health.Result{"postgres": {OK: true}, "minio": {OK: true}}
	}
	srv := New(testConfig(), testLogger(), Deps{Ready: ready})

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Status string                   `json:"status"`
		Deps   map[string]health.Result `json:"dependencies"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want ok", body.Status)
	}
	if len(body.Deps) != 2 {
		t.Errorf("dependencies = %d entries, want 2", len(body.Deps))
	}
}

func TestReadyzNotReady(t *testing.T) {
	ready := func(context.Context) map[string]health.Result {
		return map[string]health.Result{"postgres": {OK: false, Message: "down"}}
	}
	srv := New(testConfig(), testLogger(), Deps{Ready: ready})

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body struct {
		Status string                   `json:"status"`
		Deps   map[string]health.Result `json:"dependencies"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body.Status != "not_ready" {
		t.Errorf("status = %q, want not_ready", body.Status)
	}
	if body.Deps["postgres"].OK {
		t.Error("postgres should be reported not-ready")
	}
}

func TestRoot(t *testing.T) {
	srv := New(testConfig(), testLogger(), Deps{Ready: func(context.Context) map[string]health.Result { return nil }})

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["service"] != "lakehouse-api" {
		t.Errorf("service = %q, want lakehouse-api", body["service"])
	}
}

func TestServiceInfoRoute(t *testing.T) {
	srv := New(testConfig(), testLogger(), Deps{Ready: func(context.Context) map[string]health.Result { return nil }})

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/info", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if body["service"] != "lakehouse-api" {
		t.Errorf("service = %q, want lakehouse-api", body["service"])
	}
	if body["version"] == "" {
		t.Error("version should not be empty")
	}
}

func TestUnknownPathReturnsNotFound(t *testing.T) {
	srv := New(testConfig(), testLogger(), Deps{Ready: func(context.Context) map[string]health.Result { return nil }})

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/does-not-exist", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
