// Package server wires the HTTP handler for the API process: liveness and
// readiness probes, a versioned service root, and graceful shutdown.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/api"
	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/config"
	"github.com/akshat281204/lakehouse-query-studio/internal/guard"
	"github.com/akshat281204/lakehouse-query-studio/internal/health"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

// Version is the reported service version. It may be overridden at build time
// via -ldflags "-X github.com/akshat281204/lakehouse-query-studio/internal/server.Version=...".
var Version = "dev"

// ReadyFunc reports the current readiness of the process dependencies. It is
// invoked on every /readyz request so dependency state is always observed
// fresh.
type ReadyFunc func(ctx context.Context) map[string]health.Result

// Deps holds the process dependencies injected into the HTTP server.
type Deps struct {
	Ready   ReadyFunc
	Catalog catalog.Store
	Objects objectstore.Store

	// Ingest and Writer enable the upload/ingestion endpoints. Both are
	// optional; when either is nil those endpoints report unavailable.
	Ingest catalog.IngestStore
	Writer objectstore.ReadWriter

	// Queries and Validator enable the query endpoints. When either is nil
	// those endpoints report unavailable. QueryLimits bounds result paging,
	// export and queue depth.
	Queries     catalog.QueryStore
	Validator   *query.Validator
	QueryLimits api.QueryLimits

	// Metrics, when set, enables /metrics and request instrumentation.
	Metrics *telemetry.Metrics
}

// Server owns the HTTP server and its routes.
type Server struct {
	cfg  *config.Config
	log  *slog.Logger
	deps Deps
	srv  *http.Server
}

// New constructs a Server.
func New(cfg *config.Config, log *slog.Logger, deps Deps) *Server {
	s := &Server{cfg: cfg, log: log, deps: deps}
	s.srv = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// Handler exposes the HTTP handler for tests.
func (s *Server) Handler() http.Handler {
	var h http.Handler = guard.Middleware(guard.Config{
		AllowedHosts:   s.cfg.AllowedHosts,
		AllowedOrigins: s.cfg.AllowedOrigins,
		Token:          s.cfg.APIToken,
		TokenExempt:    []string{"/healthz"},
	}, s.routes())
	if s.deps.Metrics != nil {
		// Outermost, so refused requests are counted too (route "unmatched").
		h = s.deps.Metrics.Instrument(h)
	}
	return h
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleRoot)
	mux.HandleFunc("GET /api/v1/info", s.handleServiceInfo)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	if s.deps.Metrics != nil {
		mux.Handle("GET /metrics", s.deps.Metrics.Handler(s.refreshMetrics))
	}

	datasets := api.New(s.deps.Catalog, s.deps.Objects,
		api.WithIngest(api.IngestDeps{Jobs: s.deps.Ingest, Objects: s.deps.Writer}),
		api.WithQueries(api.QueryDeps{Store: s.deps.Queries, Validator: s.deps.Validator,
			Objects: s.deps.Objects, Limits: s.deps.QueryLimits, Metrics: s.deps.Metrics}))
	mux.Handle("/api/v1/datasets", datasets)
	mux.Handle("/api/v1/datasets/", datasets)
	mux.Handle("/api/v1/ingestions", datasets)
	mux.Handle("/api/v1/ingestions/", datasets)
	mux.Handle("/api/v1/queries", datasets)
	mux.Handle("/api/v1/queries/", datasets)
	return mux
}

// refreshMetrics updates scrape-time gauges: dependency health from the same
// probes /readyz uses, and queue depths from the catalog. Failures leave the
// previous value and mark the dependency down; nothing is invented.
func (s *Server) refreshMetrics() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m := s.deps.Metrics
	if s.deps.Ready != nil {
		results := s.deps.Ready(ctx)
		for _, dep := range telemetry.Dependencies {
			up := 0.0
			if r, ok := results[dep]; ok && r.OK {
				up = 1
			}
			m.DependencyUp.Set(up, dep)
		}
	}
	if s.deps.Queries != nil {
		if st, err := s.deps.Queries.Stats(ctx); err == nil {
			m.QueueDepth.Set(float64(st.QueryQueued), "query", "queued")
			m.QueueDepth.Set(float64(st.QueryRunning), "query", "running")
			m.QueueDepth.Set(float64(st.IngestQueued), "ingest", "queued")
			m.QueueDepth.Set(float64(st.IngestRunning), "ingest", "running")
		}
	}
}

// ServiceInfo is the metadata payload shared by the service root and the
// versioned info endpoint.
type ServiceInfo struct {
	Service string `json:"service"`
	Version string `json:"version"`
}

func (s *Server) serviceInfo() ServiceInfo {
	return ServiceInfo{Service: "lakehouse-api", Version: Version}
}

func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.serviceInfo())
}

// handleServiceInfo serves the versioned service-info endpoint. The web app
// calls this route (rather than `/`) because both the Vite dev server and the
// nginx SPA fallback serve `index.html` at `/`.
func (s *Server) handleServiceInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.serviceInfo())
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	results := s.deps.Ready(ctx)
	body := map[string]any{
		"status":       "not_ready",
		"dependencies": results,
	}
	status := http.StatusServiceUnavailable
	if health.AllOK(results) {
		body["status"] = "ok"
		status = http.StatusOK
	}
	writeJSON(w, status, body)
}

// Run serves until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("api listening", "addr", s.cfg.HTTPAddr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		s.log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		return s.srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
