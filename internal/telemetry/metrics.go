package telemetry

import (
	"net/http"
	"strconv"
	"time"
)

// Closed label value sets. These are the only values that ever appear as
// label values; see TestLabelValuesAreClosedSets.
var (
	QueryOutcomes  = []string{"succeeded", "failed", "cancelled", "timed_out", "abandoned"}
	FailureReasons = []string{"engine_error", "object_store", "resource_exceeded", "query_denied", "worker_interrupted", "internal"}
	IngestOutcomes = []string{"succeeded", "failed", "abandoned"}
	JobKinds       = []string{"query", "ingest"}
	JobStates      = []string{"queued", "running"}
	Dependencies   = []string{"postgres", "minio"}
	SweepKinds     = []string{"export", "staging", "history"}
	StatusClasses  = []string{"2xx", "3xx", "4xx", "5xx"}
	HTTPRoutes     = []string{
		"unmatched", "GET /{$}", "GET /api/v1/info", "GET /healthz", "GET /readyz", "GET /metrics",
		"POST /api/v1/datasets", "GET /api/v1/datasets", "GET /api/v1/datasets/{id}",
		"GET /api/v1/datasets/{id}/schema", "GET /api/v1/datasets/{id}/preview",
		"POST /api/v1/ingestions/inspect", "POST /api/v1/ingestions", "GET /api/v1/ingestions",
		"GET /api/v1/ingestions/{id}", "POST /api/v1/ingestions/{id}/retry",
		"POST /api/v1/queries", "GET /api/v1/queries", "GET /api/v1/queries/{id}",
		"POST /api/v1/queries/{id}/cancel", "GET /api/v1/queries/{id}/results", "GET /api/v1/queries/{id}/export",
	}
)

// Metrics is the full set of service metrics. Nothing here is labelled with
// query text, object keys, dataset ids, job ids or any caller-chosen value.
type Metrics struct {
	Registry *Registry

	HTTPRequests *Counter
	HTTPDuration *Histogram

	QueryJobs         *Counter
	QueryFailures     *Counter
	QueryDuration     *Histogram
	QueryCancels      *Counter
	QueryRowsReturned *Counter

	IngestJobs     *Counter
	IngestDuration *Histogram

	QueueDepth     *Gauge
	DependencyUp   *Gauge
	WorkerUp       *Gauge
	WorkerInflight *Gauge
	WorkerSlots    *Gauge
	SweeperDeleted *Counter
	BuildInfo      *Gauge
}

var latency = []float64{0.005, 0.025, 0.1, 0.5, 1, 5, 15, 60, 300}

// New builds the metric set. version is the build version (a fixed value per
// process, so one series).
func New(version string) *Metrics {
	r := NewRegistry()
	m := &Metrics{Registry: r}
	m.HTTPRequests = r.NewCounter("lqs_http_requests_total", "HTTP requests by route pattern and status class.",
		[]string{"route", "class"}, [][]string{HTTPRoutes, StatusClasses})
	m.HTTPDuration = r.NewHistogram("lqs_http_request_duration_seconds", "HTTP request duration by route pattern.",
		latency, []string{"route"}, [][]string{HTTPRoutes})
	m.QueryJobs = r.NewCounter("lqs_query_jobs_total", "Finished query jobs by outcome.",
		[]string{"outcome"}, [][]string{QueryOutcomes})
	m.QueryFailures = r.NewCounter("lqs_query_failures_total", "Failed query jobs by failure reason.",
		[]string{"reason"}, [][]string{FailureReasons})
	m.QueryDuration = r.NewHistogram("lqs_query_duration_seconds", "Query execution duration (claim to terminal state).",
		latency, nil, nil)
	m.QueryCancels = r.NewCounter("lqs_query_cancel_requests_total", "Cancel requests accepted by the API.", nil, nil)
	m.QueryRowsReturned = r.NewCounter("lqs_query_rows_returned_total", "Rows returned by succeeded queries.", nil, nil)
	m.IngestJobs = r.NewCounter("lqs_ingest_jobs_total", "Finished ingestion jobs by outcome.",
		[]string{"outcome"}, [][]string{IngestOutcomes})
	m.IngestDuration = r.NewHistogram("lqs_ingest_duration_seconds", "Ingestion job duration.", latency, nil, nil)
	m.QueueDepth = r.NewGauge("lqs_queue_depth", "Jobs by kind and state.",
		[]string{"kind", "state"}, [][]string{JobKinds, JobStates})
	m.DependencyUp = r.NewGauge("lqs_dependency_up", "1 when the dependency's readiness probe passes.",
		[]string{"dependency"}, [][]string{Dependencies})
	m.WorkerUp = r.NewGauge("lqs_worker_up", "1 while the worker process is running.", nil, nil)
	m.WorkerInflight = r.NewGauge("lqs_worker_inflight_queries", "Queries currently executing in this worker.", nil, nil)
	m.WorkerSlots = r.NewGauge("lqs_worker_query_slots", "Configured concurrent query slots in this worker.", nil, nil)
	m.SweeperDeleted = r.NewCounter("lqs_sweeper_deleted_total", "Objects or rows removed by retention sweeps.",
		[]string{"kind"}, [][]string{SweepKinds})
	m.BuildInfo = r.NewGauge("lqs_build_info", "Build version (constant per process).",
		[]string{"version"}, [][]string{{version}})
	m.BuildInfo.Set(1, version)
	return m
}

// Handler serves the registry in the Prometheus text format. refresh, if
// non-nil, runs first to update scrape-time gauges.
func (m *Metrics) Handler(refresh func()) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if refresh != nil {
			refresh()
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.Registry.Render(w)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) {
	if s.code == 0 {
		s.code = c
	}
	s.ResponseWriter.WriteHeader(c)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Instrument records request count and latency labelled by the matched mux
// pattern (a fixed set), never by the raw path.
func (m *Metrics) Instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		code := sw.code
		if code == 0 {
			code = http.StatusOK
		}
		class := strconv.Itoa(code/100) + "xx"
		m.HTTPRequests.Inc(route, class)
		m.HTTPDuration.Observe(time.Since(start).Seconds(), route)
	})
}
