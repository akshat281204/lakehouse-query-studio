package telemetry

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

func render(m *Metrics) string {
	var b bytes.Buffer
	m.Registry.Render(&b)
	return b.String()
}

func TestUnknownLabelValuesCollapseToOther(t *testing.T) {
	m := New("test")
	sentinel := "SELECT secret FROM users WHERE token='sentinel-literal'"
	m.QueryJobs.Inc(sentinel)
	m.QueryJobs.Inc("succeeded")
	m.QueryFailures.Inc("s3://bucket/lake/ns/ds/version=1/part-0.parquet")
	m.HTTPRequests.Inc("/api/v1/queries/3fa85f64-5717-4562-b3fc-2c963f66afa6", "2xx")
	out := render(m)
	for _, bad := range []string{"sentinel-literal", "SELECT", "s3://", "3fa85f64", "version=1"} {
		if strings.Contains(out, bad) {
			t.Fatalf("%q leaked into metrics:\n%s", bad, out)
		}
	}
	if !strings.Contains(out, `lqs_query_jobs_total{outcome="other"} 1`) ||
		!strings.Contains(out, `lqs_query_jobs_total{outcome="succeeded"} 1`) {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestSeriesCountIsBoundedNoMatterTheInput(t *testing.T) {
	m := New("test")
	for i := 0; i < 5000; i++ {
		v := strings.Repeat("x", i%50) + string(rune('a'+i%26))
		m.QueryJobs.Inc(v)
		m.QueryFailures.Inc(v)
		m.HTTPRequests.Inc(v, v)
		m.HTTPDuration.Observe(0.1, v)
		m.QueueDepth.Set(1, v, v)
		m.DependencyUp.Set(1, v)
		m.SweeperDeleted.Inc(v)
	}
	lines := 0
	for _, l := range strings.Split(render(m), "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			lines++
		}
	}
	if lines > 200 {
		t.Fatalf("%d series after 5000 hostile label values", lines)
	}
}

// Every label value that can ever be emitted must come from a declared set.
func TestLabelValuesAreClosedSets(t *testing.T) {
	m := New("test")
	for _, v := range QueryOutcomes {
		m.QueryJobs.Inc(v)
	}
	for _, v := range FailureReasons {
		m.QueryFailures.Inc(v)
	}
	for _, r := range HTTPRoutes {
		for _, c := range StatusClasses {
			m.HTTPRequests.Inc(r, c)
		}
	}
	allowed := map[string]bool{Other: true, "test": true}
	for _, set := range [][]string{QueryOutcomes, FailureReasons, IngestOutcomes, JobKinds, JobStates,
		Dependencies, SweepKinds, StatusClasses, HTTPRoutes} {
		for _, v := range set {
			allowed[v] = true
		}
	}
	re := regexp.MustCompile(`(\w+)="([^"]*)"`)
	for _, l := range strings.Split(render(m), "\n") {
		if strings.HasPrefix(l, "#") {
			continue
		}
		for _, mm := range re.FindAllStringSubmatch(l, -1) {
			if mm[1] == "le" {
				continue
			}
			if !allowed[mm[2]] {
				t.Fatalf("label %s=%q is not in a declared set: %s", mm[1], mm[2], l)
			}
		}
	}
}

func TestExpositionFormat(t *testing.T) {
	m := New("v1")
	m.QueryJobs.Inc("succeeded")
	m.QueryDuration.Observe(0.3)
	m.QueryDuration.Observe(70)
	m.WorkerUp.Set(1)
	out := render(m)
	for _, want := range []string{
		"# TYPE lqs_query_jobs_total counter",
		"# TYPE lqs_query_duration_seconds histogram",
		`lqs_query_duration_seconds_bucket{le="0.5"} 1`,
		`lqs_query_duration_seconds_bucket{le="60"} 1`,
		`lqs_query_duration_seconds_bucket{le="300"} 2`,
		`lqs_query_duration_seconds_bucket{le="+Inf"} 2`,
		"lqs_query_duration_seconds_count 2",
		"lqs_worker_up 1",
		`lqs_build_info{version="v1"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestInstrumentUsesTheRoutePatternNotThePath(t *testing.T) {
	m := New("test")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/queries/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	h := m.Instrument(mux)
	for _, p := range []string{"/api/v1/queries/aaa", "/api/v1/queries/bbb", "/no/such/path?q=sentinel-literal"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", p, nil))
	}
	out := render(m)
	if !strings.Contains(out, `lqs_http_requests_total{route="GET /api/v1/queries/{id}",class="4xx"} 2`) {
		t.Fatalf("pattern label missing:\n%s", out)
	}
	if !strings.Contains(out, `route="unmatched"`) {
		t.Fatalf("unmatched route missing:\n%s", out)
	}
	for _, bad := range []string{"aaa", "bbb", "sentinel-literal", "/no/such"} {
		if strings.Contains(out, bad) {
			t.Fatalf("%q leaked:\n%s", bad, out)
		}
	}
}

func TestHandlerRunsRefreshAndSetsContentType(t *testing.T) {
	m := New("test")
	called := false
	rec := httptest.NewRecorder()
	m.Handler(func() { called = true; m.WorkerUp.Set(1) }).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if !called || !strings.Contains(rec.Body.String(), "lqs_worker_up 1") ||
		!strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("%v %s", rec.Header(), rec.Body.String())
	}
}
