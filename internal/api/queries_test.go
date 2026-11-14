package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

type queryEnv struct {
	t       *testing.T
	h       http.Handler
	store   *catalog.MemoryQueryStore
	objects *objectstore.Memory
	runner  *query.Runner
	limits  QueryLimits
}

func newQueryEnv(t *testing.T, limits *QueryLimits, rows [][]any) *queryEnv {
	t.Helper()
	store := catalog.NewMemoryQueryStore()
	objects := objectstore.NewMemory()
	if rows == nil {
		rows = [][]any{{int64(1), "a", 1.5}, {int64(2), nil, 2.5}, {int64(3), "c", 3.5}}
	}
	data, err := parquet.Encode([]parquet.WriteColumn{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString, Nullable: true},
		{Name: "score", Type: parquet.TypeDouble},
	}, rows)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "lake/default/sales/version=1/attempt=1/"
	_ = objects.Put(context.Background(), prefix+"part-1.parquet", data)
	store.AddRelation(catalog.Relation{DatasetName: "sales", Version: 1, ObjectPrefix: prefix, Format: "parquet"})

	v, err := query.NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	lim := DefaultQueryLimits()
	if limits != nil {
		lim = *limits
	}
	cfg := query.DefaultRunnerConfig()
	cfg.TmpDir = t.TempDir()
	cfg.Limits.Timeout = 20 * time.Second
	runner, err := query.NewRunner(store, objects, query.NewDuckDB(), v, "test", cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	h := New(nil, objects, WithQueries(QueryDeps{Store: store, Validator: v, Objects: objects, Limits: lim}))
	return &queryEnv{t: t, h: h, store: store, objects: objects, runner: runner, limits: lim}
}

func (e *queryEnv) do(method, path, body string) *httptest.ResponseRecorder {
	e.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func (e *queryEnv) submit(stmt string) (catalog.QueryJob, *httptest.ResponseRecorder) {
	e.t.Helper()
	b, _ := json.Marshal(map[string]string{"statement": stmt})
	rec := e.do("POST", "/api/v1/queries", string(b))
	var job catalog.QueryJob
	if rec.Code == http.StatusAccepted {
		if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
			e.t.Fatal(err)
		}
	}
	return job, rec
}

func (e *queryEnv) runAll() {
	e.t.Helper()
	for {
		ran, err := e.runner.RunOnce(context.Background())
		if err != nil {
			e.t.Fatal(err)
		}
		if !ran {
			return
		}
	}
}

func (e *queryEnv) status(id string) catalog.QueryJob {
	e.t.Helper()
	rec := e.do("GET", "/api/v1/queries/"+id, "")
	if rec.Code != 200 {
		e.t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var j catalog.QueryJob
	_ = json.Unmarshal(rec.Body.Bytes(), &j)
	return j
}

func qErrCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var b errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("not an error envelope: %s", rec.Body.String())
	}
	return b.Error.Code
}

func TestQuerySubmitPollPageExport(t *testing.T) {
	e := newQueryEnv(t, nil, nil)
	job, rec := e.submit("SELECT id, label, score FROM sales ORDER BY id")
	if rec.Code != 202 || job.Status != catalog.QueryQueued || rec.Header().Get("Location") == "" {
		t.Fatalf("submit: %d %s", rec.Code, rec.Body.String())
	}
	if e.do("GET", "/api/v1/queries/"+job.ID+"/results", "").Code != 409 {
		t.Fatal("results of a queued query must be 409")
	}
	e.runAll()
	got := e.status(job.ID)
	if got.Status != catalog.QuerySucceeded || got.Statement == "" {
		t.Fatalf("%+v", got)
	}

	rec = e.do("GET", "/api/v1/queries/"+job.ID+"/results?limit=2", "")
	if rec.Code != 200 {
		t.Fatalf("results: %d %s", rec.Code, rec.Body.String())
	}
	var page resultsResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if len(page.Columns) != 3 || page.Columns[0].Name != "id" || page.Returned != 2 || page.NextOffset == nil || *page.NextOffset != 2 || page.TotalRows != 3 {
		t.Fatalf("page 1: %+v", page)
	}
	rec = e.do("GET", "/api/v1/queries/"+job.ID+"/results?offset=2&limit=2", "")
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if page.Returned != 1 || page.NextOffset != nil || page.Rows[0][0] != float64(3) {
		t.Fatalf("page 2: %+v", page)
	}

	rec = e.do("GET", "/api/v1/queries/"+job.ID+"/export", "")
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/csv") ||
		!strings.Contains(rec.Header().Get("Content-Disposition"), "attachment") || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("export headers: %d %v", rec.Code, rec.Header())
	}
	if want := "id,label,score\n1,a,1.5\n2,,2.5\n3,c,3.5\n"; rec.Body.String() != want {
		t.Fatalf("csv = %q want %q", rec.Body.String(), want)
	}

	rec = e.do("GET", "/api/v1/queries?limit=5", "")
	var hist queryListResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &hist)
	if len(hist.Queries) != 1 || hist.Queries[0].ID != job.ID {
		t.Fatalf("history: %s", rec.Body.String())
	}
}

func TestQuerySubmitDeniesUnsafeSQLAndCreatesNoJob(t *testing.T) {
	e := newQueryEnv(t, nil, nil)
	for _, stmt := range []string{
		"", "DROP TABLE sales", "SELECT 1; SELECT 2", "SELECT * FROM read_csv('/etc/passwd')",
		"SELECT * FROM read_parquet('s3://lakehouse/uploads/x.parquet')", "COPY sales TO '/tmp/x'",
		"ATTACH '/tmp/x.db'", "INSTALL httpfs", "SELECT gen_random_uuid()", "SELECT * FROM information_schema.tables",
		"SELECT * FROM does_not_exist",
	} {
		_, rec := e.submit(stmt)
		if rec.Code != 400 || qErrCode(t, rec) != "query_denied" {
			t.Errorf("%q: %d %s", stmt, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "passwd") || strings.Contains(rec.Body.String(), "uploads/x") {
			t.Errorf("%q: response echoes statement text: %s", stmt, rec.Body.String())
		}
	}
	var hist queryListResponse
	_ = json.Unmarshal(e.do("GET", "/api/v1/queries", "").Body.Bytes(), &hist)
	if len(hist.Queries) != 0 {
		t.Fatalf("denied statements created %d jobs", len(hist.Queries))
	}
}

func TestQuerySubmitRejectsMalformedBodies(t *testing.T) {
	e := newQueryEnv(t, nil, nil)
	for name, body := range map[string]string{
		"not json":      "SELECT 1",
		"unknown field": `{"statement":"SELECT 1","extra":1}`,
		"wrong type":    `{"statement":5}`,
	} {
		rec := e.do("POST", "/api/v1/queries", body)
		if rec.Code != 400 || qErrCode(t, rec) != "invalid_argument" {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	huge := `{"statement":"SELECT '` + strings.Repeat("x", 3*query.MaxSQLBytes) + `'"}`
	if rec := e.do("POST", "/api/v1/queries", huge); rec.Code != 400 || qErrCode(t, rec) != "query_denied" {
		t.Errorf("huge body: %d", rec.Code)
	}
}

func TestQueryQueueDepthLimit(t *testing.T) {
	lim := DefaultQueryLimits()
	lim.MaxQueued = 2
	e := newQueryEnv(t, &lim, nil)
	for i := 0; i < 2; i++ {
		if _, rec := e.submit("SELECT 1"); rec.Code != 202 {
			t.Fatalf("submit %d: %d", i, rec.Code)
		}
	}
	if _, rec := e.submit("SELECT 1"); rec.Code != 429 || qErrCode(t, rec) != "resource_exhausted" {
		t.Fatalf("third submit: %d %s", rec.Code, rec.Body.String())
	}
}

func TestQueryCancelAndErrors(t *testing.T) {
	e := newQueryEnv(t, nil, nil)
	job, _ := e.submit("SELECT * FROM sales")
	rec := e.do("POST", "/api/v1/queries/"+job.ID+"/cancel", "")
	if rec.Code != 202 {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body.String())
	}
	if got := e.status(job.ID); got.Status != catalog.QueryCancelled {
		t.Fatalf("status %s", got.Status)
	}
	if rec := e.do("POST", "/api/v1/queries/"+job.ID+"/cancel", ""); rec.Code != 409 || qErrCode(t, rec) != "invalid_state" {
		t.Fatalf("second cancel: %d %s", rec.Code, rec.Body.String())
	}
	if rec := e.do("GET", "/api/v1/queries/"+job.ID+"/results", ""); rec.Code != 409 {
		t.Fatalf("results of cancelled: %d", rec.Code)
	}
	if rec := e.do("GET", "/api/v1/queries/"+job.ID+"/export", ""); rec.Code != 409 {
		t.Fatalf("export of cancelled: %d", rec.Code)
	}
	for _, id := range []string{"00000000-0000-4000-8000-000000000000", "not-a-uuid"} {
		if rec := e.do("GET", "/api/v1/queries/"+id, ""); rec.Code != 404 {
			t.Errorf("get %s: %d", id, rec.Code)
		}
		if rec := e.do("POST", "/api/v1/queries/"+id+"/cancel", ""); rec.Code != 404 {
			t.Errorf("cancel %s: %d", id, rec.Code)
		}
	}
}

func TestQueryFailedHasNoResults(t *testing.T) {
	e := newQueryEnv(t, nil, nil)
	job, _ := e.submit("SELECT nope FROM sales")
	e.runAll()
	if got := e.status(job.ID); got.Status != catalog.QueryFailed || got.FailureReason == "" {
		t.Fatalf("%+v", got)
	}
	if rec := e.do("GET", "/api/v1/queries/"+job.ID+"/results", ""); rec.Code != 409 {
		t.Fatalf("results of failed: %d", rec.Code)
	}
}

func TestQueryPagingParameterBounds(t *testing.T) {
	e := newQueryEnv(t, nil, nil)
	job, _ := e.submit("SELECT id FROM sales")
	e.runAll()
	for _, q := range []string{"limit=0", "limit=10001", "limit=x", "offset=-1", "offset=abc"} {
		if rec := e.do("GET", "/api/v1/queries/"+job.ID+"/results?"+q, ""); rec.Code != 400 {
			t.Errorf("%s: %d", q, rec.Code)
		}
	}
	if rec := e.do("GET", "/api/v1/queries?limit=101", ""); rec.Code != 400 {
		t.Errorf("history limit: %d", rec.Code)
	}
	rec := e.do("GET", "/api/v1/queries/"+job.ID+"/results?offset=50", "")
	var page resultsResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if rec.Code != 200 || page.Returned != 0 || page.NextOffset != nil {
		t.Fatalf("offset past end: %d %s", rec.Code, rec.Body.String())
	}
}

func manyRows(n int, size int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int64(i), strings.Repeat("z", size), float64(i)}
	}
	return rows
}

func TestQueryPageIsByteBoundedAtWholeRows(t *testing.T) {
	lim := DefaultQueryLimits()
	lim.MaxPageBytes = 1000
	e := newQueryEnv(t, &lim, manyRows(20, 300))
	job, _ := e.submit("SELECT id, label FROM sales ORDER BY id")
	e.runAll()

	seen := int64(0)
	offset := 0
	for pages := 0; pages < 50; pages++ {
		rec := e.do("GET", "/api/v1/queries/"+job.ID+"/results?limit=1000&offset="+itoa(offset), "")
		if rec.Code != 200 {
			t.Fatalf("page: %d %s", rec.Code, rec.Body.String())
		}
		var page resultsResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &page)
		if page.Returned == 0 && page.NextOffset != nil {
			t.Fatal("empty page while rows remain")
		}
		b, _ := json.Marshal(page.Rows)
		if int64(len(b)) > lim.MaxPageBytes {
			t.Fatalf("page rows are %d bytes, limit %d", len(b), lim.MaxPageBytes)
		}
		for i, r := range page.Rows {
			if r[0] != float64(offset+i) {
				t.Fatalf("cursor skipped or split a row: got id %v at %d", r[0], offset+i)
			}
		}
		seen += int64(page.Returned)
		if page.NextOffset == nil {
			break
		}
		offset = *page.NextOffset
	}
	if seen != 20 {
		t.Fatalf("paged %d rows, want 20", seen)
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestQueryFirstRowTooLargeForPageOrExport(t *testing.T) {
	lim := DefaultQueryLimits()
	lim.MaxPageBytes = 200
	lim.MaxExportBytes = 200
	e := newQueryEnv(t, &lim, manyRows(3, 500))
	job, _ := e.submit("SELECT id, label FROM sales ORDER BY id")
	e.runAll()
	for _, p := range []string{"/results", "/export"} {
		rec := e.do("GET", "/api/v1/queries/"+job.ID+p, "")
		if rec.Code != 429 || qErrCode(t, rec) != "resource_exhausted" {
			t.Errorf("%s: %d %s", p, rec.Code, rec.Body.String())
		}
	}
}

func TestQueryExportStopsAtWholeRowByteBound(t *testing.T) {
	lim := DefaultQueryLimits()
	lim.MaxExportBytes = 700
	e := newQueryEnv(t, &lim, manyRows(20, 100))
	job, _ := e.submit("SELECT id, label FROM sales ORDER BY id")
	e.runAll()
	rec := e.do("GET", "/api/v1/queries/"+job.ID+"/export", "")
	if rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if int64(len(body)) > lim.MaxExportBytes {
		t.Fatalf("export is %d bytes, limit %d", len(body), lim.MaxExportBytes)
	}
	if rec.Header().Get("X-Export-Truncated") != "true" || !strings.HasSuffix(body, "\n") {
		t.Fatalf("truncation header/last row: %v %q", rec.Header(), body[len(body)-20:])
	}
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	if len(lines) < 2 {
		t.Fatal("export must include at least one row")
	}
	for _, l := range lines[1:] {
		if !strings.HasSuffix(l, strings.Repeat("z", 100)) {
			t.Fatalf("row cut mid-cell: %q", l)
		}
	}
}

func TestQueryExportNeutralizesSpreadsheetFormulas(t *testing.T) {
	e := newQueryEnv(t, nil, [][]any{{int64(1), "=HYPERLINK(\"http://x\")", 1.0}, {int64(2), "+cmd", 2.0}, {int64(3), "-5 apples", 3.0}, {int64(4), "@sum", 4.0}})
	job, _ := e.submit("SELECT id, label FROM sales ORDER BY id")
	e.runAll()
	body := e.do("GET", "/api/v1/queries/"+job.ID+"/export", "").Body.String()
	for _, bad := range []string{",=HYPERLINK", ",+cmd", ",-5 apples", ",@sum"} {
		if strings.Contains(body, bad) {
			t.Errorf("unneutralized formula %q in %q", bad, body)
		}
	}
	if !strings.Contains(body, "'=HYPERLINK") || !strings.Contains(body, "'+cmd") {
		t.Errorf("expected quote-prefixed cells: %q", body)
	}
}

func TestQueryEncodingsAreBoundedIndependently(t *testing.T) {
	// A control character costs 6 bytes in JSON (\u0001) but 1 in CSV, so the
	// page limit can bind while the export succeeds: bounds are per encoding.
	lim := DefaultQueryLimits()
	lim.MaxPageBytes = 400
	lim.MaxExportBytes = 400
	row := []any{int64(1), strings.Repeat("\x01", 120), 1.0}
	e := newQueryEnv(t, &lim, [][]any{row})
	job, _ := e.submit("SELECT id, label FROM sales")
	e.runAll()
	if rec := e.do("GET", "/api/v1/queries/"+job.ID+"/results", ""); rec.Code != 429 {
		t.Fatalf("page should be refused: %d %s", rec.Code, rec.Body.String())
	}
	if rec := e.do("GET", "/api/v1/queries/"+job.ID+"/export", ""); rec.Code != 200 {
		t.Fatalf("export should fit: %d %s", rec.Code, rec.Body.String())
	}
}

func TestQueryResultExpired(t *testing.T) {
	e := newQueryEnv(t, nil, nil)
	job, _ := e.submit("SELECT id FROM sales")
	e.runAll()
	for _, k := range e.objects.Keys("exports/") {
		_ = e.objects.Delete(context.Background(), k)
	}
	for _, p := range []string{"/results", "/export"} {
		rec := e.do("GET", "/api/v1/queries/"+job.ID+p, "")
		if rec.Code != 410 || qErrCode(t, rec) != "result_expired" {
			t.Errorf("%s: %d %s", p, rec.Code, rec.Body.String())
		}
	}
}

func TestQueryEndpointsUnavailableWithoutDependencies(t *testing.T) {
	h := New(nil, nil)
	for _, c := range [][2]string{{"POST", "/api/v1/queries"}, {"GET", "/api/v1/queries"}, {"GET", "/api/v1/queries/x"}} {
		req := httptest.NewRequest(c[0], c[1], strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 503 {
			t.Errorf("%v: %d", c, rec.Code)
		}
	}
}
