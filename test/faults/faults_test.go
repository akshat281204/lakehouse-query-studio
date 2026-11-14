package faults

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
	"github.com/akshat281204/lakehouse-query-studio/internal/sweeper"
)

// ---- object store unavailable ----------------------------------------------

func TestObjectStoreDownWhileQuerying(t *testing.T) {
	e := newEnv(t, nil)
	e.ingestCSV("sales", csvRows(500))
	e.runIngest()
	e.publish("sales")

	e.objects.SetDown(true)
	id := e.submit("SELECT count(*) AS n FROM sales")
	e.runQueries()

	j := e.job(id)
	if j.Status != catalog.QueryFailed || j.FailureReason != catalog.ReasonObjectStore {
		t.Fatalf("status=%s reason=%s", j.Status, j.FailureReason)
	}
	if j.RowsReturned != nil || j.ResultBytes != nil || j.ResultRef != "" || j.FinishedAt == nil {
		t.Fatalf("a failed query must record no result: %+v", j)
	}
	if got := e.keys("exports/"); len(got) != 0 {
		t.Fatalf("exports left behind: %v", got)
	}
	e.noScratchLeft()

	// The service recovers on its own: no restart, the next query succeeds.
	e.objects.Heal()
	id2 := e.submit("SELECT count(*) AS n FROM sales")
	e.runQueries()
	if e.job(id2).Status != catalog.QuerySucceeded || e.firstValue(id2) != float64(500) {
		t.Fatalf("after recovery: %+v", e.job(id2))
	}
}

func TestObjectStoreFailsWhileSpoolingResult(t *testing.T) {
	e := newEnv(t, nil)
	e.ingestCSV("sales", csvRows(100))
	e.runIngest()
	e.publish("sales")

	e.objects.FailPutsAfter(0, nil) // reads work, the spool write fails
	id := e.submit("SELECT id FROM sales")
	e.runQueries()

	j := e.job(id)
	if j.Status != catalog.QueryFailed || j.FailureReason != catalog.ReasonObjectStore {
		t.Fatalf("status=%s reason=%s", j.Status, j.FailureReason)
	}
	// Terminal success is only recorded after results exist.
	if j.ResultRef != "" || j.RowsReturned != nil {
		t.Fatalf("success fields set on a failed job: %+v", j)
	}
	if rec := e.request("GET", "/api/v1/queries/"+id+"/results", ""); rec.Code != 409 {
		t.Fatalf("results of a failed job: %d", rec.Code)
	}
	if len(e.keys("exports/")) != 0 {
		t.Fatal("partial spool left behind")
	}
}

func TestObjectStoreFailsMidIngestionThenRetrySucceeds(t *testing.T) {
	e := newEnv(t, nil)
	const rows = 250_000 // three 100k-row parts
	job := e.ingestCSV("big", csvRows(rows))

	e.objects.FailPutsAfter(1, nil) // first part lands, second fails
	e.runIngest()

	failed := e.ingestJob(job.ID)
	if failed.Status != catalog.JobFailed || !failed.Retryable {
		t.Fatalf("job = %+v", failed)
	}
	v, _ := e.ingStore.Version(failed.VersionID)
	if v.Status != catalog.VersionFailed {
		t.Fatalf("version = %+v", v)
	}
	if len(e.ingStore.ReadyVersions("big")) != 0 {
		t.Fatal("a failed ingestion published a READY version")
	}
	if left := append(e.keys("lake/"), e.keys("staging/")...); len(left) != 0 {
		t.Fatalf("failed attempt left objects behind: %v", left)
	}
	// Nothing is queryable and the API says so.
	b, _ := json.Marshal(map[string]string{"statement": "SELECT count(*) FROM big"})
	if rec := e.request("POST", "/api/v1/queries", string(b)); rec.Code != 400 {
		t.Fatalf("query on unpublished dataset: %d", rec.Code)
	}

	// The store comes back; retry creates a new version and job.
	e.objects.Heal()
	newID, _ := catalog.NewUUID()
	retry, err := e.ingStore.Retry(context.Background(), job.ID, newID)
	if err != nil {
		t.Fatal(err)
	}
	e.runIngest()
	done := e.ingestJob(retry.ID)
	if done.Status != catalog.JobSucceeded || done.Version != 2 {
		t.Fatalf("retry = %+v", done)
	}
	ready := e.ingStore.ReadyVersions("big")
	if len(ready) != 1 || ready[0].Version != 2 {
		t.Fatalf("ready = %+v", ready)
	}
	e.publish("big")
	id := e.submit("SELECT count(*) AS n FROM big")
	e.runQueries()
	if e.firstValue(id) != float64(rows) {
		t.Fatalf("count = %v want %d", e.firstValue(id), rows)
	}
}

// ---- corrupt and partial input ----------------------------------------------

func TestCorruptParquetFailsOnlyQueriesThatReadIt(t *testing.T) {
	for name, damage := range map[string]func([]byte) []byte{
		"garbage":   func([]byte) []byte { return []byte("this is not parquet at all, just text") },
		"truncated": func(b []byte) []byte { return b[:len(b)/2] },
		"bad magic": func(b []byte) []byte { c := append([]byte(nil), b...); copy(c[len(c)-4:], "XXXX"); return c },
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, nil)
			e.ingestCSV("good", csvRows(50))
			e.ingestCSV("damaged", csvRows(50))
			e.runIngest()
			e.publish("good")
			e.publish("damaged")

			part := e.keys(e.ingStore.ReadyVersions("damaged")[0].ObjectPrefix)[0]
			data, _ := e.mem.Get(context.Background(), part, 1<<30)
			_ = e.mem.Put(context.Background(), part, damage(data))

			bad := e.submit("SELECT count(*) FROM damaged")
			ok := e.submit("SELECT count(*) AS n FROM good")
			e.runQueries()

			j := e.job(bad)
			if j.Status != catalog.QueryFailed || j.FailureReason != catalog.ReasonEngineError {
				t.Fatalf("status=%s reason=%s summary=%q", j.Status, j.FailureReason, j.ErrorSummary)
			}
			if j.ResultRef != "" || strings.Contains(j.ErrorSummary, "part-") {
				t.Fatalf("result/keys leaked: %+v", j)
			}
			if e.job(ok).Status != catalog.QuerySucceeded || e.firstValue(ok) != float64(50) {
				t.Fatalf("a healthy dataset was affected: %+v", e.job(ok))
			}
			if got := e.keys("exports/"); len(got) != 1 {
				t.Fatalf("exports = %v, want only the healthy query's spool", got)
			}
			e.noScratchLeft()
		})
	}
}

func TestCorruptUploadFailsWithoutWritingAnyDataObjects(t *testing.T) {
	for name, body := range map[string]string{
		"ragged rows":  "a,b\n1,2\n3\n",
		"header only":  "a,b\n",
		"binary junk":  "\x00\x01\x02\xff\xfe",
		"unterminated": "a,b\n\"1,2\n",
		"empty":        "",
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, nil)
			job := e.ingestCSV("junk", body)
			e.runIngest()
			got := e.ingestJob(job.ID)
			if got.Status != catalog.JobFailed || got.Retryable {
				t.Fatalf("job = %+v", got)
			}
			if got.FailureReason != ingest.ReasonInvalidInput {
				t.Fatalf("reason = %q", got.FailureReason)
			}
			v, _ := e.ingStore.Version(got.VersionID)
			if v.Status != catalog.VersionFailed || len(e.ingStore.ReadyVersions("junk")) != 0 {
				t.Fatalf("version = %+v", v)
			}
			if left := append(e.keys("lake/"), e.keys("staging/")...); len(left) != 0 {
				t.Fatalf("objects written for invalid input: %v", left)
			}
			if strings.Contains(got.ErrorSummary, "\x00") {
				t.Fatal("error summary echoes input bytes")
			}
		})
	}
}

// ---- worker restarts ---------------------------------------------------------

// hangEngine blocks until the worker is told to stop, like a query in flight
// when the process is killed.
type hangEngine struct{ started chan struct{} }

func (h hangEngine) Execute(ctx context.Context, _ query.Request) (*query.Result, error) {
	select {
	case h.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, query.ErrCancelled
}

func TestQueryWorkerRestartRequeuesAndRerunsFromPersistedStatement(t *testing.T) {
	e := newEnv(t, nil)
	e.ingestCSV("sales", csvRows(300))
	e.runIngest()
	e.publish("sales")
	id := e.submit("SELECT count(*) AS n FROM sales")

	// Worker 1 claims the job and is killed mid-query.
	hang := hangEngine{started: make(chan struct{}, 1)}
	e.query.Engine = hang
	ctx, kill := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = e.query.RunOnce(ctx); close(done) }()
	<-hang.started
	kill()
	<-done

	j := e.job(id)
	if j.Status != catalog.QueryRunning || j.Attempt != 1 || j.FinishedAt != nil {
		t.Fatalf("a killed worker must leave the job RUNNING: %+v", j)
	}
	if len(e.keys("exports/")) != 0 {
		t.Fatal("killed worker left a spool")
	}
	e.noScratchLeft()

	// Its lease expires; worker 2 (a new process) recovers the job.
	e.qStore.ExpireLease(id)
	e.query.Engine = query.NewDuckDB()
	e.query.Owner = "query-2"
	e.runQueries()

	j = e.job(id)
	if j.Status != catalog.QuerySucceeded || j.Attempt != 2 {
		t.Fatalf("after restart: %+v", j)
	}
	if got := e.keys("exports/"); len(got) != 1 || !strings.Contains(got[0], "attempt=2") {
		t.Fatalf("exports = %v, want exactly the attempt=2 spool", got)
	}
	if e.firstValue(id) != float64(300) {
		t.Fatalf("result after recovery = %v", e.firstValue(id))
	}
}

func TestIngestionWorkerCrashMidWriteIsRecoveredWithoutDoubleCounting(t *testing.T) {
	e := newEnv(t, nil)
	const rows = 250_000
	job := e.ingestCSV("big", csvRows(rows))

	// Worker 1 dies mid-write: three staging parts land, then one lake part,
	// then the process is killed during the next write.
	ctx, kill := context.WithCancel(context.Background())
	e.objects.FailPutsAfter(4, kill)
	if _, err := e.ingest.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	crashed := e.ingestJob(job.ID)
	if crashed.Status != catalog.JobRunning || crashed.Attempt != 1 {
		t.Fatalf("a crashed worker must leave the job RUNNING: %+v", crashed)
	}
	attempt1 := e.keys("lake/default/big/version=1/attempt=1/")
	if len(attempt1) == 0 || len(e.keys("staging/")) == 0 {
		t.Fatalf("test did not exercise a partial lake write (lake=%d staging=%d)", len(attempt1), len(e.keys("staging/")))
	}
	if len(e.ingStore.ReadyVersions("big")) != 0 {
		t.Fatal("a half-written version is visible")
	}

	// Lease expires; worker 2 recovers as attempt 2 and publishes.
	e.objects.Heal()
	e.now = e.now.Add(time.Hour)
	e.ingest.Owner = "ingest-2"
	e.runIngest()

	done := e.ingestJob(job.ID)
	if done.Status != catalog.JobSucceeded || done.Attempt != 2 {
		t.Fatalf("job = %+v", done)
	}
	ready := e.ingStore.ReadyVersions("big")
	if len(ready) != 1 || !strings.HasSuffix(ready[0].ObjectPrefix, "attempt=2/") {
		t.Fatalf("ready = %+v", ready)
	}

	// The dead attempt's objects are NOT referenced and must not be queried:
	// the count is exactly the input, not input + attempt-1 leftovers.
	e.publish("big")
	id := e.submit("SELECT count(*) AS n FROM big")
	e.runQueries()
	if got := e.firstValue(id); got != float64(rows) {
		t.Fatalf("count = %v, want exactly %d (stale attempt leaked into results?)", got, rows)
	}
	// Known limit (docs/evaluation.md): the abandoned attempt-1 objects are
	// still in the store; they are unreferenced and unreadable by queries.
	for _, k := range attempt1 {
		if strings.HasPrefix(k, ready[0].ObjectPrefix) {
			t.Fatalf("attempt-1 key %s is under the published prefix", k)
		}
	}
}

// ---- API restart ----------------------------------------------------------------

func TestAPIRestartLosesNothing(t *testing.T) {
	e := newEnv(t, nil)
	e.ingestCSV("sales", csvRows(40))
	e.runIngest()
	e.publish("sales")
	queued := e.submit("SELECT count(*) AS n FROM sales")
	done := e.submit("SELECT sum(id) AS s FROM sales")
	e.runQueries()

	// "Restart": a brand-new handler (no in-process state) over the same
	// durable stores.
	v2, err := query.NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v2.Close() }()
	e.handler = newHandler(e, v2)

	for _, id := range []string{queued, done} {
		rec := e.request("GET", "/api/v1/queries/"+id, "")
		if rec.Code != 200 {
			t.Fatalf("status after restart: %d", rec.Code)
		}
	}
	if e.job(done).Status != catalog.QuerySucceeded || e.firstValue(done) != float64(40*39/2) {
		t.Fatalf("results lost across restart: %+v", e.job(done))
	}
	hist := e.request("GET", "/api/v1/queries", "")
	var h struct{ Queries []catalog.QueryJob }
	_ = json.Unmarshal(hist.Body.Bytes(), &h)
	if len(h.Queries) != 2 {
		t.Fatalf("history after restart: %d rows", len(h.Queries))
	}
}

// ---- timeout, cancellation, limits, expiry (real DuckDB) ---------------------------

const heavy = "SELECT count(*) FROM big a, big b, big c, big d"

func heavyEnv(t *testing.T, mutate func(*query.RunnerConfig)) *env {
	e := newEnv(t, mutate)
	e.ingestCSV("big", csvRows(30_000))
	e.runIngest()
	e.publish("big")
	return e
}

func TestRealQueryTimeoutIsTimedOutNotFailed(t *testing.T) {
	e := heavyEnv(t, func(c *query.RunnerConfig) { c.Limits.Timeout = 500 * time.Millisecond })
	id := e.submit(heavy)
	start := time.Now()
	e.runQueries()
	j := e.job(id)
	if j.Status != catalog.QueryTimedOut || j.FailureReason != "" || j.ResultRef != "" || j.FinishedAt == nil {
		t.Fatalf("%+v", j)
	}
	if time.Since(start) > 15*time.Second {
		t.Fatalf("timeout took %s", time.Since(start))
	}
	if len(e.keys("exports/")) != 0 {
		t.Fatal("timed-out query left a spool")
	}
	e.noScratchLeft()
}

func TestRealQueryCancelStopsExecution(t *testing.T) {
	e := heavyEnv(t, nil)
	id := e.submit(heavy)
	done := make(chan struct{})
	go func() { e.runQueries(); close(done) }()
	deadline := time.Now().Add(10 * time.Second)
	for e.job(id).Status != catalog.QueryRunning {
		if time.Now().After(deadline) {
			t.Fatal("query never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let DuckDB get into the scan
	rec := e.request("POST", "/api/v1/queries/"+id+"/cancel", "")
	if rec.Code != 202 {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body.String())
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("cancellation did not stop the running query")
	}
	j := e.job(id)
	if j.Status != catalog.QueryCancelled || j.ResultRef != "" {
		t.Fatalf("%+v", j)
	}
	if len(e.keys("exports/")) != 0 {
		t.Fatal("cancelled query left a spool")
	}
	e.noScratchLeft()
}

func TestOversizedResultIsTruncatedAndStaysWithinLimits(t *testing.T) {
	e := newEnv(t, func(c *query.RunnerConfig) { c.Limits.MaxRows = 100 })
	e.ingestCSV("sales", csvRows(5_000))
	e.runIngest()
	e.publish("sales")
	id := e.submit("SELECT id, label FROM sales ORDER BY id")
	e.runQueries()

	j := e.job(id)
	if j.Status != catalog.QuerySucceeded || !j.Truncated || *j.RowsReturned != 100 {
		t.Fatalf("%+v", j)
	}
	rec := e.request("GET", "/api/v1/queries/"+id+"/results?limit=1000", "")
	var page struct {
		Returned   int
		TotalRows  int64
		Truncated  bool
		NextOffset *int
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if page.Returned != 100 || page.TotalRows != 100 || !page.Truncated || page.NextOffset != nil {
		t.Fatalf("page = %+v", page)
	}
	exp := e.request("GET", "/api/v1/queries/"+id+"/export", "")
	if exp.Header().Get("X-Export-Truncated") != "true" || strings.Count(exp.Body.String(), "\n") != 101 {
		t.Fatalf("export: truncated=%s lines=%d", exp.Header().Get("X-Export-Truncated"), strings.Count(exp.Body.String(), "\n"))
	}
}

func TestOversizedRowFailsWithoutAnySpool(t *testing.T) {
	e := newEnv(t, func(c *query.RunnerConfig) { c.Limits.MaxRowBytes = 200 })
	e.ingestCSV("sales", csvRows(10))
	e.runIngest()
	e.publish("sales")
	id := e.submit("SELECT repeat('x', 5000) AS big FROM sales")
	e.runQueries()
	j := e.job(id)
	if j.Status != catalog.QueryFailed || j.FailureReason != catalog.ReasonResourceExceeded || j.ResultRef != "" {
		t.Fatalf("%+v", j)
	}
	if len(e.keys("exports/")) != 0 {
		t.Fatal("spool exists for an oversized row")
	}
}

func TestTemporaryExportExpiryEndToEnd(t *testing.T) {
	e := newEnv(t, nil)
	e.ingestCSV("sales", csvRows(20))
	e.runIngest()
	e.publish("sales")
	id := e.submit("SELECT id FROM sales")
	e.runQueries()
	if e.request("GET", "/api/v1/queries/"+id+"/results", "").Code != 200 {
		t.Fatal("results should exist before expiry")
	}
	key := e.job(id)
	if len(e.keys("exports/")) != 1 || key.ResultRef == "" {
		t.Fatal("no spool to expire")
	}

	sw := &sweeper.Sweeper{Store: e.qStore, Objects: e.objects, Log: quiet(),
		Now: func() time.Time { return time.Now().Add(25 * time.Hour) },
		Cfg: sweeper.Config{ExportTTL: 24 * time.Hour, HistoryRetention: 30 * 24 * time.Hour, StagingTTL: 24 * time.Hour, Interval: time.Hour}}
	if r := sw.Once(context.Background()); r.Exports != 1 {
		t.Fatalf("sweep = %+v", r)
	}

	// Persisted state: row kept, result reference cleared; object gone.
	j := e.job(id)
	if j.Status != catalog.QuerySucceeded || j.ResultRef != "" || j.RowsReturned == nil {
		t.Fatalf("%+v", j)
	}
	if len(e.keys("exports/")) != 0 {
		t.Fatal("spool still in the object store")
	}
	for _, p := range []string{"/results", "/export"} {
		rec := e.request("GET", "/api/v1/queries/"+id+p, "")
		if rec.Code != 410 || !strings.Contains(rec.Body.String(), "result_expired") {
			t.Fatalf("%s after expiry: %d %s", p, rec.Code, rec.Body.String())
		}
	}
	// The dataset itself is untouched.
	if len(e.keys("lake/")) == 0 {
		t.Fatal("sweeper removed dataset objects")
	}
}
