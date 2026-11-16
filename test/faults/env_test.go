// Package faults holds end-to-end failure tests. Each one chains the real
// components (API handlers, catalog stores, ingestion and query runners, the
// DuckDB engine, the sweeper) over an object store that can be made to fail,
// and asserts persisted job/catalog state and object-store contents, not just
// HTTP status codes.
package faults

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/api"
	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

type env struct {
	t        *testing.T
	mem      *objectstore.Memory
	objects  *objectstore.Faulty
	ingStore *catalog.MemoryIngestStore
	qStore   *catalog.MemoryQueryStore
	ingest   *ingest.Runner
	query    *query.Runner
	handler  http.Handler
	tmp      string
	now      time.Time
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newEnv(t *testing.T, mutate func(*query.RunnerConfig)) *env {
	t.Helper()
	e := &env{t: t, mem: objectstore.NewMemory(), tmp: t.TempDir(), now: time.Now().UTC()}
	e.objects = objectstore.NewFaulty(e.mem)
	e.ingStore = catalog.NewMemoryIngestStore()
	e.ingStore.Now = func() time.Time { return e.now }
	e.qStore = catalog.NewMemoryQueryStore()
	e.ingest = ingest.NewRunner(e.ingStore, e.objects, "ingest-1", quiet())

	v, err := query.NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	cfg := query.DefaultRunnerConfig()
	cfg.TmpDir = e.tmp
	cfg.Heartbeat = 20 * time.Millisecond
	cfg.Lease = 5 * time.Second
	cfg.Poll = 10 * time.Millisecond
	cfg.Limits.Timeout = 20 * time.Second
	cfg.Limits.Threads = 2
	if mutate != nil {
		mutate(&cfg)
	}
	e.query, err = query.NewRunner(e.qStore, e.objects, query.NewDuckDB(), v, "query-1", cfg, quiet())
	if err != nil {
		t.Fatal(err)
	}
	e.handler = api.New(nil, e.mem, api.WithQueries(api.QueryDeps{
		Store: e.qStore, Validator: v, Objects: e.mem, Limits: api.DefaultQueryLimits()}))
	return e
}

// ingestCSV queues an upload the way the API would and returns its job.
func (e *env) ingestCSV(name, body string) catalog.Job {
	e.t.Helper()
	id, _ := catalog.NewUUID()
	key := "uploads/default/" + name + "/" + id + "/input"
	if err := e.mem.Put(context.Background(), key, []byte(body)); err != nil {
		e.t.Fatal(err)
	}
	job, _, err := e.ingStore.CreateConvert(context.Background(), catalog.CreateConvert{
		JobID: id, Name: name, InputRef: key, InputFormat: catalog.InputFormatCSV})
	if err != nil {
		e.t.Fatal(err)
	}
	return job
}

func (e *env) runIngest() {
	e.t.Helper()
	for {
		ran, err := e.ingest.RunOnce(context.Background())
		if err != nil {
			e.t.Fatal(err)
		}
		if !ran {
			return
		}
	}
}

// publish makes every READY version of name queryable (the catalog bridge).
func (e *env) publish(name string) {
	for _, v := range e.ingStore.ReadyVersions(name) {
		e.qStore.AddRelation(catalog.Relation{DatasetName: name, Version: v.Version, ObjectPrefix: v.ObjectPrefix, Format: "parquet"})
	}
}

func (e *env) request(method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	return rec
}

// submit goes through the real API handler (validation, resolution, queueing).
func (e *env) submit(stmt string) string {
	e.t.Helper()
	b, _ := json.Marshal(map[string]string{"statement": stmt})
	rec := e.request("POST", "/api/v1/queries", string(b))
	if rec.Code != http.StatusAccepted {
		e.t.Fatalf("submit: %d %s", rec.Code, rec.Body.String())
	}
	var j catalog.QueryJob
	_ = json.Unmarshal(rec.Body.Bytes(), &j)
	return j.ID
}

func (e *env) runQueries() {
	e.t.Helper()
	for {
		ran, err := e.query.RunOnce(context.Background())
		if err != nil {
			e.t.Fatal(err)
		}
		if !ran {
			return
		}
	}
}

func (e *env) job(id string) catalog.QueryJob {
	e.t.Helper()
	j, err := e.qStore.GetQuery(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return j
}

func (e *env) ingestJob(id string) catalog.Job {
	e.t.Helper()
	j, err := e.ingStore.GetJob(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return j
}

// firstValue pages the first cell of the first row of a SUCCEEDED query.
func (e *env) firstValue(id string) any {
	e.t.Helper()
	rec := e.request("GET", "/api/v1/queries/"+id+"/results?limit=1", "")
	if rec.Code != 200 {
		e.t.Fatalf("results: %d %s", rec.Code, rec.Body.String())
	}
	var r struct{ Rows [][]any }
	_ = json.Unmarshal(rec.Body.Bytes(), &r)
	if len(r.Rows) == 0 {
		e.t.Fatal("no rows")
	}
	return r.Rows[0][0]
}

func (e *env) noScratchLeft() {
	e.t.Helper()
	if entries, _ := os.ReadDir(e.tmp); len(entries) != 0 {
		e.t.Fatalf("scratch directories left behind: %v", entries)
	}
}

func (e *env) keys(prefix string) []string { return e.mem.Keys(prefix) }

func csvRows(n int) string {
	var b strings.Builder
	b.WriteString("id,grp,amount,label\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%d,g%d,%d.5,row-%d\n", i, i%7, i%100, i)
	}
	return b.String()
}

// newHandler builds a fresh API handler over the same durable stores, as a
// restarted API process would.
func newHandler(e *env, v *query.Validator) http.Handler {
	return api.New(nil, e.mem, api.WithQueries(api.QueryDeps{
		Store: e.qStore, Validator: v, Objects: e.mem, Limits: api.DefaultQueryLimits()}))
}
