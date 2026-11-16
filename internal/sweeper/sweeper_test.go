package sweeper

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

type env struct {
	store   *catalog.MemoryQueryStore
	objects *objectstore.Memory
	sw      *Sweeper
	now     time.Time
}

func newEnv() *env {
	now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	store := catalog.NewMemoryQueryStore()
	store.Now = func() time.Time { return now }
	objs := objectstore.NewMemory()
	return &env{store: store, objects: objs, now: now, sw: &Sweeper{
		Store: store, Objects: objs, Now: func() time.Time { return now },
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: telemetry.New("test"),
		Cfg:     Config{ExportTTL: 24 * time.Hour, HistoryRetention: 30 * 24 * time.Hour, StagingTTL: 24 * time.Hour, Interval: time.Hour},
	}}
}

func (e *env) put(key string, age time.Duration) {
	_ = e.objects.Put(context.Background(), key, []byte("x"))
	e.objects.SetModTime(key, e.now.Add(-age))
}

func (e *env) has(key string) bool { return len(e.objects.Keys(key)) == 1 }

// succeeded runs a query to SUCCEEDED, finished `age` ago, with a spool.
func (e *env) succeeded(t *testing.T, age time.Duration) (catalog.QueryJob, string) {
	t.Helper()
	ctx := context.Background()
	id, _ := catalog.NewUUID()
	if _, err := e.store.Submit(ctx, catalog.SubmitQuery{ID: id, Statement: "SELECT 1", StatementHash: "h"}); err != nil {
		t.Fatal(err)
	}
	e.store.Now = func() time.Time { return e.now.Add(-age) }
	j, _ := e.store.ClaimQuery(ctx, "w", time.Minute)
	key := "exports/default/" + id + "/attempt=1/result.parquet"
	e.put(key, age)
	if err := e.store.Succeed(ctx, j.ID, j.FencingToken, catalog.QueryResult{ResultRef: key, RowsReturned: 1, ResultBytes: 1}); err != nil {
		t.Fatal(err)
	}
	e.store.Now = func() time.Time { return e.now }
	got, _ := e.store.GetQuery(ctx, id)
	return got, key
}

func TestExpiredResultsAreClearedAndDeleted(t *testing.T) {
	e := newEnv()
	old, oldKey := e.succeeded(t, 25*time.Hour)
	fresh, freshKey := e.succeeded(t, 1*time.Hour)
	res := e.sw.Once(context.Background())
	if res.Exports != 1 {
		t.Fatalf("%+v", res)
	}
	if e.has(oldKey) || !e.has(freshKey) {
		t.Fatal("wrong objects deleted")
	}
	o, _ := e.store.GetQuery(context.Background(), old.ID)
	if o.ResultRef != "" || o.ResultBytes != nil || o.Status != catalog.QuerySucceeded {
		t.Fatalf("expired job must lose its result reference only: %+v", o)
	}
	f, _ := e.store.GetQuery(context.Background(), fresh.ID)
	if f.ResultRef != freshKey {
		t.Fatalf("fresh result touched: %+v", f)
	}
}

func TestHistoryRetentionDeletesRowsAndStatements(t *testing.T) {
	e := newEnv()
	old, _ := e.succeeded(t, 31*24*time.Hour)
	young, _ := e.succeeded(t, 2*24*time.Hour)
	res := e.sw.Once(context.Background())
	if res.History != 1 {
		t.Fatalf("%+v", res)
	}
	if _, err := e.store.GetQuery(context.Background(), old.ID); err == nil {
		t.Fatal("old query row (with its statement) must be deleted")
	}
	if _, err := e.store.GetQuery(context.Background(), young.ID); err != nil {
		t.Fatal("young row deleted")
	}
}

func TestRunningAndQueuedQueriesAreNeverPurged(t *testing.T) {
	e := newEnv()
	ctx := context.Background()
	idQ, _ := catalog.NewUUID()
	idR, _ := catalog.NewUUID()
	e.store.Now = func() time.Time { return e.now.Add(-90 * 24 * time.Hour) }
	_, _ = e.store.Submit(ctx, catalog.SubmitQuery{ID: idQ, Statement: "SELECT 1", StatementHash: "h"})
	_, _ = e.store.Submit(ctx, catalog.SubmitQuery{ID: idR, Statement: "SELECT 2", StatementHash: "h"})
	_, _ = e.store.ClaimQuery(ctx, "w", time.Minute) // idQ -> RUNNING
	e.store.Now = func() time.Time { return e.now }
	e.sw.Once(ctx)
	for _, id := range []string{idQ, idR} {
		if _, err := e.store.GetQuery(ctx, id); err != nil {
			t.Fatalf("active query %s purged", id)
		}
	}
}

func TestOrphanedExportsAndStagingAreSweptByAge(t *testing.T) {
	e := newEnv()
	e.put("exports/default/orphan-old/attempt=1/result.parquet", 48*time.Hour)
	e.put("exports/default/orphan-new/attempt=1/result.parquet", time.Hour)
	e.put("staging/default/ds/job/attempt=1/part-0.parquet", 48*time.Hour)
	e.put("staging/default/ds/job2/attempt=1/part-0.parquet", time.Hour)
	res := e.sw.Once(context.Background())
	if res.Exports != 1 || res.Staging != 1 {
		t.Fatalf("%+v", res)
	}
	for k, want := range map[string]bool{
		"exports/default/orphan-old/attempt=1/result.parquet": false,
		"exports/default/orphan-new/attempt=1/result.parquet": true,
		"staging/default/ds/job/attempt=1/part-0.parquet":     false,
		"staging/default/ds/job2/attempt=1/part-0.parquet":    true,
	} {
		if e.has(k) != want {
			t.Errorf("%s present=%v want %v", k, e.has(k), want)
		}
	}
}

// The sweeper must never delete dataset data, uploads, or anything of unknown age.
func TestNeverDeletesLakeUploadsOrUnknownAge(t *testing.T) {
	e := newEnv()
	e.put("lake/default/sales/version=1/attempt=1/part-0.parquet", 1000*24*time.Hour)
	e.put("uploads/default/sales/job/data.csv", 1000*24*time.Hour)
	e.put("data/registered/part.parquet", 1000*24*time.Hour)
	e.put("exports/default/unknown-age/result.parquet", 0)
	e.objects.SetModTime("exports/default/unknown-age/result.parquet", time.Time{})
	e.sw.Once(context.Background())
	for _, k := range []string{
		"lake/default/sales/version=1/attempt=1/part-0.parquet",
		"uploads/default/sales/job/data.csv",
		"data/registered/part.parquet",
		"exports/default/unknown-age/result.parquet",
	} {
		if !e.has(k) {
			t.Errorf("%s was deleted", k)
		}
	}
}

// A corrupted or malicious result_ref must not make the sweeper delete a
// dataset object.
func TestRefsOutsideExportsAreNeverDeleted(t *testing.T) {
	e := newEnv()
	ctx := context.Background()
	id, _ := catalog.NewUUID()
	_, _ = e.store.Submit(ctx, catalog.SubmitQuery{ID: id, Statement: "SELECT 1", StatementHash: "h"})
	e.store.Now = func() time.Time { return e.now.Add(-48 * time.Hour) }
	j, _ := e.store.ClaimQuery(ctx, "w", time.Minute)
	victim := "lake/default/sales/version=1/attempt=1/part-0.parquet"
	e.put(victim, 48*time.Hour)
	_ = e.store.Succeed(ctx, j.ID, j.FencingToken, catalog.QueryResult{ResultRef: victim, ResultBytes: 1})
	e.store.Now = func() time.Time { return e.now }
	e.sw.Once(ctx)
	if !e.has(victim) {
		t.Fatal("sweeper deleted an object outside exports/")
	}
	if deletable("exports/../lake/x") {
		t.Fatal("path traversal accepted")
	}
}

func TestSweepIsIdempotentAndCountsMetrics(t *testing.T) {
	e := newEnv()
	e.succeeded(t, 25*time.Hour)
	if r := e.sw.Once(context.Background()); r.Exports != 1 {
		t.Fatalf("%+v", r)
	}
	if r := e.sw.Once(context.Background()); r != (Result{}) {
		t.Fatalf("second pass should find nothing: %+v", r)
	}
	var b = new(bytesBuf)
	e.sw.Metrics.Registry.Render(b)
	if !contains(b.s, `lqs_sweeper_deleted_total{kind="export"} 1`) {
		t.Fatalf("metrics:\n%s", b.s)
	}
}

type bytesBuf struct{ s string }

func (b *bytesBuf) Write(p []byte) (int, error) { b.s += string(p); return len(p), nil }

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
