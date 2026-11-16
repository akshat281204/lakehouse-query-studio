//go:build integration

package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// Runs only with -tags integration against a live PostgreSQL (TEST_POSTGRES_URL).
// Object storage is the in-memory store: no MinIO is available in this
// environment, so the S3 client itself is covered by its own HTTP-level tests.

func pg(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := catalog.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	for _, q := range []string{"query_jobs", "ingestion_jobs", "schema_columns", "dataset_versions", "datasets"} {
		if _, err := db.ExecContext(ctx, "DELETE FROM "+q); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func pgRegister(t *testing.T, db *sql.DB, name, prefix string, parts []string, cols []catalog.SchemaColumn) {
	t.Helper()
	if _, _, _, err := catalog.NewPostgresStore(db).Register(context.Background(), catalog.RegisterRecord{
		Name: name, SourcePrefix: prefix, ObjectPrefix: prefix, Format: catalog.FormatParquet,
		PartitionColumns: parts, Columns: cols,
	}); err != nil {
		t.Fatal(err)
	}
}

func newUUIDT(t *testing.T) string {
	t.Helper()
	id, err := catalog.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPGResolveRelation(t *testing.T) {
	db := pg(t)
	ctx := context.Background()
	pgRegister(t, db, "sales", "data/sales", nil, []catalog.SchemaColumn{{Name: "id", Type: "int64"}})
	pgRegister(t, db, "events", "data/events", []string{"region"}, []catalog.SchemaColumn{{Name: "region", Type: "string", IsPartition: true}})
	store := catalog.NewPostgresQueryStore(db)

	rel, err := store.ResolveRelation(ctx, "sales")
	if err != nil || rel.DatasetName != "sales" || rel.Version != 1 || rel.HivePartitioned {
		t.Fatalf("sales: %+v %v", rel, err)
	}
	if v1, err := store.ResolveRelation(ctx, "sales_v1"); err != nil || v1.VersionID != rel.VersionID {
		t.Fatalf("sales_v1: %+v %v", v1, err)
	}
	if ev, err := store.ResolveRelation(ctx, "events"); err != nil || !ev.HivePartitioned {
		t.Fatalf("registered partitioned data must be hive decoded: %+v %v", ev, err)
	}
	for _, bad := range []string{"nope", "sales_v2", "sales_v0", "SALES_V1 ", "sales; drop", "../sales", ""} {
		if _, err := store.ResolveRelation(ctx, bad); !errors.Is(err, catalog.ErrNotFound) {
			t.Errorf("%q: want ErrNotFound, got %v", bad, err)
		}
	}
	if byID, err := store.RelationByVersionID(ctx, rel.VersionID); err != nil || byID.ObjectPrefix != "data/sales" {
		t.Fatalf("by id: %+v %v", byID, err)
	}
	if _, err := store.RelationByVersionID(ctx, "not-a-uuid"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("bad id: %v", err)
	}
}

func TestPGClaimIsExclusiveAndFenced(t *testing.T) {
	db := pg(t)
	ctx := context.Background()
	store := catalog.NewPostgresQueryStore(db)
	const n = 6
	for i := 0; i < n; i++ {
		if _, err := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 1", StatementHash: "h"}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				j, err := store.ClaimQuery(ctx, fmt.Sprintf("w%d", w), time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if j == nil {
					return
				}
				mu.Lock()
				claimed[j.ID]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(claimed) != n {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), n)
	}
	for id, c := range claimed {
		if c != 1 {
			t.Fatalf("job %s claimed %d times", id, c)
		}
	}
}

func TestPGLeaseExpiryRequeuesThenFences(t *testing.T) {
	db := pg(t)
	ctx := context.Background()
	store := catalog.NewPostgresQueryStore(db)
	job, _ := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 1", StatementHash: "h"})

	first, err := store.ClaimQuery(ctx, "dead", 200*time.Millisecond)
	if err != nil || first == nil || first.Attempt != 1 {
		t.Fatalf("first claim: %+v %v", first, err)
	}
	// A live lease is never recovered.
	if n, _ := store.RecoverExpired(ctx); n != 0 {
		t.Fatalf("recovered %d live jobs", n)
	}
	if _, err := store.Heartbeat(ctx, job.ID, first.FencingToken, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if n, err := store.RecoverExpired(ctx); err != nil || n != 1 {
		t.Fatalf("recover: %d %v", n, err)
	}
	second, err := store.ClaimQuery(ctx, "alive", time.Minute)
	if err != nil || second == nil || second.Attempt != 2 || second.FencingToken == first.FencingToken {
		t.Fatalf("second claim: %+v %v", second, err)
	}
	if second.Statement != "SELECT 1" {
		t.Fatalf("requeued job must run the persisted statement, got %q", second.Statement)
	}
	// The dead worker is fenced on every transition.
	if _, err := store.Heartbeat(ctx, job.ID, first.FencingToken, time.Minute); !errors.Is(err, catalog.ErrFenced) {
		t.Fatalf("stale heartbeat: %v", err)
	}
	if err := store.Succeed(ctx, job.ID, first.FencingToken, catalog.QueryResult{ResultRef: "x"}); !errors.Is(err, catalog.ErrFenced) {
		t.Fatalf("stale succeed: %v", err)
	}
	if err := store.Finish(ctx, job.ID, first.FencingToken, catalog.QueryFailed, "x", "x", 0); !errors.Is(err, catalog.ErrFenced) {
		t.Fatalf("stale finish: %v", err)
	}
	if err := store.Succeed(ctx, job.ID, second.FencingToken, catalog.QueryResult{ResultRef: "ok", RowsReturned: 1, ResultBytes: 10}); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetQuery(ctx, job.ID)
	if got.Status != catalog.QuerySucceeded || got.ResultRef != "ok" {
		t.Fatalf("%+v", got)
	}
	// A terminal job cannot be finished again.
	if err := store.Finish(ctx, job.ID, second.FencingToken, catalog.QueryFailed, "x", "x", 0); !errors.Is(err, catalog.ErrFenced) {
		t.Fatalf("finish after success: %v", err)
	}
}

func TestPGRepeatedWorkerDeathFailsTheQuery(t *testing.T) {
	db := pg(t)
	ctx := context.Background()
	store := catalog.NewPostgresQueryStore(db)
	job, _ := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 1", StatementHash: "h"})
	for i := 0; i < catalog.MaxQueryAttempts; i++ {
		if c, err := store.ClaimQuery(ctx, "dying", 50*time.Millisecond); err != nil || c == nil {
			t.Fatalf("claim %d: %v %v", i, c, err)
		}
		time.Sleep(150 * time.Millisecond)
		if n, _ := store.RecoverExpired(ctx); n != 1 {
			t.Fatalf("recover %d: %d", i, n)
		}
	}
	got, _ := store.GetQuery(ctx, job.ID)
	if got.Status != catalog.QueryFailed || got.FailureReason != catalog.ReasonWorkerInterrupted {
		t.Fatalf("%+v", got)
	}
	if c, _ := store.ClaimQuery(ctx, "w", time.Minute); c != nil {
		t.Fatal("failed job was claimed again")
	}
}

func TestPGCancelAndQueueDepth(t *testing.T) {
	db := pg(t)
	ctx := context.Background()
	store := catalog.NewPostgresQueryStore(db)
	q1, _ := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 1", StatementHash: "h"})
	got, err := store.RequestCancel(ctx, q1.ID)
	if err != nil || got.Status != catalog.QueryCancelled {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := store.RequestCancel(ctx, q1.ID); !errors.Is(err, catalog.ErrInvalidState) {
		t.Fatalf("second cancel: %v", err)
	}

	q2, _ := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 2", StatementHash: "h"})
	claimed, _ := store.ClaimQuery(ctx, "w", time.Minute)
	if claimed == nil || claimed.ID != q2.ID {
		t.Fatalf("claimed %+v", claimed)
	}
	if got, err := store.RequestCancel(ctx, q2.ID); err != nil || got.Status != catalog.QueryRunning || !got.CancelRequested {
		t.Fatalf("cancel running: %+v %v", got, err)
	}
	if flagged, err := store.Heartbeat(ctx, q2.ID, claimed.FencingToken, time.Minute); err != nil || !flagged {
		t.Fatalf("heartbeat must surface the cancel flag: %v %v", flagged, err)
	}
	if err := store.Finish(ctx, q2.ID, claimed.FencingToken, catalog.QueryCancelled, "", "cancelled by request", time.Second); err != nil {
		t.Fatal(err)
	}

	// Combined queue depth: one queued ingestion job + one queued query.
	ing := catalog.NewPostgresIngestStore(db)
	if _, _, err := ing.CreateConvert(ctx, catalog.CreateConvert{
		JobID: newUUIDT(t), Name: "queued-ds", InputRef: "uploads/x", InputFormat: catalog.InputFormatCSV,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 3", StatementHash: "h", MaxQueued: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 4", StatementHash: "h", MaxQueued: 2}); !errors.Is(err, catalog.ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	// Concurrent submits cannot overshoot the limit.
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: "SELECT 5", StatementHash: "h", MaxQueued: 4})
			if err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// The queue already held 2 (one ingestion job, one query); with a limit of
	// 4 exactly 2 of the 10 racing submits may be accepted.
	var queued int
	if err := db.QueryRow(`SELECT (SELECT count(*) FROM query_jobs WHERE status='QUEUED') + (SELECT count(*) FROM ingestion_jobs WHERE status='QUEUED')`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if accepted != 2 || queued != 4 {
		t.Fatalf("accepted %d racing submits, %d queued; want 2 and 4", accepted, queued)
	}
}

func TestPGEndToEndRealQueryThroughRunner(t *testing.T) {
	db := pg(t)
	ctx := context.Background()
	objects := objectstore.NewMemory()

	// A registered, hive-partitioned dataset (partition values only in paths).
	for _, region := range []string{"eu", "us"} {
		data, _ := parquet.Encode([]parquet.WriteColumn{{Name: "id", Type: parquet.TypeInt64}, {Name: "amt", Type: parquet.TypeDouble}},
			[][]any{{int64(1), 10.0}, {int64(2), 20.0}})
		_ = objects.Put(ctx, "data/events/region="+region+"/part-0.parquet", data)
	}
	// A flat dataset, queried alongside it.
	flat, _ := parquet.Encode([]parquet.WriteColumn{{Name: "region", Type: parquet.TypeString}, {Name: "name", Type: parquet.TypeString}},
		[][]any{{"eu", "Europe"}, {"us", "Americas"}})
	_ = objects.Put(ctx, "data/regions/part-0.parquet", flat)
	pgRegister(t, db, "events", "data/events", []string{"region"}, []catalog.SchemaColumn{
		{Name: "id", Type: "int64"}, {Name: "amt", Type: "double"}, {Name: "region", Type: "string", IsPartition: true}})
	pgRegister(t, db, "regions", "data/regions", nil, []catalog.SchemaColumn{{Name: "region", Type: "string"}, {Name: "name", Type: "string"}})

	store := catalog.NewPostgresQueryStore(db)
	v, err := NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()

	stmt := "SELECT r.name, sum(e.amt) AS total, count(*) AS n FROM events e JOIN catalog.regions_v1 r ON e.region = r.region GROUP BY r.name ORDER BY r.name"
	val, err := v.Validate(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
	bindings := map[string]string{}
	for _, tb := range val.Tables {
		rel, err := store.ResolveRelation(ctx, tb.Name)
		if err != nil {
			t.Fatal(err)
		}
		bindings[tb.Name] = rel.VersionID
	}
	job, err := store.Submit(ctx, catalog.SubmitQuery{ID: newUUIDT(t), Statement: stmt, StatementHash: StatementHash(stmt), Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultRunnerConfig()
	cfg.TmpDir = t.TempDir()
	r, err := NewRunner(store, objects, NewDuckDB(), v, "it", cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if ran, err := r.RunOnce(ctx); err != nil || !ran {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	got, _ := store.GetQuery(ctx, job.ID)
	if got.Status != catalog.QuerySucceeded {
		t.Fatalf("status=%s reason=%s summary=%s", got.Status, got.FailureReason, got.ErrorSummary)
	}
	if got.ResultBytes == nil || got.RowsReturned == nil || *got.RowsReturned != 2 {
		t.Fatalf("%+v", got)
	}
	rr, err := parquet.ReadRows(ctx, objects, got.ResultRef, *got.ResultBytes, 0, 10)
	if err != nil || len(rr.Rows) != 2 {
		t.Fatalf("rows: %+v %v", rr, err)
	}
	if rr.Rows[0]["name"] != "Americas" || rr.Rows[0]["total"] != 30.0 || rr.Rows[1]["name"] != "Europe" || rr.Rows[1]["n"] != int64(2) {
		t.Fatalf("wrong aggregate: %v", rr.Rows)
	}
}
