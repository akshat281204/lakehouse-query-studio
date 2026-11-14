//go:build integration

package ingest

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
)

// Runs only with -tags integration against a live PostgreSQL (TEST_POSTGRES_URL).

func pgDB(t *testing.T) *sql.DB {
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
	// Isolate tests: wipe job/catalog rows.
	for _, q := range []string{"ingestion_jobs", "schema_columns", "dataset_versions", "datasets"} {
		if _, err := db.ExecContext(ctx, "DELETE FROM "+q); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func pgSubmit(t *testing.T, store catalog.IngestStore, objs *memObjects, name, body string, parts []string) catalog.Job {
	t.Helper()
	id, _ := catalog.NewUUID()
	key := "uploads/default/" + name + "/" + id + "/input"
	_ = objs.Put(context.Background(), key, []byte(body))
	job, _, err := store.CreateConvert(context.Background(), catalog.CreateConvert{
		JobID: id, Name: name, InputRef: key, InputFormat: "csv", PartitionColumns: parts,
	})
	if err != nil {
		t.Fatalf("CreateConvert: %v", err)
	}
	return job
}

func TestPostgresIngestEndToEnd(t *testing.T) {
	db := pgDB(t)
	ctx := context.Background()
	store := catalog.NewPostgresIngestStore(db)
	objs := newMemObjects()
	r := NewRunner(store, objs, "it", nilLogger())

	job := pgSubmit(t, store, objs, "sales", "id,region,amount\n1,us,1.5\n2,eu,2.5\n3,us,\n", []string{"region"})
	if job.Status != catalog.JobQueued || job.Version != 1 {
		t.Fatalf("job = %+v", job)
	}
	if worked, err := r.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("RunOnce = %v, %v", worked, err)
	}
	got, err := store.GetJob(ctx, job.ID)
	if err != nil || got.Status != catalog.JobSucceeded {
		t.Fatalf("job after run = %+v, %v", got, err)
	}

	// The catalog store sees a READY version with the schema.
	cs := catalog.NewPostgresStore(db)
	list, err := cs.List(ctx, catalog.ListOptions{})
	if err != nil || list.Total != 1 {
		t.Fatalf("List = %+v, %v", list, err)
	}
	v, err := cs.GetLatestReadyVersion(ctx, got.DatasetID)
	if err != nil || v.RowCount == nil || *v.RowCount != 3 || !strings.Contains(v.ObjectPrefix, "version=1/attempt=1/") {
		t.Fatalf("ready version = %+v, %v", v, err)
	}
	cols, _ := cs.GetSchema(ctx, v.ID)
	if len(cols) != 3 || !cols[1].IsPartition || cols[1].Name != "region" {
		t.Errorf("schema = %+v", cols)
	}
	if len(v.PartitionColumns) != 1 {
		t.Errorf("partition columns = %v", v.PartitionColumns)
	}
}

func TestPostgresFailureThenRetryNoDuplicateVisibleVersions(t *testing.T) {
	db := pgDB(t)
	ctx := context.Background()
	store := catalog.NewPostgresIngestStore(db)
	cs := catalog.NewPostgresStore(db)
	objs := newMemObjects()
	r := NewRunner(store, objs, "it", nilLogger())

	bad := pgSubmit(t, store, objs, "inv", "a,b\n1\n", nil)
	_, _ = r.RunOnce(ctx)
	got, _ := store.GetJob(ctx, bad.ID)
	if got.Status != catalog.JobFailed || got.Retryable {
		t.Fatalf("bad job = %+v", got)
	}
	if _, err := cs.GetLatestReadyVersion(ctx, got.DatasetID); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("failed ingestion exposed a ready version: %v", err)
	}
	latest, _ := cs.GetLatestVersion(ctx, got.DatasetID)
	if latest.Status != catalog.VersionFailed || latest.ErrorSummary == "" {
		t.Errorf("latest = %+v", latest)
	}
	if _, err := store.Retry(ctx, bad.ID, mustUUID()); !errors.Is(err, catalog.ErrInvalidArgument) {
		t.Errorf("non-retryable retry err = %v", err)
	}

	// A retryable failure retried to success yields exactly one READY version.
	flaky := pgSubmit(t, store, objs, "flaky", "id\n1\n2\n", nil)
	objs.putErr = func(string) error { return errTransient }
	_, _ = r.RunOnce(ctx)
	fj, _ := store.GetJob(ctx, flaky.ID)
	if fj.Status != catalog.JobFailed || !fj.Retryable {
		t.Fatalf("flaky job = %+v", fj)
	}
	objs.putErr = nil
	retry, err := store.Retry(ctx, flaky.ID, mustUUID())
	if err != nil || retry.Version != 2 || retry.RetryOf != flaky.ID {
		t.Fatalf("Retry = %+v, %v", retry, err)
	}
	if _, err := store.Retry(ctx, flaky.ID, mustUUID()); !errors.Is(err, catalog.ErrAlreadyExists) {
		t.Errorf("double retry err = %v", err)
	}
	_, _ = r.RunOnce(ctx)
	var ready int
	_ = db.QueryRowContext(ctx, `SELECT count(*) FROM dataset_versions WHERE dataset_id = $1 AND status = 'READY'`, fj.DatasetID).Scan(&ready)
	if ready != 1 {
		t.Errorf("ready versions = %d, want 1", ready)
	}
}

func TestPostgresCrashRecoveryAndFencing(t *testing.T) {
	db := pgDB(t)
	ctx := context.Background()
	store := catalog.NewPostgresIngestStore(db)
	objs := newMemObjects()
	r := NewRunner(store, objs, "it", nilLogger())

	job := pgSubmit(t, store, objs, "crash", "a\n1\n", nil)
	dead, err := store.Claim(ctx, "dead", time.Minute)
	if err != nil || dead == nil || dead.Attempt != 1 {
		t.Fatalf("claim = %+v, %v", dead, err)
	}
	if again, _ := store.Claim(ctx, "w2", time.Minute); again != nil {
		t.Fatal("claimed a live lease")
	}
	// Expire the lease as if the worker died.
	if _, err := db.ExecContext(ctx, `UPDATE ingestion_jobs SET lease_expires_at = now() - interval '1 minute' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if worked, err := r.RunOnce(ctx); err != nil || !worked {
		t.Fatalf("recovery run = %v, %v", worked, err)
	}
	got, _ := store.GetJob(ctx, job.ID)
	if got.Status != catalog.JobSucceeded || got.Attempt != 2 {
		t.Fatalf("recovered job = %+v", got)
	}
	// The dead worker cannot commit late.
	if err := store.Complete(ctx, job.ID, dead.FencingToken, catalog.Publish{ObjectPrefix: "lake/x/"}); !errors.Is(err, catalog.ErrFenced) {
		t.Errorf("stale Complete err = %v", err)
	}
	if err := store.Fail(ctx, job.ID, dead.FencingToken, "x", "y", true); !errors.Is(err, catalog.ErrFenced) {
		t.Errorf("stale Fail err = %v", err)
	}
}

func TestPostgresExhaustedAttemptsFail(t *testing.T) {
	db := pgDB(t)
	ctx := context.Background()
	store := catalog.NewPostgresIngestStore(db)
	objs := newMemObjects()
	job := pgSubmit(t, store, objs, "loop", "a\n1\n", nil)
	for i := 0; i < catalog.MaxAttempts; i++ {
		if c, err := store.Claim(ctx, "dead", time.Minute); err != nil || c == nil {
			t.Fatalf("claim %d = %v, %v", i, c, err)
		}
		_, _ = db.ExecContext(ctx, `UPDATE ingestion_jobs SET lease_expires_at = now() - interval '1 minute' WHERE id = $1`, job.ID)
	}
	if c, err := store.Claim(ctx, "w", time.Minute); err != nil || c != nil {
		t.Fatalf("exhausted claim = %v, %v", c, err)
	}
	got, _ := store.GetJob(ctx, job.ID)
	if got.Status != catalog.JobFailed || !got.Retryable || got.FailureReason != "worker_interrupted" {
		t.Errorf("job = %+v", got)
	}
}

func TestPostgresConcurrentClaimsAreExclusive(t *testing.T) {
	db := pgDB(t)
	ctx := context.Background()
	store := catalog.NewPostgresIngestStore(db)
	objs := newMemObjects()
	const n = 6
	for i := 0; i < n; i++ {
		pgSubmit(t, store, objs, "ds-"+string(rune('a'+i)), "a\n1\n", nil)
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, err := store.Claim(ctx, "w", time.Minute)
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if j == nil {
					return
				}
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("job %s claimed %d times", id, c)
		}
	}
}

func TestPostgresNewVersionOfExistingDatasetAndConcurrentCreate(t *testing.T) {
	db := pgDB(t)
	ctx := context.Background()
	store := catalog.NewPostgresIngestStore(db)
	objs := newMemObjects()

	var wg sync.WaitGroup
	versions := make(chan int, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := mustUUID()
			job, _, err := store.CreateConvert(ctx, catalog.CreateConvert{JobID: id, Name: "same", InputRef: "uploads/x/" + id, InputFormat: "csv"})
			if err != nil {
				t.Errorf("CreateConvert: %v", err)
				return
			}
			versions <- job.Version
		}()
	}
	wg.Wait()
	close(versions)
	seen := map[int]bool{}
	for v := range versions {
		if seen[v] {
			t.Errorf("duplicate version number %d", v)
		}
		seen[v] = true
	}
	if len(seen) != 4 {
		t.Errorf("versions = %v", seen)
	}
	_ = objs
}

var errTransient = objectstore.ErrUnavailable

func nilLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mustUUID() string {
	id, err := catalog.NewUUID()
	if err != nil {
		panic(err)
	}
	return id
}
