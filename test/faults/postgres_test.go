//go:build integration

package faults

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/akshat281204/lakehouse-query-studio/internal/api"
	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
)

func openPG(t *testing.T) *sql.DB {
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
	return db
}

// A real API restart: the first process (pool, store, handler) is gone; a
// second one over the same PostgreSQL sees the queued job, a worker runs it,
// and the results are served by the second handler.
func TestAPIRestartAgainstPostgres(t *testing.T) {
	ctx := context.Background()
	db1 := openPG(t)
	if err := catalog.Migrate(ctx, db1); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"query_jobs", "ingestion_jobs", "schema_columns", "dataset_versions", "datasets"} {
		if _, err := db1.ExecContext(ctx, "DELETE FROM "+q); err != nil {
			t.Fatal(err)
		}
	}
	objects := objectstore.NewMemory()
	data, _ := parquet.Encode([]parquet.WriteColumn{{Name: "id", Type: parquet.TypeInt64}}, [][]any{{int64(1)}, {int64(2)}, {int64(3)}})
	_ = objects.Put(ctx, "data/t/part.parquet", data)
	if _, _, _, err := catalog.NewPostgresStore(db1).Register(ctx, catalog.RegisterRecord{
		Name: "t", SourcePrefix: "data/t", ObjectPrefix: "data/t", Format: catalog.FormatParquet,
		Columns: []catalog.SchemaColumn{{Name: "id", Type: "int64"}}}); err != nil {
		t.Fatal(err)
	}
	v, _ := query.NewValidator()
	defer func() { _ = v.Close() }()

	mk := func(db *sql.DB) http.Handler {
		return api.New(nil, objects, api.WithQueries(api.QueryDeps{
			Store: catalog.NewPostgresQueryStore(db), Validator: v, Objects: objects, Limits: api.DefaultQueryLimits()}))
	}
	do := func(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
		return rec
	}

	// Process 1 accepts the query, then dies before any worker ran it.
	h1 := mk(db1)
	rec := do(h1, "POST", "/api/v1/queries", `{"statement":"SELECT sum(id) AS s FROM t"}`)
	if rec.Code != 202 {
		t.Fatalf("submit: %d %s", rec.Code, rec.Body.String())
	}
	var job catalog.QueryJob
	_ = json.Unmarshal(rec.Body.Bytes(), &job)
	_ = db1.Close()

	// Process 2: new pool, new store, new handler.
	db2 := openPG(t)
	h2 := mk(db2)
	if rec := do(h2, "GET", "/api/v1/queries/"+job.ID, ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"status":"QUEUED"`) {
		t.Fatalf("queued job lost across restart: %d %s", rec.Code, rec.Body.String())
	}
	cfg := query.DefaultRunnerConfig()
	cfg.TmpDir = t.TempDir()
	runner, err := query.NewRunner(catalog.NewPostgresQueryStore(db2), objects, query.NewDuckDB(), v, "w", cfg, quiet())
	if err != nil {
		t.Fatal(err)
	}
	if ran, err := runner.RunOnce(ctx); err != nil || !ran {
		t.Fatalf("ran=%v err=%v", ran, err)
	}
	rec = do(h2, "GET", "/api/v1/queries/"+job.ID+"/results", "")
	var page struct{ Rows [][]any }
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if rec.Code != 200 || len(page.Rows) != 1 || page.Rows[0][0] != float64(6) {
		t.Fatalf("results: %d %s", rec.Code, rec.Body.String())
	}
	var status string
	if err := db2.QueryRow(`SELECT status FROM query_jobs WHERE id = $1`, job.ID).Scan(&status); err != nil || status != "SUCCEEDED" {
		t.Fatalf("persisted status = %q err=%v", status, err)
	}
}
