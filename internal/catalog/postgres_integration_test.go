//go:build integration

package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// This file is tagged "integration" and is excluded from `make verify`. It runs
// only against a live PostgreSQL via TEST_POSTGRES_URL.

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

func TestPostgresRegistrationSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	name := fmt.Sprintf("it-%d", time.Now().UnixNano())
	store := NewPostgresStore(db)
	rowCount := int64(10)
	rec := RegisterRecord{
		Name:             name,
		SourcePrefix:     "data/" + name,
		ObjectPrefix:     "data/" + name,
		Format:           FormatParquet,
		PartitionColumns: []string{"id"},
		RowCount:         &rowCount,
		Columns: []SchemaColumn{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
		},
	}

	dataset, version, cols, err := store.Register(ctx, rec)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Duplicate registration must be rejected.
	if _, _, _, err := store.Register(ctx, rec); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate Register() error = %v, want ErrAlreadyExists", err)
	}

	// A fresh store instance (simulating an API restart) reads the same record.
	fresh := NewPostgresStore(db)
	got, err := fresh.Get(ctx, dataset.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Name != name || got.SourcePrefix != rec.SourcePrefix {
		t.Errorf("Get() = %+v", got)
	}

	v, err := fresh.GetLatestVersion(ctx, dataset.ID)
	if err != nil {
		t.Fatalf("GetLatestVersion() error = %v", err)
	}
	if v.ID != version.ID || v.Version != 1 || len(v.PartitionColumns) != 1 {
		t.Errorf("GetLatestVersion() = %+v", v)
	}

	schema, err := fresh.GetSchema(ctx, version.ID)
	if err != nil {
		t.Fatalf("GetSchema() error = %v", err)
	}
	if len(schema) != 2 {
		t.Fatalf("GetSchema() = %d columns, want 2", len(schema))
	}
	if !schema[0].IsPartition {
		t.Errorf("schema[0].IsPartition = false, want true")
	}
	if len(cols) != 2 {
		t.Errorf("Register() returned %d columns, want 2", len(cols))
	}
}
