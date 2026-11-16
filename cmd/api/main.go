// Command api is the Lakehouse Query Studio control plane. It serves the
// service root, liveness/readiness probes, and the versioned dataset API, and
// shuts down gracefully.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/akshat281204/lakehouse-query-studio/internal/api"
	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/config"
	"github.com/akshat281204/lakehouse-query-studio/internal/health"
	"github.com/akshat281204/lakehouse-query-studio/internal/logging"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/query"
	"github.com/akshat281204/lakehouse-query-studio/internal/server"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	log, err := logging.New(os.Stdout, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var db *sql.DB
	var catalogStore catalog.Store
	var ingestStore catalog.IngestStore
	var queryStore catalog.QueryStore
	var savedStore catalog.SavedQueryStore
	if cfg.PostgresURL != "" {
		db, err = sql.Open("pgx", cfg.PostgresURL)
		if err != nil {
			log.Warn("postgres pool open failed; catalog will report unavailable", "error", err)
		} else {
			db.SetMaxOpenConns(8)
			db.SetConnMaxLifetime(5 * time.Minute)
			migCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := catalog.Migrate(migCtx, db); err != nil {
				log.Warn("catalog migration failed; dataset endpoints will report unavailable", "error", err)
			} else {
				catalogStore = catalog.NewPostgresStore(db)
				ingestStore = catalog.NewPostgresIngestStore(db)
				queryStore = catalog.NewPostgresQueryStore(db)
				savedStore = catalog.NewPostgresSavedQueryStore(db)
			}
			cancel()
		}
	}

	objects := objectstore.New(objectstore.Config{
		Endpoint:  cfg.MinIOEndpoint,
		AccessKey: cfg.MinIOAccessKey,
		SecretKey: cfg.MinIOSecretKey,
		Bucket:    cfg.MinIOBucket,
		Region:    "us-east-1",
		UseSSL:    cfg.MinIOUseSSL,
	})

	// The bucket may not exist on a clean store, and the store may still be
	// starting. Retry for a bounded time; failure is not fatal because
	// readiness reports the object store separately.
	if err := ensureBucket(ctx, objects, time.Minute); err != nil {
		log.Warn("ensure bucket failed", "error", err.Error())
	}

	// The validator parses SQL with DuckDB's own parser on a private, empty,
	// in-memory database. It never executes a statement and holds no data.
	validator, err := query.NewValidator()
	if err != nil {
		return fmt.Errorf("sql validator: %w", err)
	}
	defer func() { _ = validator.Close() }()

	metrics := telemetry.New(server.Version)

	srv := server.New(cfg, log, server.Deps{
		Metrics: metrics,
		Ready:   buildReady(db, objects),
		Catalog: catalogStore,
		Objects: objects,
		Ingest:  ingestStore,
		Writer:  objects,

		Queries:   queryStore,
		Saved:     savedStore,
		Validator: validator,
		QueryLimits: api.QueryLimits{
			MaxPageBytes:   cfg.Query.MaxPageBytes,
			MaxExportBytes: cfg.Query.MaxExportBytes,
			MaxExportRows:  minInt(cfg.Query.MaxExportRows, cfg.Query.MaxRows),
			MaxQueued:      cfg.Query.QueueDepth,
		},
	})
	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ensureBucket creates the bucket, retrying while the store is starting.
func ensureBucket(ctx context.Context, objects *objectstore.Client, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	for {
		bctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := objects.EnsureBucket(bctx)
		cancel()
		if err == nil || ctx.Err() != nil || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// buildReady constructs the aggregate readiness check. Dependencies are probed
// lazily on every /readyz request; a missing or unreachable dependency is
// reported as not-ready rather than failing startup. The object-store probe is
// a signed, bounded List call, so it proves connectivity, credentials and the
// bucket on any S3-compatible store.
func buildReady(db *sql.DB, objects *objectstore.Client) server.ReadyFunc {
	postgres := health.NewPostgres("postgres", nil)
	if db != nil {
		postgres = health.NewPostgres("postgres", db)
	}
	store := health.NewObjectStore("objectstore", func(ctx context.Context) error {
		_, err := objects.List(ctx, objectstore.ListOptions{MaxKeys: 1})
		return err
	})
	return health.NewAggregate(postgres, store).Check
}
