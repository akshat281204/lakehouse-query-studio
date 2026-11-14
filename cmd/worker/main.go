// Command worker runs the asynchronous ingestion jobs: it claims uploaded
// CSV/JSON/JSONL conversion jobs from PostgreSQL, writes versioned Parquet to
// object storage, and publishes the dataset version atomically. Query
// execution joins this process in a later phase.
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

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/config"
	"github.com/akshat281204/lakehouse-query-studio/internal/ingest"
	"github.com/akshat281204/lakehouse-query-studio/internal/logging"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
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

	if cfg.PostgresURL == "" {
		return fmt.Errorf("POSTGRES_URL is required for the worker")
	}
	db, err := sql.Open("pgx", cfg.PostgresURL)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(4)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Wait for the schema: the API owns migration, but whichever process starts
	// first applies it under an advisory lock, so this is safe to repeat.
	if err := migrateWithRetry(ctx, db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	objects := objectstore.New(objectstore.Config{
		Endpoint:  cfg.MinIOEndpoint,
		AccessKey: cfg.MinIOAccessKey,
		SecretKey: cfg.MinIOSecretKey,
		Bucket:    cfg.MinIOBucket,
		Region:    "us-east-1",
		UseSSL:    cfg.MinIOUseSSL,
	})
	bctx, bcancel := context.WithTimeout(ctx, 10*time.Second)
	if err := objects.EnsureBucket(bctx); err != nil {
		log.Warn("ensure bucket failed", "error", err.Error())
	}
	bcancel()

	host, _ := os.Hostname()
	owner := fmt.Sprintf("%s-%d", host, os.Getpid())
	runner := ingest.NewRunner(catalog.NewPostgresIngestStore(db), objects, owner, log)

	log.Info("worker started", "owner", owner)
	runner.Run(ctx)
	log.Info("worker shutting down")
	return nil
}

func migrateWithRetry(ctx context.Context, db *sql.DB) error {
	var err error
	for i := 0; i < 10; i++ {
		mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = catalog.Migrate(mctx, db)
		cancel()
		if err == nil || ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return err
}
