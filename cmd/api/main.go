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

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/config"
	"github.com/akshat281204/lakehouse-query-studio/internal/health"
	"github.com/akshat281204/lakehouse-query-studio/internal/logging"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/server"
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

	srv := server.New(cfg, log, server.Deps{
		Ready:   buildReady(cfg, db),
		Catalog: catalogStore,
		Objects: objects,
	})
	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

// buildReady constructs the aggregate readiness check. Dependencies are probed
// lazily on every /readyz request; a missing or unreachable dependency is
// reported as not-ready rather than failing startup.
func buildReady(cfg *config.Config, db *sql.DB) server.ReadyFunc {
	postgres := health.NewPostgres("postgres", nil)
	if db != nil {
		postgres = health.NewPostgres("postgres", db)
	}
	minio := health.NewMinIO("minio", cfg.MinIOEndpoint, cfg.MinIOUseSSL, nil)
	return health.NewAggregate(postgres, minio).Check
}
