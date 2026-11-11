// Command api is the Lakehouse Query Studio control plane. Phase 1 serves the
// service root plus liveness and readiness probes and shuts down gracefully.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/akshat281204/lakehouse-query-studio/internal/config"
	"github.com/akshat281204/lakehouse-query-studio/internal/health"
	"github.com/akshat281204/lakehouse-query-studio/internal/logging"
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

	srv := server.New(cfg, log, buildReady(cfg, log))
	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

// buildReady constructs the aggregate readiness check. Dependencies are probed
// lazily on every /readyz request; a missing or unreachable dependency is
// reported as not-ready rather than failing startup.
func buildReady(cfg *config.Config, log *slog.Logger) server.ReadyFunc {
	postgres := health.NewPostgres("postgres", nil)
	if cfg.PostgresURL != "" {
		db, err := sql.Open("pgx", cfg.PostgresURL)
		if err != nil {
			log.Warn("postgres pool open failed; readiness will report failure", "error", err)
		} else {
			db.SetMaxOpenConns(2)
			db.SetConnMaxLifetime(5 * time.Minute)
			postgres = health.NewPostgres("postgres", db)
		}
	}

	minio := health.NewMinIO("minio", cfg.MinIOEndpoint, cfg.MinIOUseSSL, nil)
	return health.NewAggregate(postgres, minio).Check
}
