package config

import (
	"fmt"
	"time"
)

// RetentionConfig holds retention windows (design §9, §10).
type RetentionConfig struct {
	// ExportTTL: spooled query results expire after this long (default 24h).
	ExportTTL time.Duration
	// HistoryRetention: finished query rows (statement included) are deleted
	// after this long (default 30 days).
	HistoryRetention time.Duration
	// StagingTTL: leftover staging/ objects older than this are removed.
	StagingTTL time.Duration
	// SweepInterval: how often the worker sweeps.
	SweepInterval time.Duration
}

func loadRetention() (RetentionConfig, error) {
	r := RetentionConfig{
		ExportTTL: 24 * time.Hour, HistoryRetention: 30 * 24 * time.Hour,
		StagingTTL: 24 * time.Hour, SweepInterval: 10 * time.Minute,
	}
	var err error
	for _, f := range []struct {
		key string
		dst *time.Duration
	}{{"EXPORT_TTL", &r.ExportTTL}, {"HISTORY_RETENTION", &r.HistoryRetention},
		{"STAGING_TTL", &r.StagingTTL}, {"SWEEP_INTERVAL", &r.SweepInterval}} {
		if *f.dst, err = getenvDuration(f.key, *f.dst); err != nil {
			return r, fmt.Errorf("%s: %w", f.key, err)
		}
	}
	return r, r.Validate()
}

// Validate enforces the invariants the sweepers rely on.
func (r RetentionConfig) Validate() error {
	switch {
	case r.ExportTTL < time.Minute:
		return fmt.Errorf("EXPORT_TTL must be at least 1m")
	case r.HistoryRetention < r.ExportTTL:
		return fmt.Errorf("HISTORY_RETENTION must not be shorter than EXPORT_TTL")
	case r.StagingTTL < 2*time.Hour:
		// Ingestion leases run up to 30 minutes; staging must outlive them.
		return fmt.Errorf("STAGING_TTL must be at least 2h so it outlives an ingestion lease")
	case r.SweepInterval < time.Second:
		return fmt.Errorf("SWEEP_INTERVAL must be at least 1s")
	}
	return nil
}
