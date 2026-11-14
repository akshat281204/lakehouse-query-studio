package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

// QueryConfig holds the query-engine knobs of design §9 and §12. Zero values
// are never used: Load fills every field from its default and validates the
// documented hard bounds, so a bad value fails startup instead of widening a
// limit.
type QueryConfig struct {
	Workers        int           // QUERY_WORKERS: concurrent RUNNING queries (1..16)
	Timeout        time.Duration // QUERY_TIMEOUT: execution budget (<= 300s)
	Lease          time.Duration // QUERY_LEASE_DURATION (<= 600s)
	Heartbeat      time.Duration // QUERY_HEARTBEAT_INTERVAL (default Lease/3)
	LeaseMargin    time.Duration // LEASE_RENEWAL_MARGIN
	MaxRows        int           // MAX_RESULT_ROWS (<= 100000)
	MaxResultBytes int64         // MAX_RESULT_BYTES (<= 1 GiB)
	MaxRowBytes    int64         // MAX_ROW_BYTES (<= 16 MiB)
	MaxPageBytes   int64         // MAX_PAGE_BYTES (<= 64 MiB)
	MaxExportBytes int64         // MAX_EXPORT_BYTES (<= 512 MiB)
	MaxExportRows  int           // MAX_EXPORT_ROWS
	MaxScanBytes   int64         // MAX_SCAN_BYTES: dataset bytes staged per query
	QueueDepth     int           // QUEUE_DEPTH: combined queued jobs
	Threads        int           // DUCKDB_THREADS
	Memory         string        // DUCKDB_MEMORY, e.g. "2GiB"
	TmpDir         string        // QUERY_TMP_DIR: per-query scratch parent
}

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
)

func loadQuery() (QueryConfig, error) {
	threads := runtime.NumCPU()
	if threads > 4 {
		threads = 4
	}
	q := QueryConfig{
		Workers: 2, Timeout: 60 * time.Second, Lease: 120 * time.Second, LeaseMargin: 30 * time.Second,
		MaxRows: 10_000, MaxResultBytes: 256 * mib, MaxRowBytes: mib, MaxPageBytes: 4 * mib,
		MaxExportBytes: 64 * mib, MaxExportRows: 50_000, MaxScanBytes: gib, QueueDepth: 100,
		Threads: threads, Memory: getenv("DUCKDB_MEMORY", "2GiB"), TmpDir: getenv("QUERY_TMP_DIR", os.TempDir()),
	}
	var err error
	ints := []struct {
		key string
		dst *int
	}{{"QUERY_WORKERS", &q.Workers}, {"MAX_RESULT_ROWS", &q.MaxRows}, {"MAX_EXPORT_ROWS", &q.MaxExportRows},
		{"QUEUE_DEPTH", &q.QueueDepth}, {"DUCKDB_THREADS", &q.Threads}}
	for _, f := range ints {
		if *f.dst, err = getenvInt(f.key, *f.dst); err != nil {
			return q, fmt.Errorf("%s: %w", f.key, err)
		}
	}
	int64s := []struct {
		key string
		dst *int64
	}{{"MAX_RESULT_BYTES", &q.MaxResultBytes}, {"MAX_ROW_BYTES", &q.MaxRowBytes}, {"MAX_PAGE_BYTES", &q.MaxPageBytes},
		{"MAX_EXPORT_BYTES", &q.MaxExportBytes}, {"MAX_SCAN_BYTES", &q.MaxScanBytes}}
	for _, f := range int64s {
		v, err := getenvInt(f.key, int(*f.dst))
		if err != nil {
			return q, fmt.Errorf("%s: %w", f.key, err)
		}
		*f.dst = int64(v)
	}
	durs := []struct {
		key string
		dst *time.Duration
	}{{"QUERY_TIMEOUT", &q.Timeout}, {"QUERY_LEASE_DURATION", &q.Lease}, {"LEASE_RENEWAL_MARGIN", &q.LeaseMargin}}
	for _, f := range durs {
		if *f.dst, err = getenvDuration(f.key, *f.dst); err != nil {
			return q, fmt.Errorf("%s: %w", f.key, err)
		}
	}
	q.Heartbeat = q.Lease / 3
	if q.Heartbeat, err = getenvDuration("QUERY_HEARTBEAT_INTERVAL", q.Heartbeat); err != nil {
		return q, fmt.Errorf("QUERY_HEARTBEAT_INTERVAL: %w", err)
	}
	return q, q.Validate()
}

// Validate enforces the hard maximums of design §9/§12.
func (q QueryConfig) Validate() error {
	switch {
	case q.Workers < 1 || q.Workers > 16:
		return fmt.Errorf("QUERY_WORKERS must be between 1 and 16 (got %d)", q.Workers)
	case q.Timeout <= 0 || q.Timeout > 300*time.Second:
		return fmt.Errorf("QUERY_TIMEOUT must be positive and at most 300s")
	case q.Lease <= 0 || q.Lease > 600*time.Second:
		return fmt.Errorf("QUERY_LEASE_DURATION must be positive and at most 600s")
	case q.Heartbeat <= 0 || q.Heartbeat+q.LeaseMargin >= q.Lease:
		return fmt.Errorf("QUERY_HEARTBEAT_INTERVAL + LEASE_RENEWAL_MARGIN must be shorter than QUERY_LEASE_DURATION")
	case q.MaxRows < 1 || q.MaxRows > 100_000:
		return fmt.Errorf("MAX_RESULT_ROWS must be between 1 and 100000")
	case q.MaxResultBytes < 1 || q.MaxResultBytes > gib:
		return fmt.Errorf("MAX_RESULT_BYTES must be between 1 and 1 GiB")
	case q.MaxRowBytes < 1 || q.MaxRowBytes > 16*mib:
		return fmt.Errorf("MAX_ROW_BYTES must be between 1 and 16 MiB")
	case q.MaxRowBytes >= q.MaxResultBytes:
		return fmt.Errorf("MAX_ROW_BYTES must be smaller than MAX_RESULT_BYTES")
	case q.MaxPageBytes < 1 || q.MaxPageBytes > 64*mib:
		return fmt.Errorf("MAX_PAGE_BYTES must be between 1 and 64 MiB")
	case q.MaxExportBytes < 1 || q.MaxExportBytes > 512*mib:
		return fmt.Errorf("MAX_EXPORT_BYTES must be between 1 and 512 MiB")
	case q.MaxExportRows < 1:
		return fmt.Errorf("MAX_EXPORT_ROWS must be positive (it is also capped by MAX_RESULT_ROWS)")
	case q.MaxScanBytes < 1:
		return fmt.Errorf("MAX_SCAN_BYTES must be positive")
	case q.QueueDepth < 1:
		return fmt.Errorf("QUEUE_DEPTH must be positive")
	case q.Threads < 1:
		return fmt.Errorf("DUCKDB_THREADS must be positive")
	}
	return nil
}

func getenvInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q", v)
	}
	return n, nil
}
