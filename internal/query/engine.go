// Package query validates and executes bounded, read-only SQL over registered
// datasets. The engine boundary (Engine) hides DuckDB so a different engine
// could be swapped in if a measured need ever justified one. The first release
// is single-node DuckDB; nothing here shards, replicates or scales out.
package query

import (
	"context"
	"errors"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
)

// Sentinel errors carried from the engine to the job runner, which maps them
// to terminal job states and failure reasons.
var (
	ErrTimeout   = errors.New("query timed out")
	ErrCancelled = errors.New("query cancelled")
	ErrLimit     = errors.New("query exceeded a resource limit")
	ErrEngine    = errors.New("query failed")
)

// Limits are the per-query server-side bounds (design §9, §12).
type Limits struct {
	Timeout        time.Duration
	MaxRows        int   // MAX_RESULT_ROWS: rows returned; one more is probed
	MaxResultBytes int64 // MAX_RESULT_BYTES: finalized spool size
	MaxRowBytes    int64 // MAX_ROW_BYTES: one row's encoded size
	MaxScanBytes   int64 // total dataset bytes staged for a query
	Threads        int
	MemoryLimit    string // DuckDB memory_limit, e.g. "2GiB"
}

// DefaultLimits returns the design defaults.
func DefaultLimits() Limits {
	return Limits{
		Timeout:        60 * time.Second,
		MaxRows:        10_000,
		MaxResultBytes: 256 << 20,
		MaxRowBytes:    1 << 20,
		MaxScanBytes:   1 << 30,
		Threads:        4,
		MemoryLimit:    "2GiB",
	}
}

// Source binds a relation name to a directory of Parquet files staged on
// local disk by trusted worker code. Names are the relation names user SQL may
// reference (e.g. "sales" and "sales_v2"). The directory is never visible to
// user SQL.
type Source struct {
	Names            []string
	Dir              string
	HivePartitioning bool
}

// Request is one query execution.
type Request struct {
	Statement string
	Sources   []Source
	// WorkDir is a private scratch directory (DuckDB spill files). Together
	// with the Source directories it is the only filesystem the engine may
	// touch.
	WorkDir string
	Limits  Limits
}

// Result is a fully materialized, bounded result.
type Result struct {
	Columns     []parquet.WriteColumn
	Rows        [][]any
	Truncated   bool
	PlanSummary string
}

// Engine executes a validated statement. Implementations must enforce the
// limits and honour context cancellation.
type Engine interface {
	Execute(ctx context.Context, req Request) (*Result, error)
}
