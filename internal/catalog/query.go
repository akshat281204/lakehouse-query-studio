package catalog

import (
	"context"
	"errors"
	"time"
)

// Query job states (design §13).
const (
	QueryQueued    = "QUEUED"
	QueryRunning   = "RUNNING"
	QuerySucceeded = "SUCCEEDED"
	QueryFailed    = "FAILED"
	QueryCancelled = "CANCELLED"
	QueryTimedOut  = "TIMED_OUT"
)

// Query failure reasons recorded on FAILED jobs.
const (
	ReasonEngineError       = "engine_error"
	ReasonObjectStore       = "object_store"
	ReasonResourceExceeded  = "resource_exceeded"
	ReasonDenied            = "query_denied"
	ReasonWorkerInterrupted = "worker_interrupted"
	ReasonInternal          = "internal"
)

// MaxQueryAttempts bounds how many times a query is claimed (first claim plus
// crash recoveries) before it is failed instead of requeued, so a statement
// that kills its worker cannot loop forever.
const MaxQueryAttempts = 3

var (
	// ErrInvalidState: the requested transition is not valid from the current
	// state (for example cancelling a finished query).
	ErrInvalidState = errors.New("catalog: invalid state")
	// ErrQueueFull: the combined queue is at its depth limit.
	ErrQueueFull = errors.New("catalog: queue full")
)

// IsTerminalQuery reports whether a query status is final.
func IsTerminalQuery(status string) bool {
	switch status {
	case QuerySucceeded, QueryFailed, QueryCancelled, QueryTimedOut:
		return true
	}
	return false
}

// QueryJob is a durable query.
type QueryJob struct {
	ID                string            `json:"id"`
	Statement         string            `json:"statement"`
	StatementHash     string            `json:"statementHash"`
	Bindings          map[string]string `json:"-"`
	DatasetVersionIDs []string          `json:"datasetVersionIds"`
	Status            string            `json:"status"`
	FailureReason     string            `json:"failureReason,omitempty"`
	ErrorSummary      string            `json:"errorSummary,omitempty"`
	RowsReturned      *int64            `json:"rowsReturned,omitempty"`
	Truncated         bool              `json:"truncated"`
	ResultRef         string            `json:"-"`
	ResultBytes       *int64            `json:"resultBytes,omitempty"`
	BytesStaged       *int64            `json:"bytesStaged,omitempty"`
	PlanSummary       string            `json:"planSummary,omitempty"`
	DurationMs        *int64            `json:"durationMs,omitempty"`
	Attempt           int               `json:"attempt"`
	FencingToken      string            `json:"-"`
	CancelRequested   bool              `json:"cancelRequested"`
	CreatedAt         time.Time         `json:"createdAt"`
	StartedAt         *time.Time        `json:"startedAt,omitempty"`
	FinishedAt        *time.Time        `json:"finishedAt,omitempty"`
}

// SubmitQuery is a validated statement to queue. Bindings maps each relation
// name the statement uses to a READY dataset version id.
type SubmitQuery struct {
	ID            string
	Statement     string
	StatementHash string
	Bindings      map[string]string
	// MaxQueued rejects the submission with ErrQueueFull when the combined
	// count of QUEUED ingestion and query jobs has reached it. Zero disables.
	MaxQueued int
}

// QueryResult is recorded atomically with SUCCEEDED, after the spool exists.
type QueryResult struct {
	ResultRef    string
	RowsReturned int64
	Truncated    bool
	ResultBytes  int64
	BytesStaged  int64
	PlanSummary  string
	Duration     time.Duration
}

// Relation is a READY dataset version a query may read.
type Relation struct {
	DatasetName      string
	Version          int
	VersionID        string
	ObjectPrefix     string
	Format           string
	PartitionColumns []string
	// HivePartitioned is true for registered data whose partition values live
	// only in directory names. Converted data keeps partition columns in the
	// files, so it must not be hive-decoded a second time.
	HivePartitioned bool
}

// QueryStore is the durable query-job surface used by the API and worker.
type QueryStore interface {
	// ResolveRelation maps `<dataset>` (latest READY version) or
	// `<dataset>_v<n>` to a READY version of an ACTIVE dataset. Unknown or
	// not-ready relations return ErrNotFound.
	ResolveRelation(ctx context.Context, name string) (Relation, error)
	// RelationByVersionID returns the relation for a version id (worker side).
	RelationByVersionID(ctx context.Context, versionID string) (Relation, error)

	Submit(ctx context.Context, req SubmitQuery) (QueryJob, error)
	GetQuery(ctx context.Context, id string) (QueryJob, error)
	ListQueries(ctx context.Context, offset, limit int) ([]QueryJob, error)

	// ClaimQuery leases the next QUEUED query. It returns (nil, nil) when none.
	ClaimQuery(ctx context.Context, owner string, lease time.Duration) (*QueryJob, error)
	// RecoverExpired requeues RUNNING queries whose lease expired (or fails
	// them after MaxQueryAttempts) and returns how many it touched.
	RecoverExpired(ctx context.Context) (int, error)
	// Heartbeat extends the lease and reports whether cancellation was
	// requested. ErrFenced means the caller no longer holds the job.
	Heartbeat(ctx context.Context, id, token string, lease time.Duration) (cancelRequested bool, err error)
	// Succeed records SUCCEEDED conditional on the token.
	Succeed(ctx context.Context, id, token string, r QueryResult) error
	// Finish records FAILED, CANCELLED or TIMED_OUT conditional on the token.
	Finish(ctx context.Context, id, token, status, reason, summary string, d time.Duration) error
	// RequestCancel cancels a QUEUED query immediately, or flags a RUNNING one
	// for its worker to stop. Terminal queries return ErrInvalidState.
	RequestCancel(ctx context.Context, id string) (QueryJob, error)
}
