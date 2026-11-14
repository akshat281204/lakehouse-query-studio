package catalog

import (
	"context"
	"errors"
	"time"
)

// Ingestion limits and defaults.
const (
	// MaxAttempts bounds how many times a job may be claimed (initial claim plus
	// crash recoveries) before it is failed as retryable.
	MaxAttempts = 3

	// KindConvert jobs convert an uploaded CSV/JSON/JSONL file.
	InputFormatCSV   = "csv"
	InputFormatJSONL = "jsonl"
	InputFormatJSON  = "json"
)

// ErrFenced is returned when a worker tries to transition a job it no longer
// holds (its lease expired and the job was reclaimed, or it is already
// terminal). The stale worker must discard its work.
var ErrFenced = errors.New("catalog: job fenced")

// Job is a durable ingestion job.
type Job struct {
	ID               string     `json:"id"`
	DatasetID        string     `json:"datasetId"`
	DatasetName      string     `json:"datasetName"`
	VersionID        string     `json:"versionId"`
	Version          int        `json:"version"`
	Kind             string     `json:"kind"`
	Status           string     `json:"status"`
	InputRef         string     `json:"-"`
	InputFormat      string     `json:"inputFormat"`
	RetainOriginal   bool       `json:"-"`
	PartitionColumns []string   `json:"partitionColumns"`
	FailureReason    string     `json:"failureReason,omitempty"`
	ErrorSummary     string     `json:"errorSummary,omitempty"`
	Retryable        bool       `json:"retryable"`
	RetryOf          string     `json:"retryOf,omitempty"`
	Attempt          int        `json:"attempt"`
	FencingToken     string     `json:"-"`
	CreatedAt        time.Time  `json:"createdAt"`
	StartedAt        *time.Time `json:"startedAt,omitempty"`
	FinishedAt       *time.Time `json:"finishedAt,omitempty"`
}

// CreateConvert describes a new conversion job. The upload has already been
// written to object storage at InputRef under JobID.
type CreateConvert struct {
	JobID            string
	Name             string
	InputRef         string
	InputFormat      string
	PartitionColumns []string
	RetainOriginal   bool
	RetryOf          string
}

// Publish is the atomic result of a successful conversion.
type Publish struct {
	ObjectPrefix     string
	PartitionColumns []string
	RowCount         int64
	Columns          []SchemaColumn
}

// IngestStore is the durable job surface used by the upload API and worker.
type IngestStore interface {
	// CreateConvert creates the dataset when new (or targets the next version of
	// an existing ACTIVE dataset), a PENDING version, and a QUEUED job.
	CreateConvert(ctx context.Context, req CreateConvert) (Job, Dataset, error)
	// Retry creates a new QUEUED job (fresh job id and version number) for a
	// FAILED retryable job. The caller supplies the new job id.
	Retry(ctx context.Context, sourceJobID, newJobID string) (Job, error)
	// GetJob returns a job by id.
	GetJob(ctx context.Context, id string) (Job, error)
	// ListJobs returns the most recent jobs, newest first.
	ListJobs(ctx context.Context, limit int) ([]Job, error)
	// Claim leases the next runnable job: a QUEUED job, or a RUNNING job whose
	// lease expired (crash recovery). It returns (nil, nil) when none is ready.
	Claim(ctx context.Context, owner string, lease time.Duration) (*Job, error)
	// Complete publishes the version READY and the job SUCCEEDED in one fenced
	// transaction. It returns ErrFenced if the token is stale.
	Complete(ctx context.Context, jobID, fencingToken string, p Publish) error
	// Fail marks the job and its version FAILED in one fenced transaction.
	Fail(ctx context.Context, jobID, fencingToken, reason, summary string, retryable bool) error
}
