package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/parquet"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

// Failure reasons recorded on jobs.
const (
	ReasonInvalidInput = "invalid_input"
	ReasonUploadGone   = "upload_missing"
	ReasonStorage      = "object_store_error"
	ReasonInternal     = "internal"
)

// Runner claims and executes conversion jobs. Run it from the worker process;
// it holds the only write access to the lake/ and staging/ prefixes.
type Runner struct {
	Store   catalog.IngestStore
	Objects objectstore.ReadWriter
	Owner   string
	Lease   time.Duration
	Limits  Limits
	Poll    time.Duration
	Log     *slog.Logger
	// Metrics is optional; labels come from closed sets only.
	Metrics *telemetry.Metrics
}

// NewRunner returns a Runner with the documented defaults.
func NewRunner(store catalog.IngestStore, objects objectstore.ReadWriter, owner string, log *slog.Logger) *Runner {
	return &Runner{
		Store: store, Objects: objects, Owner: owner, Lease: 15 * time.Minute,
		Limits: DefaultLimits, Poll: 2 * time.Second, Log: log,
	}
}

// Run processes jobs until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) {
	for ctx.Err() == nil {
		worked, err := r.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			r.Log.Error("ingestion loop error", "error", err.Error())
		}
		if worked && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(r.Poll):
		}
	}
}

// RunOnce claims and processes at most one job, reporting whether one ran.
func (r *Runner) RunOnce(ctx context.Context) (bool, error) {
	job, err := r.Store.Claim(ctx, r.Owner, r.Lease)
	if err != nil {
		return false, fmt.Errorf("claim job: %w", err)
	}
	if job == nil {
		return false, nil
	}
	r.process(ctx, job)
	return true, nil
}

// Key builders (docs/design.md §4).

func stagingPrefix(job *catalog.Job) string {
	return fmt.Sprintf("staging/%s/%s/%s/attempt=%d/", catalog.NamespaceDefault, job.DatasetName, job.ID, job.Attempt)
}

func lakePrefix(job *catalog.Job) string {
	return fmt.Sprintf("lake/%s/%s/version=%d/attempt=%d/", catalog.NamespaceDefault, job.DatasetName, job.Version, job.Attempt)
}

func (r *Runner) process(ctx context.Context, job *catalog.Job) {
	log := r.Log.With("job_id", job.ID, "dataset", job.DatasetName, "version", job.Version, "attempt", job.Attempt)
	log.Info("ingestion started")
	started := time.Now()
	record := func(outcome string) {
		if r.Metrics != nil {
			r.Metrics.IngestJobs.Inc(outcome)
			r.Metrics.IngestDuration.Observe(time.Since(started).Seconds())
		}
	}

	timeout := max(r.Lease-time.Minute, r.Lease/2)
	jctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var written []string // every key this attempt created
	prefix := lakePrefix(job)

	publish, err := r.convert(jctx, job, &written)
	if err == nil {
		err = r.Store.Complete(jctx, job.ID, job.FencingToken, publish)
		if errors.Is(err, catalog.ErrFenced) {
			log.Warn("job fenced; discarding attempt output")
			r.cleanup(ctx, written)
			record("abandoned")
			return
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			// Shutting down mid-job: leave it RUNNING; lease expiry recovers it.
			log.Warn("ingestion interrupted by shutdown")
			record("abandoned")
			return
		}
		// This attempt is the sole writer of its attempt-scoped keys and has
		// stopped writing, so removing them now cannot race a live write.
		r.cleanup(ctx, written)
		reason, summary, retryable := classifyFailure(err)
		log.Warn("ingestion failed", "reason", reason, "retryable", retryable)
		if ferr := r.Store.Fail(ctx, job.ID, job.FencingToken, reason, summary, retryable); ferr != nil && !errors.Is(ferr, catalog.ErrFenced) {
			log.Error("record failure", "error", ferr.Error())
		}
		record("failed")
		return
	}

	// Courtesy cleanup after the fenced READY commit.
	r.cleanup(ctx, stagingKeys(written, job))
	if !job.RetainOriginal && job.InputRef != "" {
		if derr := r.Objects.Delete(ctx, job.InputRef); derr != nil {
			log.Warn("delete original upload", "error", derr.Error())
		}
	}
	record("succeeded")
	log.Info("ingestion succeeded", "rows", publish.RowCount, "prefix", prefix)
}

func stagingKeys(written []string, job *catalog.Job) []string {
	var out []string
	sp := stagingPrefix(job)
	for _, k := range written {
		if len(k) >= len(sp) && k[:len(sp)] == sp {
			out = append(out, k)
		}
	}
	return out
}

func (r *Runner) convert(ctx context.Context, job *catalog.Job, written *[]string) (catalog.Publish, error) {
	data, err := r.Objects.Get(ctx, job.InputRef, r.Limits.MaxBytes)
	if err != nil {
		if errors.Is(err, objectstore.ErrTooLarge) {
			return catalog.Publish{}, &UserError{Msg: fmt.Sprintf("file exceeds the %d MiB upload limit", r.Limits.MaxBytes>>20)}
		}
		return catalog.Publish{}, err
	}

	table, err := Parse(job.InputFormat, data, r.Limits)
	if err != nil {
		return catalog.Publish{}, err
	}
	parts, err := Convert(table, job.PartitionColumns, r.Limits)
	if err != nil {
		return catalog.Publish{}, err
	}

	stage, lake := stagingPrefix(job), lakePrefix(job)
	// Stage every part first so a conversion or storage failure leaves nothing
	// under lake/; only then write the durable attempt prefix.
	for _, p := range parts {
		key := stage + p.RelPath
		if err := r.Objects.Put(ctx, key, p.Data); err != nil {
			*written = append(*written, key) // may have partially landed
			return catalog.Publish{}, err
		}
		*written = append(*written, key)
	}
	var rows int64
	for _, p := range parts {
		key := lake + p.RelPath
		*written = append(*written, key)
		if err := r.Objects.Put(ctx, key, p.Data); err != nil {
			return catalog.Publish{}, err
		}
		rows += int64(p.Rows)
	}
	if err := r.verify(ctx, lake, len(parts)); err != nil {
		return catalog.Publish{}, err
	}

	partSet := map[string]bool{}
	for _, p := range job.PartitionColumns {
		partSet[p] = true
	}
	cols := make([]catalog.SchemaColumn, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = catalog.SchemaColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable, IsPartition: partSet[c.Name]}
	}
	return catalog.Publish{ObjectPrefix: lake, PartitionColumns: job.PartitionColumns, RowCount: rows, Columns: cols}, nil
}

// verify checks that exactly the expected number of part objects are present
// under the attempt prefix before the version is published.
func (r *Runner) verify(ctx context.Context, prefix string, want int) error {
	got := 0
	token := ""
	for {
		res, err := r.Objects.List(ctx, objectstore.ListOptions{Prefix: prefix, Token: token})
		if err != nil {
			return err
		}
		got += len(res.Objects)
		if !res.Truncated {
			break
		}
		token = res.NextToken
	}
	if got != want {
		return fmt.Errorf("verify attempt prefix: found %d objects, expected %d", got, want)
	}
	return nil
}

func (r *Runner) cleanup(ctx context.Context, keys []string) {
	// Use a fresh short context so cleanup still runs after a job timeout.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	for _, k := range keys {
		if err := r.Objects.Delete(cctx, k); err != nil {
			r.Log.Warn("cleanup delete failed", "error", err.Error())
		}
	}
}

// classifyFailure maps an error to a stored reason, a safe summary (never raw
// internals, paths, or values), and whether a retry may help.
func classifyFailure(err error) (reason, summary string, retryable bool) {
	var ue *UserError
	switch {
	case errors.As(err, &ue):
		return ReasonInvalidInput, ue.Msg, false
	case errors.Is(err, objectstore.ErrNotFound):
		return ReasonUploadGone, "the uploaded file is no longer available; upload it again", false
	case errors.Is(err, objectstore.ErrUnavailable), errors.Is(err, objectstore.ErrAccessDenied):
		return ReasonStorage, "the object store was unavailable; retry the job", true
	case errors.Is(err, parquet.ErrWrite):
		return ReasonInternal, "the data could not be encoded as Parquet", false
	default:
		return ReasonInternal, "an internal error occurred; retry the job", true
	}
}
