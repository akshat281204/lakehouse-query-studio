package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// NewPostgresIngestStore returns the PostgreSQL-backed IngestStore.
func NewPostgresIngestStore(db *sql.DB) IngestStore { return &postgresStore{db: db} }

const jobSelect = `
	SELECT j.id, j.dataset_id, d.name, j.target_version_id, v.version, j.kind, j.status,
	       COALESCE(j.input_ref, ''), COALESCE(j.input_format, ''), j.partition_columns,
	       COALESCE(j.failure_reason, ''), COALESCE(j.error_summary, ''), j.retryable,
	       COALESCE(j.retry_of::text, ''), j.attempt, COALESCE(j.fencing_token::text, ''),
	       j.created_at, j.started_at, j.finished_at, d.retain_original
	FROM ingestion_jobs j
	JOIN datasets d ON d.id = j.dataset_id
	JOIN dataset_versions v ON v.id = j.target_version_id`

func scanJob(row rowScanner) (Job, error) {
	var j Job
	var parts []byte
	var started, finished sql.NullTime
	if err := row.Scan(&j.ID, &j.DatasetID, &j.DatasetName, &j.VersionID, &j.Version, &j.Kind, &j.Status,
		&j.InputRef, &j.InputFormat, &parts, &j.FailureReason, &j.ErrorSummary, &j.Retryable,
		&j.RetryOf, &j.Attempt, &j.FencingToken, &j.CreatedAt, &started, &finished, &j.RetainOriginal); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, fmt.Errorf("%w: job", ErrNotFound)
		}
		return Job{}, fmt.Errorf("scan job: %w", err)
	}
	if err := json.Unmarshal(parts, &j.PartitionColumns); err != nil {
		return Job{}, fmt.Errorf("decode job partition columns: %w", err)
	}
	if started.Valid {
		t := started.Time
		j.StartedAt = &t
	}
	if finished.Valid {
		t := finished.Time
		j.FinishedAt = &t
	}
	return j, nil
}

func (s *postgresStore) GetLatestReadyVersion(ctx context.Context, datasetID string) (DatasetVersion, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, dataset_id, version, object_prefix, format, partition_columns, status, error_summary, row_count, created_at
		FROM dataset_versions
		WHERE dataset_id = $1 AND status = $2
		ORDER BY version DESC
		LIMIT 1`, datasetID, VersionReady)
	return scanVersion(row)
}

func (s *postgresStore) GetJob(ctx context.Context, id string) (Job, error) {
	if !validUUID(id) {
		return Job{}, fmt.Errorf("%w: job", ErrNotFound)
	}
	return scanJob(s.db.QueryRowContext(ctx, jobSelect+` WHERE j.id = $1`, id))
}

func (s *postgresStore) ListJobs(ctx context.Context, limit int) ([]Job, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, jobSelect+` ORDER BY j.created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	jobs := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func lakeBasePrefix(namespace, dataset string, version int) string {
	return fmt.Sprintf("lake/%s/%s/version=%d/", namespace, dataset, version)
}

// newVersionAndJob inserts the next PENDING version and a QUEUED job for an
// already-locked dataset inside tx.
func newVersionAndJob(ctx context.Context, tx *sql.Tx, ds Dataset, req CreateConvert) (string, error) {
	var next int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) + 1 FROM dataset_versions WHERE dataset_id = $1`, ds.ID).Scan(&next); err != nil {
		return "", fmt.Errorf("next version: %w", err)
	}
	versionID, err := newUUID()
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	if err := insertVersion(ctx, tx, DatasetVersion{
		ID:               versionID,
		DatasetID:        ds.ID,
		Version:          next,
		ObjectPrefix:     lakeBasePrefix(ds.NamespaceSlug, ds.Name, next),
		Format:           FormatParquet,
		PartitionColumns: orEmpty(req.PartitionColumns),
		Status:           VersionPending,
		CreatedAt:        now,
	}); err != nil {
		return "", fmt.Errorf("insert version: %w", err)
	}
	parts, err := json.Marshal(orEmpty(req.PartitionColumns))
	if err != nil {
		return "", err
	}
	var retryOf any
	if req.RetryOf != "" {
		retryOf = req.RetryOf
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingestion_jobs (id, dataset_id, target_version_id, kind, status, input_ref, input_format, partition_columns, retry_of, attempt, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, $10)`,
		req.JobID, ds.ID, versionID, KindConvert, JobQueued, req.InputRef, req.InputFormat, parts, retryOf, now); err != nil {
		return "", fmt.Errorf("insert job: %w", err)
	}
	return versionID, nil
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (s *postgresStore) CreateConvert(ctx context.Context, req CreateConvert) (Job, Dataset, error) {
	ns, err := s.defaultNamespace(ctx)
	if err != nil {
		return Job{}, Dataset{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, Dataset{}, fmt.Errorf("begin create job: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ds, err := lockOrCreateDataset(ctx, tx, ns, req)
	if err != nil {
		return Job{}, Dataset{}, err
	}
	if _, err := newVersionAndJob(ctx, tx, ds, req); err != nil {
		return Job{}, Dataset{}, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, Dataset{}, fmt.Errorf("commit create job: %w", err)
	}
	job, err := s.GetJob(ctx, req.JobID)
	return job, ds, err
}

// lockOrCreateDataset returns the ACTIVE dataset named req.Name locked for
// update, creating it when it does not exist.
func lockOrCreateDataset(ctx context.Context, tx *sql.Tx, ns Namespace, req CreateConvert) (Dataset, error) {
	for attempt := 0; attempt < 2; attempt++ {
		var ds Dataset
		err := tx.QueryRowContext(ctx, `
			SELECT id, namespace_id, name, source_prefix, format, status, retain_original, created_at, updated_at
			FROM datasets WHERE namespace_id = $1 AND name = $2 FOR UPDATE`, ns.ID, req.Name).
			Scan(&ds.ID, &ds.NamespaceID, &ds.Name, &ds.SourcePrefix, &ds.Format, &ds.Status, &ds.RetainOriginal, &ds.CreatedAt, &ds.UpdatedAt)
		if err == nil {
			if ds.Status != DatasetActive {
				return Dataset{}, fmt.Errorf("%w: dataset %q is not active", ErrInvalidArgument, req.Name)
			}
			ds.NamespaceSlug = ns.Slug
			return ds, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Dataset{}, fmt.Errorf("lock dataset: %w", err)
		}
		id, err := newUUID()
		if err != nil {
			return Dataset{}, err
		}
		now := time.Now().UTC()
		ds = Dataset{
			ID: id, NamespaceID: ns.ID, NamespaceSlug: ns.Slug, Name: req.Name,
			SourcePrefix: fmt.Sprintf("uploads/%s/%s/", ns.Slug, req.Name), Format: FormatParquet,
			Status: DatasetActive, RetainOriginal: req.RetainOriginal, CreatedAt: now, UpdatedAt: now,
		}
		// A savepoint keeps the transaction usable if a concurrent creator wins.
		if _, err := tx.ExecContext(ctx, `SAVEPOINT create_dataset`); err != nil {
			return Dataset{}, err
		}
		if err := insertDataset(ctx, tx, ds); err != nil {
			if isUniqueViolation(err) {
				if _, rerr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT create_dataset`); rerr != nil {
					return Dataset{}, rerr
				}
				continue
			}
			return Dataset{}, fmt.Errorf("insert dataset: %w", err)
		}
		return ds, nil
	}
	return Dataset{}, fmt.Errorf("%w: dataset %q", ErrAlreadyExists, req.Name)
}

func (s *postgresStore) Retry(ctx context.Context, sourceJobID, newJobID string) (Job, error) {
	if !validUUID(sourceJobID) {
		return Job{}, fmt.Errorf("%w: job", ErrNotFound)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, fmt.Errorf("begin retry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var datasetID, status, inputRef, inputFormat string
	var retryable bool
	var parts []byte
	err = tx.QueryRowContext(ctx, `
		SELECT dataset_id, status, retryable, COALESCE(input_ref, ''), COALESCE(input_format, ''), partition_columns
		FROM ingestion_jobs WHERE id = $1 AND kind = $2 FOR UPDATE`, sourceJobID, KindConvert).
		Scan(&datasetID, &status, &retryable, &inputRef, &inputFormat, &parts)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, fmt.Errorf("%w: job", ErrNotFound)
	}
	if err != nil {
		return Job{}, fmt.Errorf("lock job: %w", err)
	}
	if status != JobFailed || !retryable {
		return Job{}, fmt.Errorf("%w: only failed, retryable jobs can be retried", ErrInvalidArgument)
	}
	var existing int
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*) FROM ingestion_jobs WHERE retry_of = $1 AND status IN ('QUEUED','RUNNING','SUCCEEDED')`, sourceJobID).Scan(&existing); err != nil {
		return Job{}, fmt.Errorf("check retries: %w", err)
	}
	if existing > 0 {
		return Job{}, fmt.Errorf("%w: job already retried", ErrAlreadyExists)
	}
	var partition []string
	if err := json.Unmarshal(parts, &partition); err != nil {
		return Job{}, fmt.Errorf("decode partition columns: %w", err)
	}

	var ds Dataset
	if err := tx.QueryRowContext(ctx, `
		SELECT d.id, d.namespace_id, n.slug, d.name, d.source_prefix, d.format, d.status, d.retain_original, d.created_at, d.updated_at
		FROM datasets d JOIN namespaces n ON n.id = d.namespace_id WHERE d.id = $1 FOR UPDATE OF d`, datasetID).
		Scan(&ds.ID, &ds.NamespaceID, &ds.NamespaceSlug, &ds.Name, &ds.SourcePrefix, &ds.Format, &ds.Status, &ds.RetainOriginal, &ds.CreatedAt, &ds.UpdatedAt); err != nil {
		return Job{}, fmt.Errorf("lock dataset: %w", err)
	}
	if ds.Status != DatasetActive {
		return Job{}, fmt.Errorf("%w: dataset %q is not active", ErrInvalidArgument, ds.Name)
	}
	if _, err := newVersionAndJob(ctx, tx, ds, CreateConvert{
		JobID: newJobID, Name: ds.Name, InputRef: inputRef, InputFormat: inputFormat,
		PartitionColumns: partition, RetryOf: sourceJobID,
	}); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, fmt.Errorf("commit retry: %w", err)
	}
	return s.GetJob(ctx, newJobID)
}

func (s *postgresStore) Claim(ctx context.Context, owner string, lease time.Duration) (*Job, error) {
	for i := 0; i < 8; i++ {
		job, again, err := s.claimOnce(ctx, owner, lease)
		if err != nil {
			return nil, err
		}
		if job != nil || !again {
			return job, nil
		}
	}
	return nil, nil
}

// claimOnce claims one job. again is true when it failed an exhausted job and
// the caller should look for the next runnable one.
func (s *postgresStore) claimOnce(ctx context.Context, owner string, lease time.Duration) (*Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var id, status string
	var attempt int
	var versionID string
	err = tx.QueryRowContext(ctx, `
		SELECT id, status, attempt, target_version_id FROM ingestion_jobs
		WHERE kind = $1 AND (status = $2 OR (status = $3 AND lease_expires_at < now()))
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`, KindConvert, JobQueued, JobRunning).Scan(&id, &status, &attempt, &versionID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("select claimable job: %w", err)
	}

	if status == JobRunning && attempt >= MaxAttempts {
		// The worker repeatedly died on this job: stop retrying it silently.
		if _, err := tx.ExecContext(ctx, `
			UPDATE ingestion_jobs SET status = $2, retryable = true, failure_reason = 'worker_interrupted',
			       error_summary = 'ingestion was interrupted repeatedly; retry the job', finished_at = now(),
			       lease_owner = NULL, lease_expires_at = NULL
			WHERE id = $1`, id, JobFailed); err != nil {
			return nil, false, fmt.Errorf("fail exhausted job: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE dataset_versions SET status = $2, error_summary = 'ingestion was interrupted repeatedly' WHERE id = $1`,
			versionID, VersionFailed); err != nil {
			return nil, false, fmt.Errorf("fail exhausted version: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit exhausted job: %w", err)
		}
		return nil, true, nil
	}

	token, err := newUUID()
	if err != nil {
		return nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE ingestion_jobs SET status = $2, attempt = attempt + 1, fencing_token = $3, lease_owner = $4,
		       lease_expires_at = now() + make_interval(secs => $5), started_at = COALESCE(started_at, now())
		WHERE id = $1`, id, JobRunning, token, owner, lease.Seconds()); err != nil {
		return nil, false, fmt.Errorf("claim job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dataset_versions SET status = $2 WHERE id = $1`, versionID, VersionBuilding); err != nil {
		return nil, false, fmt.Errorf("mark version building: %w", err)
	}
	job, err := scanJob(tx.QueryRowContext(ctx, jobSelect+` WHERE j.id = $1`, id))
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit claim: %w", err)
	}
	return &job, false, nil
}

func (s *postgresStore) Complete(ctx context.Context, jobID, token string, p Publish) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin complete: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var versionID, datasetID string
	err = tx.QueryRowContext(ctx, `
		UPDATE ingestion_jobs SET status = $3, finished_at = now(), lease_owner = NULL, lease_expires_at = NULL,
		       retryable = false, failure_reason = NULL, error_summary = NULL
		WHERE id = $1 AND fencing_token = $2 AND status = $4
		RETURNING target_version_id, dataset_id`, jobID, token, JobSucceeded, JobRunning).Scan(&versionID, &datasetID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrFenced
	}
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	parts, err := json.Marshal(orEmpty(p.PartitionColumns))
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE dataset_versions SET status = $2, object_prefix = $3, row_count = $4, partition_columns = $5, error_summary = NULL
		WHERE id = $1`, versionID, VersionReady, p.ObjectPrefix, p.RowCount, parts); err != nil {
		return fmt.Errorf("publish version: %w", err)
	}
	for i, c := range p.Columns {
		id, err := newUUID()
		if err != nil {
			return err
		}
		c.ID, c.VersionID, c.Position = id, versionID, i
		if err := insertColumn(ctx, tx, c); err != nil {
			return fmt.Errorf("insert column: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE datasets SET updated_at = now() WHERE id = $1`, datasetID); err != nil {
		return fmt.Errorf("touch dataset: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit complete: %w", err)
	}
	return nil
}

func (s *postgresStore) Fail(ctx context.Context, jobID, token, reason, summary string, retryable bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fail: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var versionID string
	err = tx.QueryRowContext(ctx, `
		UPDATE ingestion_jobs SET status = $3, finished_at = now(), lease_owner = NULL, lease_expires_at = NULL,
		       failure_reason = $5, error_summary = $6, retryable = $7
		WHERE id = $1 AND fencing_token = $2 AND status = $4
		RETURNING target_version_id`, jobID, token, JobFailed, JobRunning, reason, summary, retryable).Scan(&versionID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrFenced
	}
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dataset_versions SET status = $2, error_summary = $3 WHERE id = $1`,
		versionID, VersionFailed, summary); err != nil {
		return fmt.Errorf("fail version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fail: %w", err)
	}
	return nil
}
