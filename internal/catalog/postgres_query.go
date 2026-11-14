package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type postgresQueryStore struct{ db *sql.DB }

// NewPostgresQueryStore returns a QueryStore backed by PostgreSQL.
func NewPostgresQueryStore(db *sql.DB) QueryStore { return &postgresQueryStore{db: db} }

var versionedRelation = regexp.MustCompile(`^([a-z0-9](?:[a-z0-9-]*[a-z0-9])?)_v([0-9]{1,9})$`)

// ParseRelation splits `<dataset>` or `<dataset>_v<n>`. Dataset names never
// contain '_', so the suffix is unambiguous. version is 0 for "latest".
func ParseRelation(name string) (dataset string, version int, ok bool) {
	name = strings.ToLower(name)
	if m := versionedRelation.FindStringSubmatch(name); m != nil {
		n, err := strconv.Atoi(m[2])
		if err != nil || n < 1 {
			return "", 0, false
		}
		return m[1], n, true
	}
	if _, err := ValidateDatasetName(name); err != nil {
		return "", 0, false
	}
	return name, 0, true
}

func hiveFor(prefix string, parts []string) bool {
	return len(parts) > 0 && !strings.HasPrefix(prefix, "lake/")
}

func scanRelation(row rowScanner) (Relation, error) {
	var r Relation
	var parts []byte
	if err := row.Scan(&r.DatasetName, &r.Version, &r.VersionID, &r.ObjectPrefix, &r.Format, &parts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Relation{}, fmt.Errorf("%w: relation", ErrNotFound)
		}
		return Relation{}, fmt.Errorf("scan relation: %w", err)
	}
	if err := json.Unmarshal(parts, &r.PartitionColumns); err != nil {
		return Relation{}, fmt.Errorf("decode partition columns: %w", err)
	}
	r.HivePartitioned = hiveFor(r.ObjectPrefix, r.PartitionColumns)
	return r, nil
}

const relationSelect = `
	SELECT d.name, v.version, v.id, v.object_prefix, v.format, v.partition_columns
	FROM dataset_versions v JOIN datasets d ON d.id = v.dataset_id`

func (s *postgresQueryStore) ResolveRelation(ctx context.Context, name string) (Relation, error) {
	ds, ver, ok := ParseRelation(name)
	if !ok {
		return Relation{}, fmt.Errorf("%w: relation", ErrNotFound)
	}
	if ver > 0 {
		return scanRelation(s.db.QueryRowContext(ctx, relationSelect+`
			WHERE d.name = $1 AND d.status = 'ACTIVE' AND v.status = 'READY' AND v.version = $2`, ds, ver))
	}
	return scanRelation(s.db.QueryRowContext(ctx, relationSelect+`
		WHERE d.name = $1 AND d.status = 'ACTIVE' AND v.status = 'READY'
		ORDER BY v.version DESC LIMIT 1`, ds))
}

func (s *postgresQueryStore) RelationByVersionID(ctx context.Context, versionID string) (Relation, error) {
	if !validUUID(versionID) {
		return Relation{}, fmt.Errorf("%w: relation", ErrNotFound)
	}
	return scanRelation(s.db.QueryRowContext(ctx, relationSelect+`
		WHERE v.id = $1 AND d.status = 'ACTIVE' AND v.status = 'READY'`, versionID))
}

const queryCols = `id, statement, statement_hash, bindings, dataset_version_ids, status,
	COALESCE(failure_reason, ''), COALESCE(error_summary, ''), rows_returned, truncated,
	COALESCE(result_ref, ''), result_bytes, bytes_staged, COALESCE(plan_summary, ''), duration_ms,
	attempt, COALESCE(fencing_token::text, ''), cancel_requested, created_at, started_at, finished_at`

func scanQuery(row rowScanner) (QueryJob, error) {
	var q QueryJob
	var bindings []byte
	var ids []byte
	var rows, rbytes, staged, dur sql.NullInt64
	var started, finished sql.NullTime
	if err := row.Scan(&q.ID, &q.Statement, &q.StatementHash, &bindings, &ids, &q.Status,
		&q.FailureReason, &q.ErrorSummary, &rows, &q.Truncated, &q.ResultRef, &rbytes, &staged,
		&q.PlanSummary, &dur, &q.Attempt, &q.FencingToken, &q.CancelRequested, &q.CreatedAt, &started, &finished); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return QueryJob{}, fmt.Errorf("%w: query", ErrNotFound)
		}
		return QueryJob{}, fmt.Errorf("scan query: %w", err)
	}
	if err := json.Unmarshal(bindings, &q.Bindings); err != nil {
		return QueryJob{}, fmt.Errorf("decode bindings: %w", err)
	}
	if err := json.Unmarshal(ids, &q.DatasetVersionIDs); err != nil {
		return QueryJob{}, fmt.Errorf("decode dataset version ids: %w", err)
	}
	if q.DatasetVersionIDs == nil {
		q.DatasetVersionIDs = []string{}
	}
	if rows.Valid {
		q.RowsReturned = &rows.Int64
	}
	if rbytes.Valid {
		q.ResultBytes = &rbytes.Int64
	}
	if staged.Valid {
		q.BytesStaged = &staged.Int64
	}
	if dur.Valid {
		q.DurationMs = &dur.Int64
	}
	if started.Valid {
		t := started.Time
		q.StartedAt = &t
	}
	if finished.Valid {
		t := finished.Time
		q.FinishedAt = &t
	}
	return q, nil
}

func (s *postgresQueryStore) Submit(ctx context.Context, req SubmitQuery) (QueryJob, error) {
	if !validUUID(req.ID) {
		return QueryJob{}, fmt.Errorf("%w: query id", ErrInvalidArgument)
	}
	ids := make([]string, 0, len(req.Bindings))
	seen := map[string]bool{}
	for _, id := range req.Bindings {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	bindings, err := json.Marshal(req.Bindings)
	if err != nil {
		return QueryJob{}, err
	}
	idsJSON, err := json.Marshal(ids)
	if err != nil {
		return QueryJob{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return QueryJob{}, fmt.Errorf("begin submit: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if req.MaxQueued > 0 {
		// Serialize depth checks so concurrent submits cannot overshoot.
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(727275)`); err != nil {
			return QueryJob{}, fmt.Errorf("lock queue: %w", err)
		}
		var n int
		if err := tx.QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM query_jobs WHERE status = 'QUEUED')
			     + (SELECT count(*) FROM ingestion_jobs WHERE status = 'QUEUED')`).Scan(&n); err != nil {
			return QueryJob{}, fmt.Errorf("count queue: %w", err)
		}
		if n >= req.MaxQueued {
			return QueryJob{}, ErrQueueFull
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO query_jobs (id, statement, statement_hash, bindings, dataset_version_ids, status)
		VALUES ($1, $2, $3, $4, $5, 'QUEUED')`,
		req.ID, req.Statement, req.StatementHash, bindings, idsJSON); err != nil {
		return QueryJob{}, fmt.Errorf("insert query: %w", err)
	}
	q, err := scanQuery(tx.QueryRowContext(ctx, `SELECT `+queryCols+` FROM query_jobs WHERE id = $1`, req.ID))
	if err != nil {
		return QueryJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return QueryJob{}, fmt.Errorf("commit submit: %w", err)
	}
	return q, nil
}

func (s *postgresQueryStore) GetQuery(ctx context.Context, id string) (QueryJob, error) {
	if !validUUID(id) {
		return QueryJob{}, fmt.Errorf("%w: query", ErrNotFound)
	}
	return scanQuery(s.db.QueryRowContext(ctx, `SELECT `+queryCols+` FROM query_jobs WHERE id = $1`, id))
}

func (s *postgresQueryStore) ListQueries(ctx context.Context, offset, limit int) ([]QueryJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+queryCols+` FROM query_jobs ORDER BY created_at DESC, id LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list queries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []QueryJob{}
	for rows.Next() {
		q, err := scanQuery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *postgresQueryStore) ClaimQuery(ctx context.Context, owner string, lease time.Duration) (*QueryJob, error) {
	token, err := newUUID()
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, `
		UPDATE query_jobs SET status = 'RUNNING', attempt = attempt + 1, fencing_token = $1, lease_owner = $2,
		       lease_expires_at = now() + make_interval(secs => $3), started_at = COALESCE(started_at, now())
		WHERE id = (SELECT id FROM query_jobs WHERE status = 'QUEUED' ORDER BY created_at
		            LIMIT 1 FOR UPDATE SKIP LOCKED)
		RETURNING `+queryCols, token, owner, lease.Seconds())
	q, err := scanQuery(row)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &q, nil
}

func (s *postgresQueryStore) RecoverExpired(ctx context.Context) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin recover: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Give up on statements that keep killing their worker.
	failed, err := tx.ExecContext(ctx, `
		UPDATE query_jobs SET status = 'FAILED', failure_reason = $1,
		       error_summary = 'the query worker was interrupted repeatedly', finished_at = now(),
		       lease_owner = NULL, lease_expires_at = NULL, fencing_token = NULL
		WHERE status = 'RUNNING' AND lease_expires_at < now() AND attempt >= $2`, ReasonWorkerInterrupted, MaxQueryAttempts)
	if err != nil {
		return 0, fmt.Errorf("fail exhausted queries: %w", err)
	}
	requeued, err := tx.ExecContext(ctx, `
		UPDATE query_jobs SET status = 'QUEUED', lease_owner = NULL, lease_expires_at = NULL, fencing_token = NULL
		WHERE status = 'RUNNING' AND lease_expires_at < now() AND attempt < $1`, MaxQueryAttempts)
	if err != nil {
		return 0, fmt.Errorf("requeue expired queries: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit recover: %w", err)
	}
	a, _ := failed.RowsAffected()
	b, _ := requeued.RowsAffected()
	return int(a + b), nil
}

func (s *postgresQueryStore) Heartbeat(ctx context.Context, id, token string, lease time.Duration) (bool, error) {
	if !validUUID(id) || !validUUID(token) {
		return false, ErrFenced
	}
	var cancel bool
	err := s.db.QueryRowContext(ctx, `
		UPDATE query_jobs SET lease_expires_at = now() + make_interval(secs => $3)
		WHERE id = $1 AND fencing_token = $2 AND status = 'RUNNING'
		RETURNING cancel_requested`, id, token, lease.Seconds()).Scan(&cancel)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrFenced
	}
	if err != nil {
		return false, fmt.Errorf("heartbeat: %w", err)
	}
	return cancel, nil
}

func (s *postgresQueryStore) Succeed(ctx context.Context, id, token string, r QueryResult) error {
	if !validUUID(id) || !validUUID(token) {
		return ErrFenced
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE query_jobs SET status = 'SUCCEEDED', result_ref = $3, rows_returned = $4, truncated = $5,
		       result_bytes = $6, bytes_staged = $7, plan_summary = $8, duration_ms = $9, finished_at = now(),
		       lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1 AND fencing_token = $2 AND status = 'RUNNING'`,
		id, token, r.ResultRef, r.RowsReturned, r.Truncated, r.ResultBytes, r.BytesStaged, r.PlanSummary, r.Duration.Milliseconds())
	if err != nil {
		return fmt.Errorf("succeed query: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrFenced
	}
	return nil
}

func (s *postgresQueryStore) Finish(ctx context.Context, id, token, status, reason, summary string, d time.Duration) error {
	switch status {
	case QueryFailed, QueryCancelled, QueryTimedOut:
	default:
		return fmt.Errorf("%w: terminal status", ErrInvalidArgument)
	}
	if !validUUID(id) || !validUUID(token) {
		return ErrFenced
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE query_jobs SET status = $3, failure_reason = NULLIF($4, ''), error_summary = NULLIF($5, ''),
		       duration_ms = $6, finished_at = now(), lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1 AND fencing_token = $2 AND status = 'RUNNING'`,
		id, token, status, reason, summary, d.Milliseconds())
	if err != nil {
		return fmt.Errorf("finish query: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrFenced
	}
	return nil
}

func (s *postgresQueryStore) RequestCancel(ctx context.Context, id string) (QueryJob, error) {
	if !validUUID(id) {
		return QueryJob{}, fmt.Errorf("%w: query", ErrNotFound)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return QueryJob{}, fmt.Errorf("begin cancel: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM query_jobs WHERE id = $1 FOR UPDATE`, id).Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return QueryJob{}, fmt.Errorf("%w: query", ErrNotFound)
		}
		return QueryJob{}, fmt.Errorf("lock query: %w", err)
	}
	switch status {
	case QueryQueued:
		_, err = tx.ExecContext(ctx, `UPDATE query_jobs SET status = 'CANCELLED', finished_at = now(), cancel_requested = true WHERE id = $1`, id)
	case QueryRunning:
		_, err = tx.ExecContext(ctx, `UPDATE query_jobs SET cancel_requested = true WHERE id = $1`, id)
	default:
		return QueryJob{}, fmt.Errorf("%w: query is already %s", ErrInvalidState, strings.ToLower(status))
	}
	if err != nil {
		return QueryJob{}, fmt.Errorf("cancel query: %w", err)
	}
	q, err := scanQuery(tx.QueryRowContext(ctx, `SELECT `+queryCols+` FROM query_jobs WHERE id = $1`, id))
	if err != nil {
		return QueryJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return QueryJob{}, fmt.Errorf("commit cancel: %w", err)
	}
	return q, nil
}
