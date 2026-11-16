package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const defaultNamespaceID = "00000000-0000-0000-0000-000000000001"

type postgresStore struct {
	db *sql.DB
}

// NewPostgresStore returns a catalog Store backed by PostgreSQL.
func NewPostgresStore(db *sql.DB) Store {
	return &postgresStore{db: db}
}

func (s *postgresStore) Register(ctx context.Context, rec RegisterRecord) (Dataset, DatasetVersion, []SchemaColumn, error) {
	ns, err := s.defaultNamespace(ctx)
	if err != nil {
		return Dataset{}, DatasetVersion{}, nil, err
	}

	datasetID, err := newUUID()
	if err != nil {
		return Dataset{}, DatasetVersion{}, nil, err
	}
	versionID, err := newUUID()
	if err != nil {
		return Dataset{}, DatasetVersion{}, nil, err
	}
	jobID, err := newUUID()
	if err != nil {
		return Dataset{}, DatasetVersion{}, nil, err
	}

	now := time.Now().UTC()
	dataset := Dataset{
		ID:            datasetID,
		NamespaceID:   ns.ID,
		NamespaceSlug: ns.Slug,
		Name:          rec.Name,
		SourcePrefix:  rec.SourcePrefix,
		Format:        rec.Format,
		Status:        DatasetActive,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	version := DatasetVersion{
		ID:               versionID,
		DatasetID:        datasetID,
		Version:          1,
		ObjectPrefix:     rec.ObjectPrefix,
		Format:           rec.Format,
		PartitionColumns: rec.PartitionColumns,
		Status:           VersionReady,
		RowCount:         rec.RowCount,
		CreatedAt:        now,
	}

	partitions := map[string]bool{}
	for _, p := range rec.PartitionColumns {
		partitions[p] = true
	}
	cols := make([]SchemaColumn, 0, len(rec.Columns))
	for i, src := range rec.Columns {
		id, err := newUUID()
		if err != nil {
			return Dataset{}, DatasetVersion{}, nil, err
		}
		cols = append(cols, SchemaColumn{
			ID:          id,
			VersionID:   versionID,
			Position:    i,
			Name:        src.Name,
			Type:        src.Type,
			Nullable:    src.Nullable,
			IsPartition: partitions[src.Name],
		})
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Dataset{}, DatasetVersion{}, nil, fmt.Errorf("begin registration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := insertDataset(ctx, tx, dataset); err != nil {
		if isUniqueViolation(err) {
			return Dataset{}, DatasetVersion{}, nil, fmt.Errorf("%w: dataset %q", ErrAlreadyExists, rec.Name)
		}
		return Dataset{}, DatasetVersion{}, nil, fmt.Errorf("insert dataset: %w", err)
	}
	if err := insertVersion(ctx, tx, version); err != nil {
		return Dataset{}, DatasetVersion{}, nil, fmt.Errorf("insert version: %w", err)
	}
	for _, c := range cols {
		if err := insertColumn(ctx, tx, c); err != nil {
			return Dataset{}, DatasetVersion{}, nil, fmt.Errorf("insert column: %w", err)
		}
	}
	if err := insertRegisterJob(ctx, tx, jobID, datasetID, versionID, rec, now); err != nil {
		return Dataset{}, DatasetVersion{}, nil, fmt.Errorf("insert job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return Dataset{}, DatasetVersion{}, nil, fmt.Errorf("commit registration: %w", err)
	}
	return dataset, version, cols, nil
}

func (s *postgresStore) List(ctx context.Context, opts ListOptions) (ListResult, error) {
	limit := opts.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}

	filter := ""
	args := []any{}
	next := 1
	if q := strings.TrimSpace(opts.Query); q != "" {
		filter = " WHERE d.name ILIKE $" + strconv.Itoa(next)
		args = append(args, "%"+q+"%")
		next++
	}

	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM datasets d`+filter, args...).Scan(&total); err != nil {
		return ListResult{}, fmt.Errorf("count datasets: %w", err)
	}

	args = append(args, limit, offset)
	query := `
		SELECT d.id, d.namespace_id, n.slug, d.name, d.source_prefix, d.format, d.status, d.retain_original, d.created_at, d.updated_at
		FROM datasets d
		JOIN namespaces n ON n.id = d.namespace_id` + filter + `
		ORDER BY d.name ASC
		LIMIT $` + strconv.Itoa(next) + ` OFFSET $` + strconv.Itoa(next+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ListResult{}, fmt.Errorf("list datasets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	datasets := []Dataset{}
	for rows.Next() {
		var d Dataset
		if err := rows.Scan(&d.ID, &d.NamespaceID, &d.NamespaceSlug, &d.Name, &d.SourcePrefix, &d.Format, &d.Status, &d.RetainOriginal, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return ListResult{}, fmt.Errorf("scan dataset: %w", err)
		}
		datasets = append(datasets, d)
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, fmt.Errorf("iterate datasets: %w", err)
	}
	return ListResult{Datasets: datasets, Total: total}, nil
}

func (s *postgresStore) Get(ctx context.Context, id string) (Dataset, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT d.id, d.namespace_id, n.slug, d.name, d.source_prefix, d.format, d.status, d.retain_original, d.created_at, d.updated_at
		FROM datasets d
		JOIN namespaces n ON n.id = d.namespace_id
		WHERE d.id = $1`, id)
	var d Dataset
	if err := row.Scan(&d.ID, &d.NamespaceID, &d.NamespaceSlug, &d.Name, &d.SourcePrefix, &d.Format, &d.Status, &d.RetainOriginal, &d.CreatedAt, &d.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Dataset{}, fmt.Errorf("%w: dataset", ErrNotFound)
		}
		return Dataset{}, fmt.Errorf("get dataset: %w", err)
	}
	return d, nil
}

func (s *postgresStore) GetLatestVersion(ctx context.Context, datasetID string) (DatasetVersion, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, dataset_id, version, object_prefix, format, partition_columns, status, error_summary, row_count, created_at
		FROM dataset_versions
		WHERE dataset_id = $1
		ORDER BY version DESC
		LIMIT 1`, datasetID)
	return scanVersion(row)
}

func (s *postgresStore) GetSchema(ctx context.Context, versionID string) ([]SchemaColumn, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, version_id, position, name, type, nullable, is_partition
		FROM schema_columns
		WHERE version_id = $1
		ORDER BY position ASC`, versionID)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols := []SchemaColumn{}
	for rows.Next() {
		var c SchemaColumn
		if err := rows.Scan(&c.ID, &c.VersionID, &c.Position, &c.Name, &c.Type, &c.Nullable, &c.IsPartition); err != nil {
			return nil, fmt.Errorf("scan schema column: %w", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema columns: %w", err)
	}
	return cols, nil
}

func (s *postgresStore) defaultNamespace(ctx context.Context) (Namespace, error) {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO namespaces (id, slug)
		VALUES ($1, $2)
		ON CONFLICT (slug) DO NOTHING`, defaultNamespaceID, NamespaceDefault); err != nil {
		return Namespace{}, fmt.Errorf("ensure namespace: %w", err)
	}
	row := s.db.QueryRowContext(ctx, `SELECT id, slug FROM namespaces WHERE slug = $1`, NamespaceDefault)
	var ns Namespace
	if err := row.Scan(&ns.ID, &ns.Slug); err != nil {
		return Namespace{}, fmt.Errorf("load namespace: %w", err)
	}
	return ns, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanVersion(row rowScanner) (DatasetVersion, error) {
	var v DatasetVersion
	var parts []byte
	var errSummary sql.NullString
	var rowCount sql.NullInt64
	if err := row.Scan(&v.ID, &v.DatasetID, &v.Version, &v.ObjectPrefix, &v.Format, &parts, &v.Status, &errSummary, &rowCount, &v.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DatasetVersion{}, fmt.Errorf("%w: version", ErrNotFound)
		}
		return DatasetVersion{}, fmt.Errorf("scan version: %w", err)
	}
	if err := json.Unmarshal(parts, &v.PartitionColumns); err != nil {
		return DatasetVersion{}, fmt.Errorf("decode partition columns: %w", err)
	}
	if errSummary.Valid {
		v.ErrorSummary = errSummary.String
	}
	if rowCount.Valid {
		n := rowCount.Int64
		v.RowCount = &n
	}
	return v, nil
}

func insertDataset(ctx context.Context, tx *sql.Tx, d Dataset) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO datasets (id, namespace_id, name, source_prefix, format, status, retain_original, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		d.ID, d.NamespaceID, d.Name, d.SourcePrefix, d.Format, d.Status, d.RetainOriginal, d.CreatedAt, d.UpdatedAt)
	return err
}

func insertVersion(ctx context.Context, tx *sql.Tx, v DatasetVersion) error {
	parts, err := json.Marshal(v.PartitionColumns)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dataset_versions (id, dataset_id, version, object_prefix, format, partition_columns, status, row_count, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		v.ID, v.DatasetID, v.Version, v.ObjectPrefix, v.Format, parts, v.Status, v.RowCount, v.CreatedAt)
	return err
}

func insertColumn(ctx context.Context, tx *sql.Tx, c SchemaColumn) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO schema_columns (id, version_id, position, name, type, nullable, is_partition)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		c.ID, c.VersionID, c.Position, c.Name, c.Type, c.Nullable, c.IsPartition)
	return err
}

func insertRegisterJob(ctx context.Context, tx *sql.Tx, jobID, datasetID, versionID string, rec RegisterRecord, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO ingestion_jobs (id, dataset_id, target_version_id, kind, status, input_ref, attempt, created_at, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $8)`,
		jobID, datasetID, versionID, KindRegister, JobSucceeded, rec.SourcePrefix, 1, now)
	return err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
