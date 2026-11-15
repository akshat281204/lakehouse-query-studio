package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// Limits on saved queries. The count cap keeps an unauthenticated local tool
// from growing a table without bound.
const (
	MaxSavedQueries      = 500
	MaxSavedNameChars    = 120
	MaxSavedDescription  = 500
	maxSavedListPageSize = 200
)

// SavedQuery is a named, reusable statement.
type SavedQuery struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Statement   string    `json:"statement"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// SavedQueryInput is the writable part of a saved query.
type SavedQueryInput struct {
	Name        string
	Description string
	Statement   string
}

// SavedQueryStore persists saved queries. Names are unique ignoring case.
type SavedQueryStore interface {
	// Create adds a saved query. ErrAlreadyExists when the name is taken;
	// ErrInvalidArgument when the limit is reached.
	Create(ctx context.Context, in SavedQueryInput) (SavedQuery, error)
	// Get returns one saved query by id (ErrNotFound if absent).
	Get(ctx context.Context, id string) (SavedQuery, error)
	// List returns saved queries most recently updated first.
	List(ctx context.Context, offset, limit int) ([]SavedQuery, int, error)
	// Update replaces name, description and statement (ErrNotFound,
	// ErrAlreadyExists for a name taken by another saved query).
	Update(ctx context.Context, id string, in SavedQueryInput) (SavedQuery, error)
	// Delete removes a saved query (ErrNotFound if absent).
	Delete(ctx context.Context, id string) error
}

// NormalizeSavedQuery trims and validates the writable fields. The statement
// itself is validated by the SQL validator in the API layer.
func NormalizeSavedQuery(in SavedQueryInput) (SavedQueryInput, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.Description = strings.TrimSpace(in.Description)
	switch {
	case in.Name == "":
		return in, fmt.Errorf("%w: name is required", ErrInvalidArgument)
	case utf8.RuneCountInString(in.Name) > MaxSavedNameChars:
		return in, fmt.Errorf("%w: name is longer than %d characters", ErrInvalidArgument, MaxSavedNameChars)
	case utf8.RuneCountInString(in.Description) > MaxSavedDescription:
		return in, fmt.Errorf("%w: description is longer than %d characters", ErrInvalidArgument, MaxSavedDescription)
	case strings.TrimSpace(in.Statement) == "":
		return in, fmt.Errorf("%w: statement is required", ErrInvalidArgument)
	}
	for _, r := range in.Name {
		if unicode.IsControl(r) {
			return in, fmt.Errorf("%w: name contains a control character", ErrInvalidArgument)
		}
	}
	return in, nil
}

func clampPage(offset, limit int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > maxSavedListPageSize {
		limit = 50
	}
	return offset, limit
}

// ---- memory ----------------------------------------------------------------

// MemorySavedQueryStore is an in-process SavedQueryStore for tests.
type MemorySavedQueryStore struct {
	mu   sync.Mutex
	Now  func() time.Time
	byID map[string]SavedQuery
}

// NewMemorySavedQueryStore returns an empty store.
func NewMemorySavedQueryStore() *MemorySavedQueryStore {
	return &MemorySavedQueryStore{Now: func() time.Time { return time.Now().UTC() }, byID: map[string]SavedQuery{}}
}

func (m *MemorySavedQueryStore) nameTaken(name, exceptID string) bool {
	for id, q := range m.byID {
		if id != exceptID && strings.EqualFold(q.Name, name) {
			return true
		}
	}
	return false
}

func (m *MemorySavedQueryStore) Create(_ context.Context, in SavedQueryInput) (SavedQuery, error) {
	in, err := NormalizeSavedQuery(in)
	if err != nil {
		return SavedQuery{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.byID) >= MaxSavedQueries {
		return SavedQuery{}, fmt.Errorf("%w: at most %d saved queries are allowed", ErrInvalidArgument, MaxSavedQueries)
	}
	if m.nameTaken(in.Name, "") {
		return SavedQuery{}, fmt.Errorf("%w: a saved query with this name exists", ErrAlreadyExists)
	}
	id, err := newUUID()
	if err != nil {
		return SavedQuery{}, err
	}
	now := m.Now()
	q := SavedQuery{ID: id, Name: in.Name, Description: in.Description, Statement: in.Statement, CreatedAt: now, UpdatedAt: now}
	m.byID[id] = q
	return q, nil
}

func (m *MemorySavedQueryStore) Get(_ context.Context, id string) (SavedQuery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.byID[id]
	if !ok {
		return SavedQuery{}, fmt.Errorf("%w: saved query", ErrNotFound)
	}
	return q, nil
}

func (m *MemorySavedQueryStore) List(_ context.Context, offset, limit int) ([]SavedQuery, int, error) {
	offset, limit = clampPage(offset, limit)
	m.mu.Lock()
	defer m.mu.Unlock()
	all := make([]SavedQuery, 0, len(m.byID))
	for _, q := range m.byID {
		all = append(all, q)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].UpdatedAt.Equal(all[j].UpdatedAt) {
			return all[i].UpdatedAt.After(all[j].UpdatedAt)
		}
		return all[i].Name < all[j].Name
	})
	total := len(all)
	if offset >= total {
		return []SavedQuery{}, total, nil
	}
	end := min(offset+limit, total)
	return all[offset:end], total, nil
}

func (m *MemorySavedQueryStore) Update(_ context.Context, id string, in SavedQueryInput) (SavedQuery, error) {
	in, err := NormalizeSavedQuery(in)
	if err != nil {
		return SavedQuery{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.byID[id]
	if !ok {
		return SavedQuery{}, fmt.Errorf("%w: saved query", ErrNotFound)
	}
	if m.nameTaken(in.Name, id) {
		return SavedQuery{}, fmt.Errorf("%w: a saved query with this name exists", ErrAlreadyExists)
	}
	q.Name, q.Description, q.Statement, q.UpdatedAt = in.Name, in.Description, in.Statement, m.Now()
	m.byID[id] = q
	return q, nil
}

func (m *MemorySavedQueryStore) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byID[id]; !ok {
		return fmt.Errorf("%w: saved query", ErrNotFound)
	}
	delete(m.byID, id)
	return nil
}

// ---- postgres --------------------------------------------------------------

type postgresSavedQueryStore struct{ db *sql.DB }

// NewPostgresSavedQueryStore returns a SavedQueryStore backed by PostgreSQL.
func NewPostgresSavedQueryStore(db *sql.DB) SavedQueryStore { return &postgresSavedQueryStore{db: db} }

const savedColumns = `id, name, description, statement, created_at, updated_at`

func scanSaved(row rowScanner) (SavedQuery, error) {
	var q SavedQuery
	if err := row.Scan(&q.ID, &q.Name, &q.Description, &q.Statement, &q.CreatedAt, &q.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SavedQuery{}, fmt.Errorf("%w: saved query", ErrNotFound)
		}
		return SavedQuery{}, fmt.Errorf("scan saved query: %w", err)
	}
	return q, nil
}

func (s *postgresSavedQueryStore) Create(ctx context.Context, in SavedQueryInput) (SavedQuery, error) {
	in, err := NormalizeSavedQuery(in)
	if err != nil {
		return SavedQuery{}, err
	}
	id, err := newUUID()
	if err != nil {
		return SavedQuery{}, err
	}
	// Creates are serialised by a transaction-scoped advisory lock: under
	// READ COMMITTED two concurrent inserts can both see "one slot left" and
	// both succeed, so the count check alone cannot hold the cap.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SavedQuery{}, fmt.Errorf("begin create saved query: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(727276)`); err != nil {
		return SavedQuery{}, fmt.Errorf("lock saved queries: %w", err)
	}
	row := tx.QueryRowContext(ctx, `
		INSERT INTO saved_queries (id, name, description, statement)
		SELECT $1, $2, $3, $4 WHERE (SELECT count(*) FROM saved_queries) < $5
		RETURNING `+savedColumns, id, in.Name, in.Description, in.Statement, MaxSavedQueries)
	q, err := scanSaved(row)
	switch {
	case errors.Is(err, ErrNotFound):
		return SavedQuery{}, fmt.Errorf("%w: at most %d saved queries are allowed", ErrInvalidArgument, MaxSavedQueries)
	case isUniqueViolation(err):
		return SavedQuery{}, fmt.Errorf("%w: a saved query with this name exists", ErrAlreadyExists)
	case err != nil:
		return SavedQuery{}, err
	}
	if err := tx.Commit(); err != nil {
		return SavedQuery{}, fmt.Errorf("commit saved query: %w", err)
	}
	return q, nil
}

func (s *postgresSavedQueryStore) Get(ctx context.Context, id string) (SavedQuery, error) {
	if !validUUID(id) {
		return SavedQuery{}, fmt.Errorf("%w: saved query", ErrNotFound)
	}
	return scanSaved(s.db.QueryRowContext(ctx, `SELECT `+savedColumns+` FROM saved_queries WHERE id = $1`, id))
}

func (s *postgresSavedQueryStore) List(ctx context.Context, offset, limit int) ([]SavedQuery, int, error) {
	offset, limit = clampPage(offset, limit)
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM saved_queries`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count saved queries: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+savedColumns+` FROM saved_queries ORDER BY updated_at DESC, name LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list saved queries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []SavedQuery{}
	for rows.Next() {
		q, err := scanSaved(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, q)
	}
	return out, total, rows.Err()
}

func (s *postgresSavedQueryStore) Update(ctx context.Context, id string, in SavedQueryInput) (SavedQuery, error) {
	in, err := NormalizeSavedQuery(in)
	if err != nil {
		return SavedQuery{}, err
	}
	if !validUUID(id) {
		return SavedQuery{}, fmt.Errorf("%w: saved query", ErrNotFound)
	}
	q, err := scanSaved(s.db.QueryRowContext(ctx, `
		UPDATE saved_queries SET name = $2, description = $3, statement = $4, updated_at = now()
		WHERE id = $1 RETURNING `+savedColumns, id, in.Name, in.Description, in.Statement))
	if isUniqueViolation(err) {
		return SavedQuery{}, fmt.Errorf("%w: a saved query with this name exists", ErrAlreadyExists)
	}
	return q, err
}

func (s *postgresSavedQueryStore) Delete(ctx context.Context, id string) error {
	if !validUUID(id) {
		return fmt.Errorf("%w: saved query", ErrNotFound)
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM saved_queries WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete saved query: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: saved query", ErrNotFound)
	}
	return nil
}
