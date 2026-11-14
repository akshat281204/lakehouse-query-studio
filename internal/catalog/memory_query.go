package catalog

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryQueryStore is an in-process QueryStore with the same state machine,
// leases and fencing as the PostgreSQL store, for unit tests of the runner and
// API.
type MemoryQueryStore struct {
	mu        sync.Mutex
	Now       func() time.Time
	relations map[string]Relation // by lower-case `<dataset>` or `<dataset>_v<n>`
	byVersion map[string]Relation
	jobs      map[string]*QueryJob
	leases    map[string]time.Time
	order     []string
	// IngestQueued lets tests simulate queued ingestion jobs that share the
	// combined queue-depth limit.
	IngestQueued int
}

// NewMemoryQueryStore returns an empty store.
func NewMemoryQueryStore() *MemoryQueryStore {
	return &MemoryQueryStore{
		Now:       func() time.Time { return time.Now().UTC() },
		relations: map[string]Relation{},
		byVersion: map[string]Relation{},
		jobs:      map[string]*QueryJob{},
		leases:    map[string]time.Time{},
	}
}

// AddRelation registers a READY dataset version. The highest version of a
// dataset is also reachable by its bare name.
func (m *MemoryQueryStore) AddRelation(r Relation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.VersionID == "" {
		r.VersionID, _ = newUUID()
	}
	m.byVersion[r.VersionID] = r
	m.relations[fmt.Sprintf("%s_v%d", r.DatasetName, r.Version)] = r
	if cur, ok := m.relations[r.DatasetName]; !ok || cur.Version < r.Version {
		m.relations[r.DatasetName] = r
	}
}

func (m *MemoryQueryStore) ResolveRelation(_ context.Context, name string) (Relation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.relations[strings.ToLower(name)]; ok {
		return r, nil
	}
	return Relation{}, fmt.Errorf("%w: relation", ErrNotFound)
}

func (m *MemoryQueryStore) RelationByVersionID(_ context.Context, id string) (Relation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.byVersion[id]; ok {
		return r, nil
	}
	return Relation{}, fmt.Errorf("%w: relation", ErrNotFound)
}

func cloneQuery(q *QueryJob) QueryJob {
	c := *q
	c.Bindings = map[string]string{}
	for k, v := range q.Bindings {
		c.Bindings[k] = v
	}
	c.DatasetVersionIDs = append([]string{}, q.DatasetVersionIDs...)
	return c
}

func (m *MemoryQueryStore) Submit(_ context.Context, req SubmitQuery) (QueryJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validUUID(req.ID) {
		return QueryJob{}, fmt.Errorf("%w: query id", ErrInvalidArgument)
	}
	if req.MaxQueued > 0 {
		n := m.IngestQueued
		for _, j := range m.jobs {
			if j.Status == QueryQueued {
				n++
			}
		}
		if n >= req.MaxQueued {
			return QueryJob{}, ErrQueueFull
		}
	}
	seen := map[string]bool{}
	ids := []string{}
	keys := make([]string, 0, len(req.Bindings))
	for k := range req.Bindings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if id := req.Bindings[k]; !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	q := &QueryJob{ID: req.ID, Statement: req.Statement, StatementHash: req.StatementHash,
		Bindings: req.Bindings, DatasetVersionIDs: ids, Status: QueryQueued, CreatedAt: m.Now()}
	m.jobs[q.ID] = q
	m.order = append(m.order, q.ID)
	return cloneQuery(q), nil
}

func (m *MemoryQueryStore) GetQuery(_ context.Context, id string) (QueryJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.jobs[id]
	if !ok {
		return QueryJob{}, fmt.Errorf("%w: query", ErrNotFound)
	}
	return cloneQuery(q), nil
}

func (m *MemoryQueryStore) ListQueries(_ context.Context, offset, limit int) ([]QueryJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []QueryJob{}
	for i := len(m.order) - 1 - offset; i >= 0 && len(out) < limit; i-- {
		out = append(out, cloneQuery(m.jobs[m.order[i]]))
	}
	return out, nil
}

func (m *MemoryQueryStore) ClaimQuery(_ context.Context, owner string, lease time.Duration) (*QueryJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.order {
		q := m.jobs[id]
		if q.Status != QueryQueued {
			continue
		}
		tok, err := newUUID()
		if err != nil {
			return nil, err
		}
		now := m.Now()
		q.Status, q.FencingToken = QueryRunning, tok
		q.Attempt++
		m.leases[id] = now.Add(lease)
		if q.StartedAt == nil {
			q.StartedAt = &now
		}
		c := cloneQuery(q)
		return &c, nil
	}
	return nil, nil
}

func (m *MemoryQueryStore) RecoverExpired(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	now := m.Now()
	for id, q := range m.jobs {
		if q.Status != QueryRunning || !m.leases[id].Before(now) {
			continue
		}
		delete(m.leases, id)
		q.FencingToken = ""
		if q.Attempt >= MaxQueryAttempts {
			q.Status, q.FailureReason = QueryFailed, ReasonWorkerInterrupted
			q.ErrorSummary = "the query worker was interrupted repeatedly"
			q.FinishedAt = &now
		} else {
			q.Status = QueryQueued
		}
		n++
	}
	return n, nil
}

func (m *MemoryQueryStore) held(id, token string) (*QueryJob, bool) {
	q, ok := m.jobs[id]
	if !ok || q.Status != QueryRunning || q.FencingToken == "" || q.FencingToken != token {
		return nil, false
	}
	return q, true
}

func (m *MemoryQueryStore) Heartbeat(_ context.Context, id, token string, lease time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.held(id, token)
	if !ok {
		return false, ErrFenced
	}
	m.leases[id] = m.Now().Add(lease)
	return q.CancelRequested, nil
}

func (m *MemoryQueryStore) Succeed(_ context.Context, id, token string, r QueryResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.held(id, token)
	if !ok {
		return ErrFenced
	}
	now := m.Now()
	rows, rb, bs, d := r.RowsReturned, r.ResultBytes, r.BytesStaged, r.Duration.Milliseconds()
	q.Status, q.ResultRef, q.RowsReturned, q.Truncated = QuerySucceeded, r.ResultRef, &rows, r.Truncated
	q.ResultBytes, q.BytesStaged, q.PlanSummary, q.DurationMs, q.FinishedAt = &rb, &bs, r.PlanSummary, &d, &now
	delete(m.leases, id)
	return nil
}

func (m *MemoryQueryStore) Finish(_ context.Context, id, token, status, reason, summary string, dur time.Duration) error {
	switch status {
	case QueryFailed, QueryCancelled, QueryTimedOut:
	default:
		return fmt.Errorf("%w: terminal status", ErrInvalidArgument)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.held(id, token)
	if !ok {
		return ErrFenced
	}
	now := m.Now()
	d := dur.Milliseconds()
	q.Status, q.FailureReason, q.ErrorSummary, q.DurationMs, q.FinishedAt = status, reason, summary, &d, &now
	delete(m.leases, id)
	return nil
}

func (m *MemoryQueryStore) RequestCancel(_ context.Context, id string) (QueryJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.jobs[id]
	if !ok {
		return QueryJob{}, fmt.Errorf("%w: query", ErrNotFound)
	}
	switch q.Status {
	case QueryQueued:
		now := m.Now()
		q.Status, q.CancelRequested, q.FinishedAt = QueryCancelled, true, &now
	case QueryRunning:
		q.CancelRequested = true
	default:
		return QueryJob{}, fmt.Errorf("%w: query is already %s", ErrInvalidState, strings.ToLower(q.Status))
	}
	return cloneQuery(q), nil
}

func (m *MemoryQueryStore) Stats(_ context.Context) (QueueStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := QueueStats{IngestQueued: m.IngestQueued}
	for _, j := range m.jobs {
		switch j.Status {
		case QueryQueued:
			st.QueryQueued++
		case QueryRunning:
			st.QueryRunning++
		}
	}
	return st, nil
}

func (m *MemoryQueryStore) ExpireResults(_ context.Context, cutoff time.Time) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var refs []string
	for _, q := range m.jobs {
		if q.Status == QuerySucceeded && q.ResultRef != "" && q.FinishedAt != nil && q.FinishedAt.Before(cutoff) {
			refs = append(refs, q.ResultRef)
			q.ResultRef, q.ResultBytes = "", nil
		}
	}
	return refs, nil
}

func (m *MemoryQueryStore) PurgeHistory(_ context.Context, cutoff time.Time) (int, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var refs []string
	n := 0
	var keep []string
	for _, id := range m.order {
		q := m.jobs[id]
		if IsTerminalQuery(q.Status) && q.FinishedAt != nil && q.FinishedAt.Before(cutoff) {
			if q.ResultRef != "" {
				refs = append(refs, q.ResultRef)
			}
			delete(m.jobs, id)
			n++
			continue
		}
		keep = append(keep, id)
	}
	m.order = keep
	return n, refs, nil
}

// ExpireLease forces the lease of a RUNNING query into the past (tests).
func (m *MemoryQueryStore) ExpireLease(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leases[id] = m.Now().Add(-time.Second)
}

var _ QueryStore = (*MemoryQueryStore)(nil)
var _ QueryStore = (*postgresQueryStore)(nil)
