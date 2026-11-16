package catalog

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryIngestStore is an in-process IngestStore with the same state machine,
// fencing, and lease semantics as the PostgreSQL store. It backs unit tests of
// the runner and API without requiring a database.
type MemoryIngestStore struct {
	mu       sync.Mutex
	Now      func() time.Time
	datasets map[string]*Dataset // by name
	jobs     map[string]*Job
	versions map[string]*DatasetVersion // by version id
	columns  map[string][]SchemaColumn  // by version id
	leases   map[string]time.Time
	order    []string
}

// NewMemoryIngestStore returns an empty store.
func NewMemoryIngestStore() *MemoryIngestStore {
	return &MemoryIngestStore{
		Now:      func() time.Time { return time.Now().UTC() },
		datasets: map[string]*Dataset{},
		jobs:     map[string]*Job{},
		versions: map[string]*DatasetVersion{},
		columns:  map[string][]SchemaColumn{},
		leases:   map[string]time.Time{},
	}
}

// Version returns a copy of a version row by id.
func (m *MemoryIngestStore) Version(id string) (DatasetVersion, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.versions[id]
	if !ok {
		return DatasetVersion{}, false
	}
	return *v, true
}

// ReadyVersions returns the READY versions of a dataset name.
func (m *MemoryIngestStore) ReadyVersions(name string) []DatasetVersion {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []DatasetVersion
	d, ok := m.datasets[name]
	if !ok {
		return nil
	}
	for _, v := range m.versions {
		if v.DatasetID == d.ID && v.Status == VersionReady {
			out = append(out, *v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// Columns returns the stored schema of a version.
func (m *MemoryIngestStore) Columns(versionID string) []SchemaColumn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]SchemaColumn(nil), m.columns[versionID]...)
}

func (m *MemoryIngestStore) nextVersion(datasetID string) int {
	n := 0
	for _, v := range m.versions {
		if v.DatasetID == datasetID && v.Version > n {
			n = v.Version
		}
	}
	return n + 1
}

// latestReady returns the dataset's highest READY version.
func (m *MemoryIngestStore) latestReady(datasetID string) (*DatasetVersion, bool) {
	var best *DatasetVersion
	for _, v := range m.versions {
		if v.DatasetID == datasetID && v.Status == VersionReady && (best == nil || v.Version > best.Version) {
			best = v
		}
	}
	return best, best != nil
}

func (m *MemoryIngestStore) enqueue(ds *Dataset, req CreateConvert) (Job, error) {
	mode, baseID := req.Mode, ""
	if mode == "" {
		mode = ModeReplace
	}
	if mode == ModeAppend {
		base, ok := m.latestReady(ds.ID)
		if !ok {
			return Job{}, fmt.Errorf("%w: dataset %q has no READY version to append to", ErrInvalidArgument, ds.Name)
		}
		if err := CheckAppendable(ds.Name, *base); err != nil {
			return Job{}, err
		}
		baseID, req.PartitionColumns = base.ID, base.PartitionColumns
	}
	vid, err := newUUID()
	if err != nil {
		return Job{}, err
	}
	n := m.nextVersion(ds.ID)
	now := m.Now()
	m.versions[vid] = &DatasetVersion{
		ID: vid, DatasetID: ds.ID, Version: n, Format: FormatParquet, Status: VersionPending,
		ObjectPrefix:     lakeBasePrefix(ds.NamespaceSlug, ds.Name, n),
		PartitionColumns: orEmpty(req.PartitionColumns), CreatedAt: now,
	}
	j := &Job{
		ID: req.JobID, DatasetID: ds.ID, DatasetName: ds.Name, VersionID: vid, Version: n,
		Kind: KindConvert, Status: JobQueued, InputRef: req.InputRef, InputFormat: req.InputFormat,
		PartitionColumns: orEmpty(req.PartitionColumns), RetryOf: req.RetryOf,
		RetainOriginal: ds.RetainOriginal, CreatedAt: now, Mode: mode, BaseVersionID: baseID,
	}
	m.jobs[j.ID] = j
	m.order = append(m.order, j.ID)
	return *j, nil
}

func (m *MemoryIngestStore) CreateConvert(_ context.Context, req CreateConvert) (Job, Dataset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ds, ok := m.datasets[req.Name]
	if !ok && req.Mode == ModeAppend {
		return Job{}, Dataset{}, fmt.Errorf("%w: dataset %q does not exist; an append needs an existing dataset", ErrNotFound, req.Name)
	}
	if !ok {
		id, err := newUUID()
		if err != nil {
			return Job{}, Dataset{}, err
		}
		now := m.Now()
		ds = &Dataset{
			ID: id, NamespaceSlug: NamespaceDefault, Name: req.Name, Format: FormatParquet,
			SourcePrefix: fmt.Sprintf("uploads/%s/%s/", NamespaceDefault, req.Name), Status: DatasetActive,
			RetainOriginal: req.RetainOriginal, CreatedAt: now, UpdatedAt: now,
		}
		m.datasets[req.Name] = ds
	} else if ds.Status != DatasetActive {
		return Job{}, Dataset{}, fmt.Errorf("%w: dataset %q is not active", ErrInvalidArgument, req.Name)
	}
	j, err := m.enqueue(ds, req)
	return j, *ds, err
}

func (m *MemoryIngestStore) Retry(_ context.Context, sourceID, newID string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	src, ok := m.jobs[sourceID]
	if !ok {
		return Job{}, fmt.Errorf("%w: job", ErrNotFound)
	}
	if src.Status != JobFailed || !src.Retryable {
		return Job{}, fmt.Errorf("%w: only failed, retryable jobs can be retried", ErrInvalidArgument)
	}
	for _, j := range m.jobs {
		if j.RetryOf == sourceID && j.Status != JobFailed && j.Status != JobCancelled {
			return Job{}, fmt.Errorf("%w: job already retried", ErrAlreadyExists)
		}
	}
	ds := m.datasets[src.DatasetName]
	return m.enqueue(ds, CreateConvert{
		JobID: newID, Name: ds.Name, InputRef: src.InputRef, InputFormat: src.InputFormat,
		PartitionColumns: src.PartitionColumns, RetryOf: sourceID, Mode: src.Mode,
	})
}

func (m *MemoryIngestStore) GetJob(_ context.Context, id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return Job{}, fmt.Errorf("%w: job", ErrNotFound)
	}
	return *j, nil
}

func (m *MemoryIngestStore) ListJobs(_ context.Context, limit int) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for i := len(m.order) - 1; i >= 0 && (limit <= 0 || len(out) < limit); i-- {
		out = append(out, *m.jobs[m.order[i]])
	}
	return out, nil
}

func (m *MemoryIngestStore) Claim(_ context.Context, _ string, lease time.Duration) (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.Now()
	for _, id := range m.order {
		j := m.jobs[id]
		expired := j.Status == JobRunning && m.leases[id].Before(now)
		if j.Status != JobQueued && !expired {
			continue
		}
		if expired && j.Attempt >= MaxAttempts {
			j.Status, j.Retryable, j.FailureReason = JobFailed, true, "worker_interrupted"
			j.ErrorSummary = "ingestion was interrupted repeatedly; retry the job"
			m.versions[j.VersionID].Status = VersionFailed
			m.versions[j.VersionID].ErrorSummary = j.ErrorSummary
			continue
		}
		tok, err := newUUID()
		if err != nil {
			return nil, err
		}
		j.Status, j.Attempt, j.FencingToken = JobRunning, j.Attempt+1, tok
		if j.StartedAt == nil {
			t := now
			j.StartedAt = &t
		}
		m.leases[id] = now.Add(lease)
		m.versions[j.VersionID].Status = VersionBuilding
		out := *j
		return &out, nil
	}
	return nil, nil
}

func (m *MemoryIngestStore) fenced(id, token string) (*Job, error) {
	j, ok := m.jobs[id]
	if !ok || j.Status != JobRunning || j.FencingToken != token {
		return nil, ErrFenced
	}
	return j, nil
}

func (m *MemoryIngestStore) Complete(_ context.Context, id, token string, p Publish) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.fenced(id, token)
	if err != nil {
		return err
	}
	v := m.versions[j.VersionID]
	rc := p.RowCount
	rowCount := &rc
	var segments []string
	if j.Mode == ModeAppend {
		if latest, ok := m.latestReady(j.DatasetID); !ok || latest.ID != j.BaseVersionID {
			return ErrBaseChanged
		}
		base := m.versions[j.BaseVersionID]
		segments = append(append(segments, base.BaseSegments...), base.ObjectPrefix)
		if base.RowCount == nil {
			rowCount = nil
		} else {
			total := *base.RowCount + p.RowCount
			rowCount = &total
		}
	}
	now := m.Now()
	j.Status, j.FinishedAt, j.Retryable, j.FailureReason, j.ErrorSummary = JobSucceeded, &now, false, "", ""
	v.Status, v.ObjectPrefix, v.RowCount, v.PartitionColumns = VersionReady, p.ObjectPrefix, rowCount, orEmpty(p.PartitionColumns)
	v.BaseSegments = segments
	cols := make([]SchemaColumn, len(p.Columns))
	for i, c := range p.Columns {
		c.VersionID, c.Position = v.ID, i
		cols[i] = c
	}
	m.columns[v.ID] = cols
	return nil
}

func (m *MemoryIngestStore) Fail(_ context.Context, id, token, reason, summary string, retryable bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.fenced(id, token)
	if err != nil {
		return err
	}
	now := m.Now()
	j.Status, j.FinishedAt, j.FailureReason, j.ErrorSummary, j.Retryable = JobFailed, &now, reason, summary, retryable
	v := m.versions[j.VersionID]
	v.Status, v.ErrorSummary = VersionFailed, summary
	return nil
}

func (m *MemoryIngestStore) AppendBase(_ context.Context, baseVersionID string) (DatasetVersion, []SchemaColumn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.versions[baseVersionID]
	if !ok {
		return DatasetVersion{}, nil, fmt.Errorf("%w: base version", ErrNotFound)
	}
	return *v, append([]SchemaColumn(nil), m.columns[baseVersionID]...), nil
}

func (m *MemoryIngestStore) ReadySchema(_ context.Context, name string) (DatasetVersion, []SchemaColumn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ds, ok := m.datasets[name]
	if !ok || ds.Status != DatasetActive {
		return DatasetVersion{}, nil, fmt.Errorf("%w: dataset %q", ErrNotFound, name)
	}
	v, ok := m.latestReady(ds.ID)
	if !ok {
		return DatasetVersion{}, nil, fmt.Errorf("%w: dataset %q has no READY version", ErrNotFound, name)
	}
	return *v, append([]SchemaColumn(nil), m.columns[v.ID]...), nil
}
