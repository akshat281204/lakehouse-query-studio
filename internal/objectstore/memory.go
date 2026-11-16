package objectstore

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is an in-process ReadWriter for tests. It is not used by any binary.
type Memory struct {
	mu   sync.Mutex
	data map[string][]byte
	mod  map[string]time.Time
	// Gets counts whole-object reads, so tests can prove nothing was fetched.
	Gets int
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{data: map[string][]byte{}, mod: map[string]time.Time{}} }

var _ ReadWriter = (*Memory)(nil)

// Bucket implements Store.
func (m *Memory) Bucket() string { return "memory" }

// Keys returns all keys under prefix, sorted.
func (m *Memory) Keys(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// List implements Store (single page).
func (m *Memory) List(_ context.Context, opts ListOptions) (ListResult, error) {
	var res ListResult
	for _, k := range m.Keys(opts.Prefix) {
		m.mu.Lock()
		res.Objects = append(res.Objects, Object{Key: k, Size: int64(len(m.data[k])), LastModified: m.mod[k]})
		m.mu.Unlock()
	}
	return res, nil
}

// Range implements Store.
func (m *Memory) Range(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	if offset > int64(len(b)) {
		offset = int64(len(b))
	}
	end := offset + length
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	return io.NopCloser(bytes.NewReader(b[offset:end])), nil
}

// Put implements ReadWriter.
func (m *Memory) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), data...)
	m.mod[key] = time.Now()
	return nil
}

// Get implements ReadWriter.
func (m *Memory) Get(_ context.Context, key string, maxBytes int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Gets++
	b, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	if int64(len(b)) > maxBytes {
		return nil, ErrTooLarge
	}
	return append([]byte(nil), b...), nil
}

// Delete implements ReadWriter.
func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	delete(m.mod, key)
	return nil
}

// SetModTime overrides an object's modification time (tests of age-based
// sweeps). A zero time simulates an unknown age.
func (m *Memory) SetModTime(key string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mod[key] = t
}
