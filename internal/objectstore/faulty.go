package objectstore

import (
	"context"
	"io"
	"sync"
)

// Faulty wraps a ReadWriter and injects failures on demand, to test how the
// services behave when object storage is unavailable or fails mid-operation.
// It is a test helper and is not used by any binary.
type Faulty struct {
	ReadWriter

	mu        sync.Mutex
	down      bool
	failPuts  int // fail every Put after this many have succeeded (when armed)
	putsOK    int
	putArmed  bool
	onFailure func() // called when an injected failure fires (e.g. cancel a context)
	Failures  int
}

// NewFaulty wraps rw.
func NewFaulty(rw ReadWriter) *Faulty { return &Faulty{ReadWriter: rw} }

// SetDown makes every operation fail with ErrUnavailable until cleared.
func (f *Faulty) SetDown(down bool) {
	f.mu.Lock()
	f.down = down
	f.mu.Unlock()
}

// FailPutsAfter lets n more Puts succeed, then fails every later Put with
// ErrUnavailable. onFailure, if non-nil, runs each time a Put is failed.
func (f *Faulty) FailPutsAfter(n int, onFailure func()) {
	f.mu.Lock()
	f.failPuts, f.putsOK, f.putArmed, f.onFailure = n, 0, true, onFailure
	f.mu.Unlock()
}

// Heal removes every injected fault.
func (f *Faulty) Heal() {
	f.mu.Lock()
	f.down, f.putArmed, f.onFailure = false, false, nil
	f.mu.Unlock()
}

func (f *Faulty) check() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		f.Failures++
		return ErrUnavailable
	}
	return nil
}

func (f *Faulty) List(ctx context.Context, o ListOptions) (ListResult, error) {
	if err := f.check(); err != nil {
		return ListResult{}, err
	}
	return f.ReadWriter.List(ctx, o)
}

func (f *Faulty) Range(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	if err := f.check(); err != nil {
		return nil, err
	}
	return f.ReadWriter.Range(ctx, key, off, n)
}

func (f *Faulty) Get(ctx context.Context, key string, max int64) ([]byte, error) {
	if err := f.check(); err != nil {
		return nil, err
	}
	return f.ReadWriter.Get(ctx, key, max)
}

func (f *Faulty) Delete(ctx context.Context, key string) error {
	if err := f.check(); err != nil {
		return err
	}
	return f.ReadWriter.Delete(ctx, key)
}

func (f *Faulty) Put(ctx context.Context, key string, data []byte) error {
	if err := f.check(); err != nil {
		return err
	}
	f.mu.Lock()
	if f.putArmed {
		if f.putsOK >= f.failPuts {
			f.Failures++
			cb := f.onFailure
			f.mu.Unlock()
			if cb != nil {
				cb()
			}
			return ErrUnavailable
		}
		f.putsOK++
	}
	f.mu.Unlock()
	return f.ReadWriter.Put(ctx, key, data)
}
