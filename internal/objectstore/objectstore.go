// Package objectstore defines the object-store boundary used to list and read
// Parquet data during registration and preview. The concrete MinIO/S3 adapter
// holds server-side credentials and never exposes them to callers.
package objectstore

import (
	"context"
	"errors"
	"io"
	"time"
)

// Sentinel errors mapped by the API to stable HTTP error codes. Object-store
// access failures are server-side and surface as unavailable (503), never as a
// caller authorization error.
var (
	ErrNotFound     = errors.New("object store: not found")
	ErrUnavailable  = errors.New("object store: unavailable")
	ErrAccessDenied = errors.New("object store: access denied")
)

// Object is a single object returned by a listing.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ListOptions controls a bounded prefix listing.
type ListOptions struct {
	Prefix  string
	MaxKeys int
	Token   string
}

// ListResult is one page of a prefix listing.
type ListResult struct {
	Objects   []Object
	NextToken string
	Truncated bool
}

// Store is the object-store boundary. Keys are relative to the store's bucket;
// the bucket name itself never leaves the server.
type Store interface {
	// Bucket returns the configured bucket name.
	Bucket() string
	// List returns up to MaxKeys objects under the given prefix.
	List(ctx context.Context, opts ListOptions) (ListResult, error)
	// Range returns a bounded read of [offset, offset+length) of the object at
	// key. The caller must close the returned reader.
	Range(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

// ReadWriter extends Store with the write, whole-object read, and delete
// operations used by ingestion. Only trusted server-side code (the ingestion
// worker and upload handler) receives a ReadWriter; query and preview paths
// keep the read-only Store.
type ReadWriter interface {
	Store
	// Put writes data to key, replacing any existing object.
	Put(ctx context.Context, key string, data []byte) error
	// Get reads the whole object at key. It fails with ErrTooLarge when the
	// object exceeds maxBytes, so the read is always bounded.
	Get(ctx context.Context, key string, maxBytes int64) ([]byte, error)
	// Delete removes the object at key; a missing object is not an error.
	Delete(ctx context.Context, key string) error
}

// ErrTooLarge is returned by Get when an object exceeds the caller's bound.
var ErrTooLarge = errors.New("object store: object too large")
