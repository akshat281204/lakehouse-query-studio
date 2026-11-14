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
