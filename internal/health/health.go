// Package health defines dependency health checks for the API readiness probe.
package health

import (
	"context"
	"errors"
)

// Result is the outcome of a single dependency check.
type Result struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// Checker probes a single dependency.
type Checker interface {
	// Name returns the stable dependency name used in readiness output.
	Name() string
	// Check returns nil when the dependency is healthy.
	Check(ctx context.Context) error
}

// Aggregate runs a set of checkers and reports each dependency's status.
type Aggregate struct {
	checkers []Checker
}

// NewAggregate builds an Aggregate from the given checkers.
func NewAggregate(checkers ...Checker) *Aggregate {
	return &Aggregate{checkers: checkers}
}

// Check runs every checker and returns a map keyed by checker name.
func (a *Aggregate) Check(ctx context.Context) map[string]Result {
	out := make(map[string]Result, len(a.checkers))
	for _, c := range a.checkers {
		if err := c.Check(ctx); err != nil {
			out[c.Name()] = Result{OK: false, Message: err.Error()}
			continue
		}
		out[c.Name()] = Result{OK: true}
	}
	return out
}

// AllOK reports whether every result is healthy.
func AllOK(results map[string]Result) bool {
	for _, r := range results {
		if !r.OK {
			return false
		}
	}
	return true
}

// Pinger is the subset of *sql.DB used by the Postgres checker.
type Pinger interface {
	PingContext(ctx context.Context) error
}

// Postgres probes a PostgreSQL connection.
type Postgres struct {
	name   string
	pinger Pinger
}

// NewPostgres builds a Postgres checker. A nil pinger makes the check report
// the database as not configured.
func NewPostgres(name string, p Pinger) *Postgres {
	return &Postgres{name: name, pinger: p}
}

// Name returns the checker's dependency name.
func (p *Postgres) Name() string { return p.name }

// Check pings the database. It returns an error when the database is not
// configured or is unreachable.
func (p *Postgres) Check(ctx context.Context) error {
	if p.pinger == nil {
		return errors.New("postgres not configured")
	}
	return p.pinger.PingContext(ctx)
}

// ObjectStore probes an S3-compatible object store with a caller-supplied
// probe. The API passes a signed, bounded List call, which proves connectivity,
// credentials and the bucket together and works with any S3-compatible server
// (it does not depend on a vendor-specific health endpoint).
type ObjectStore struct {
	name  string
	probe func(ctx context.Context) error
}

// NewObjectStore builds the checker. A nil probe reports not configured.
func NewObjectStore(name string, probe func(ctx context.Context) error) *ObjectStore {
	return &ObjectStore{name: name, probe: probe}
}

// Name returns the checker's dependency name.
func (o *ObjectStore) Name() string { return o.name }

// Check runs the probe.
func (o *ObjectStore) Check(ctx context.Context) error {
	if o.probe == nil {
		return errors.New("object store not configured")
	}
	return o.probe(ctx)
}
