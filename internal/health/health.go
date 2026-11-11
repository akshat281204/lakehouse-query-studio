// Package health defines dependency health checks for the API readiness probe.
package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
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

// MinIO probes a MinIO server's readiness endpoint.
type MinIO struct {
	name   string
	client *http.Client
	url    string
}

// NewMinIO builds a MinIO checker that GETs the server's readiness endpoint.
func NewMinIO(name, endpoint string, useSSL bool, client *http.Client) *MinIO {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	scheme := "http"
	if useSSL {
		scheme = "https"
	}
	return &MinIO{
		name:   name,
		client: client,
		url:    fmt.Sprintf("%s://%s/minio/health/ready", scheme, endpoint),
	}
}

// Name returns the checker's dependency name.
func (m *MinIO) Name() string { return m.name }

// Check probes the MinIO readiness endpoint.
func (m *MinIO) Check(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url, nil)
	if err != nil {
		return err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("minio health returned status %d", resp.StatusCode)
	}
	return nil
}
