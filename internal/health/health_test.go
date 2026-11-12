package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type okPinger struct{}

func (okPinger) PingContext(context.Context) error { return nil }

type errPinger struct{ err error }

func (p errPinger) PingContext(context.Context) error { return p.err }

type namedChecker struct {
	name string
	err  error
}

func (c namedChecker) Name() string                { return c.name }
func (c namedChecker) Check(context.Context) error { return c.err }

func TestAggregateAllOK(t *testing.T) {
	agg := NewAggregate(
		namedChecker{name: "a"},
		namedChecker{name: "b"},
	)
	results := agg.Check(context.Background())
	if !AllOK(results) {
		t.Fatalf("AllOK() = false, want true; results = %+v", results)
	}
}

func TestAggregateReportsFailure(t *testing.T) {
	agg := NewAggregate(
		namedChecker{name: "a"},
		namedChecker{name: "b", err: errors.New("down")},
	)
	results := agg.Check(context.Background())
	if AllOK(results) {
		t.Fatal("AllOK() = true, want false")
	}
	if results["a"].OK != true {
		t.Errorf("dependency a should be ok: %+v", results["a"])
	}
	if results["b"].OK {
		t.Errorf("dependency b should be failing: %+v", results["b"])
	}
	if results["b"].Message != "down" {
		t.Errorf("dependency b message = %q, want down", results["b"].Message)
	}
}

func TestPostgresNotConfigured(t *testing.T) {
	p := NewPostgres("postgres", nil)
	if err := p.Check(context.Background()); err == nil {
		t.Fatal("Check() expected error for unconfigured postgres")
	}
}

func TestPostgresPings(t *testing.T) {
	p := NewPostgres("postgres", okPinger{})
	if err := p.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	p = NewPostgres("postgres", errPinger{err: errors.New("dial failed")})
	if err := p.Check(context.Background()); err == nil {
		t.Fatal("Check() expected error from failing pinger")
	}
}

func TestMinIOReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	m := NewMinIO("minio", addr, false, nil)
	if err := m.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v, want nil", err)
	}
}

func TestMinIONotReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	m := NewMinIO("minio", addr, false, nil)
	if err := m.Check(context.Background()); err == nil {
		t.Fatal("Check() expected error for 503 response")
	}
}
