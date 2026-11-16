package logging

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactsSensitiveFields(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "info", "json")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	log.Info("connect", "user", "alice", "password", "hunter2")

	out := buf.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("log output leaked the secret value: %s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("log output did not redact the secret: %s", out)
	}
	if !strings.Contains(out, "alice") {
		t.Fatalf("non-sensitive value was not preserved: %s", out)
	}
}

func TestRedactsSensitiveKeyVariants(t *testing.T) {
	keys := []string{
		"password", "Password", "secret", "api_token", "access_key",
		"secret_key", "authorization", "credential", "private_key",
	}
	for _, key := range keys {
		var buf bytes.Buffer
		log, err := New(&buf, "info", "json")
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		log.Info("event", key, "top-secret-value")
		out := buf.String()
		if strings.Contains(out, "top-secret-value") {
			t.Fatalf("key %q leaked its value: %s", key, out)
		}
		if !strings.Contains(out, "[REDACTED]") {
			t.Fatalf("key %q was not redacted: %s", key, out)
		}
	}
}

func TestRedactsSensitiveFieldInsideGroup(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "info", "json")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	log.Info("connect", slog.Group("database",
		slog.String("user", "alice"),
		slog.String("password", "hunter2"),
	))

	out := buf.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("group leaked the secret value: %s", out)
	}
	if !strings.Contains(out, "alice") {
		t.Fatalf("group lost a non-sensitive value: %s", out)
	}
}

func TestNewRejectsUnknownLevel(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, "bogus", "json"); err == nil {
		t.Fatal("New() expected error for unknown level")
	}
}

func TestNewRejectsUnknownFormat(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, "info", "xml"); err == nil {
		t.Fatal("New() expected error for unknown format")
	}
}

func TestStatementTextAndURLCredentialsAreRedacted(t *testing.T) {
	var buf bytes.Buffer
	log, _ := New(&buf, "info", "json")
	log.Info("x", "statement", "SELECT 'sentinel-literal'", "sql", "SELECT 2", "statement_hash", "abc123",
		"error", errors.New("dial postgres://lakehouse:sentinel-password@db:5432/x failed"),
		"note", "see https://user:sentinel-pass2@host/path")
	out := buf.String()
	for _, bad := range []string{"sentinel-literal", "SELECT 2", "sentinel-password", "sentinel-pass2"} {
		if strings.Contains(out, bad) {
			t.Fatalf("%q leaked: %s", bad, out)
		}
	}
	if !strings.Contains(out, "abc123") {
		t.Fatalf("statement_hash must stay visible: %s", out)
	}
	if !strings.Contains(out, "db:5432/x failed") {
		t.Fatalf("scrubbing must keep the useful part: %s", out)
	}
}
