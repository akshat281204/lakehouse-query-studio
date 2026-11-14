// Package logging constructs the structured logger used across the process and
// enforces a redaction policy so secrets never reach log output.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
)

// New builds a structured slog.Logger writing to w with the given level and
// format. Every record passes through a redacting handler.
func New(w io.Writer, level, format string) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{
		Level:     lvl,
		AddSource: lvl == slog.LevelDebug,
	}

	var base slog.Handler
	switch strings.ToLower(format) {
	case "json":
		base = slog.NewJSONHandler(w, opts)
	case "text":
		base = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q", format)
	}

	return slog.New(&redactingHandler{next: base}), nil
}

func parseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", level)
	}
}

// sensitive reports whether an attribute key names a value that must be
// redacted from log output.
func sensitive(key string) bool {
	k := strings.ToLower(key)
	// Query text must never be logged. Exact keys only: "statement_hash" is
	// the safe, intended identifier.
	switch k {
	case "statement", "sql", "query_text", "stmt", "query_sql":
		return true
	}
	for _, frag := range []string{
		"password", "secret", "token", "credential", "authorization",
		"api_key", "apikey", "access_key", "secret_key", "private_key",
	} {
		if strings.Contains(k, frag) {
			return true
		}
	}
	return false
}

// urlCreds matches the userinfo of a URL (scheme://user:password@host).
var urlCreds = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://)[^/\s:@]*:[^/\s@]*@`)

// scrubURLCredentials masks credentials embedded in URLs, which libraries
// (database drivers in particular) sometimes echo inside error strings.
func scrubURLCredentials(s string) string {
	if !strings.Contains(s, "@") {
		return s
	}
	return urlCreds.ReplaceAllString(s, "${1}[REDACTED]@")
}

// redactingHandler wraps a slog.Handler and masks the values of sensitive
// attributes before they are written.
type redactingHandler struct {
	next slog.Handler
}

func (h *redactingHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.next.Enabled(ctx, lvl)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	record := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		record.AddAttrs(redactAttr(a))
		return true
	})
	return h.next.Handle(ctx, record)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, redactAttr(a))
	}
	return &redactingHandler{next: h.next.WithAttrs(out)}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{next: h.next.WithGroup(name)}
}

// redactAttr returns the attribute with its value masked when the key is
// sensitive. Group values are redacted recursively.
func redactAttr(a slog.Attr) slog.Attr {
	if sensitive(a.Key) {
		a.Value = slog.StringValue("[REDACTED]")
		return a
	}
	switch a.Value.Kind() {
	case slog.KindString:
		a.Value = slog.StringValue(scrubURLCredentials(a.Value.String()))
	case slog.KindAny:
		if err, ok := a.Value.Any().(error); ok && err != nil {
			a.Value = slog.StringValue(scrubURLCredentials(err.Error()))
		}
	}
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		out := make([]slog.Attr, 0, len(group))
		for _, g := range group {
			out = append(out, redactAttr(g))
		}
		a.Value = slog.GroupValue(out...)
	}
	return a
}
