package server

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"regexp"
	"time"
)

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{8,64}$`)

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// accessLog logs one structured line per request and sets X-Request-Id so a
// client report can be matched to the log line. It records the matched route
// pattern, never the raw path or query string (which can carry dataset names,
// object keys, filenames or tokens). An inbound X-Request-Id is honoured only
// when it is a short, safe token, so it cannot be used to inject log content.
func accessLog(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if !requestIDPattern.MatchString(id) {
			id = newRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		start := time.Now()
		sw := &logWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		code := sw.code
		if code == 0 {
			code = http.StatusOK
		}
		log.Info("request", "request_id", id, "method", r.Method, "route", route, "status", code,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

type logWriter struct {
	http.ResponseWriter
	code int
}

func (l *logWriter) WriteHeader(c int) {
	if l.code == 0 {
		l.code = c
	}
	l.ResponseWriter.WriteHeader(c)
}

func (l *logWriter) Write(b []byte) (int, error) {
	if l.code == 0 {
		l.code = http.StatusOK
	}
	return l.ResponseWriter.Write(b)
}

func (l *logWriter) Flush() {
	if f, ok := l.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
