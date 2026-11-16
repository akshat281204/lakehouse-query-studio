// Package sweeper enforces retention (design §9, §10): spooled query results
// expire after EXPORT_TTL, finished query rows (statement text included) are
// deleted after HISTORY_RETENTION, and leftover staging objects are removed.
//
// It deletes only under the exports/ and staging/ prefixes. lake/ and uploads/
// are never touched, whatever the database or the object listing says.
package sweeper

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/akshat281204/lakehouse-query-studio/internal/catalog"
	"github.com/akshat281204/lakehouse-query-studio/internal/objectstore"
	"github.com/akshat281204/lakehouse-query-studio/internal/telemetry"
)

const (
	exportsPrefix = "exports/"
	stagingPrefix = "staging/"
	// maxDeletesPerPass bounds work (and blast radius) of one sweep.
	maxDeletesPerPass = 10_000
)

// Config holds the retention windows.
type Config struct {
	ExportTTL        time.Duration
	HistoryRetention time.Duration
	StagingTTL       time.Duration
	Interval         time.Duration
}

// Sweeper removes expired data.
type Sweeper struct {
	Store   catalog.QueryStore
	Objects objectstore.ReadWriter
	Cfg     Config
	Log     *slog.Logger
	Metrics *telemetry.Metrics
	Now     func() time.Time
}

// Result counts what one pass removed.
type Result struct {
	Exports, Staging, History int
}

func (s *Sweeper) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// deletable reports whether key may ever be removed by the sweeper.
func deletable(key string) bool {
	if strings.Contains(key, "..") {
		return false
	}
	return strings.HasPrefix(key, exportsPrefix) || strings.HasPrefix(key, stagingPrefix)
}

func (s *Sweeper) deleteKeys(ctx context.Context, keys []string) int {
	n := 0
	for _, k := range keys {
		if !deletable(k) {
			s.Log.Warn("sweeper refused to delete a key outside exports/ and staging/")
			continue
		}
		if err := s.Objects.Delete(ctx, k); err != nil {
			if ctx.Err() != nil {
				return n
			}
			s.Log.Warn("sweeper delete failed", "error", "object store error")
			continue
		}
		n++
	}
	return n
}

// ageSweep deletes objects under prefix older than ttl. Objects whose age is
// unknown (zero LastModified) are kept.
func (s *Sweeper) ageSweep(ctx context.Context, prefix string, ttl time.Duration, budget int) int {
	cutoff := s.now().Add(-ttl)
	deleted := 0
	token := ""
	for deleted < budget {
		page, err := s.Objects.List(ctx, objectstore.ListOptions{Prefix: prefix, MaxKeys: 1000, Token: token})
		if err != nil {
			if ctx.Err() == nil {
				s.Log.Warn("sweeper list failed", "error", "object store error")
			}
			return deleted
		}
		var old []string
		for _, o := range page.Objects {
			if !o.LastModified.IsZero() && o.LastModified.Before(cutoff) {
				old = append(old, o.Key)
			}
		}
		deleted += s.deleteKeys(ctx, old)
		if !page.Truncated || page.NextToken == "" {
			break
		}
		token = page.NextToken
	}
	return deleted
}

// Once runs a single sweep pass.
func (s *Sweeper) Once(ctx context.Context) Result {
	var res Result
	now := s.now()

	// 1. Expire results: clear the reference first (the API then answers
	// result_expired), then delete the objects.
	if refs, err := s.Store.ExpireResults(ctx, now.Add(-s.Cfg.ExportTTL)); err != nil {
		s.Log.Warn("expire results failed", "error", "store error")
	} else {
		res.Exports += s.deleteKeys(ctx, refs)
	}

	// 2. Purge old history (and any spool it still pointed at).
	if n, refs, err := s.Store.PurgeHistory(ctx, now.Add(-s.Cfg.HistoryRetention)); err != nil {
		s.Log.Warn("purge history failed", "error", "store error")
	} else {
		res.History = n
		res.Exports += s.deleteKeys(ctx, refs)
	}

	// 3. Age sweeps catch orphans no row references (fenced attempts, failed
	// cleanups, abandoned staging).
	res.Exports += s.ageSweep(ctx, exportsPrefix, s.Cfg.ExportTTL, maxDeletesPerPass)
	res.Staging += s.ageSweep(ctx, stagingPrefix, s.Cfg.StagingTTL, maxDeletesPerPass)

	if s.Metrics != nil {
		s.Metrics.SweeperDeleted.Add(float64(res.Exports), "export")
		s.Metrics.SweeperDeleted.Add(float64(res.Staging), "staging")
		s.Metrics.SweeperDeleted.Add(float64(res.History), "history")
	}
	if res != (Result{}) {
		s.Log.Info("retention sweep", "exports", res.Exports, "staging", res.Staging, "history", res.History)
	}
	return res
}

// Run sweeps every Interval until ctx ends.
func (s *Sweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.Cfg.Interval)
	defer t.Stop()
	s.Once(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Once(ctx)
		}
	}
}
