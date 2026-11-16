package catalog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Notification channels. A notification carries no payload: it only tells an
// idle worker to look at the queue now instead of at its next poll. Polling
// stays as the fallback, so a lost notification costs latency, never a job.
const (
	ChannelQueryJobs  = "lqs_query_jobs"
	ChannelIngestJobs = "lqs_ingest_jobs"
)

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// notify sends a wake-up on channel. Inside a transaction it is delivered only
// when the transaction commits, so a worker never wakes for a job that is not
// yet visible.
func notify(ctx context.Context, q execer, channel string) error {
	_, err := q.ExecContext(ctx, `SELECT pg_notify($1, '')`, channel)
	return err
}

// Listen returns a channel that receives a signal whenever a notification
// arrives on any of the given PostgreSQL channels. It holds one dedicated
// connection, reconnects with backoff, and signals once after every
// (re)subscription because jobs may have been queued while it was not
// listening. The returned channel has capacity 1 and sends never block, so
// bursts coalesce. It stops when ctx is cancelled.
func Listen(ctx context.Context, db *sql.DB, log *slog.Logger, channels ...string) <-chan struct{} {
	out := make(chan struct{}, 1)
	go func() {
		backoff := 200 * time.Millisecond
		for ctx.Err() == nil {
			started := time.Now()
			err := listenOnce(ctx, db, channels, out)
			if ctx.Err() != nil {
				return
			}
			if log != nil {
				log.Warn("job notification listener lost; falling back to polling until it reconnects", "error", "listen failed")
			}
			if time.Since(started) > 30*time.Second {
				backoff = 200 * time.Millisecond // it was healthy for a while
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
			_ = err
		}
	}()
	return out
}

func signal(out chan<- struct{}) {
	select {
	case out <- struct{}{}:
	default:
	}
}

func listenOnce(ctx context.Context, db *sql.DB, channels []string, out chan<- struct{}) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var inner error
	_ = conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			inner = errors.New("not a pgx connection")
			return driver.ErrBadConn
		}
		pc := sc.Conn()
		for _, ch := range channels {
			if _, err := pc.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
				inner = err
				return driver.ErrBadConn
			}
		}
		signal(out)
		for {
			if _, err := pc.WaitForNotification(ctx); err != nil {
				inner = err
				// A connection that has run LISTEN must never return to the pool.
				return driver.ErrBadConn
			}
			signal(out)
		}
	})
	return inner
}
