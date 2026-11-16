//go:build integration

package catalog

import (
	"context"
	"testing"
	"time"
)

func waitSignal(ch <-chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

func TestSubmittedQueryWakesAListeningWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	wake := Listen(ctx, db, nil, ChannelQueryJobs)
	if !waitSignal(wake, 3*time.Second) { // the signal sent after subscribing
		t.Fatal("listener never subscribed")
	}

	store := NewPostgresQueryStore(db)
	id, _ := NewUUID()
	start := time.Now()
	if _, err := store.Submit(ctx, SubmitQuery{ID: id, Statement: "SELECT 1", StatementHash: "h", Bindings: map[string]string{}}); err != nil {
		t.Fatal(err)
	}
	if !waitSignal(wake, 2*time.Second) {
		t.Fatal("no wake-up after Submit")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("wake-up took %s", d)
	}
	_, _ = db.Exec(`DELETE FROM query_jobs WHERE id = $1`, id)
}

// A notification sent inside a transaction that rolls back must never be
// delivered: a worker must not wake for a job that does not exist.
func TestRolledBackSubmitDoesNotWakeWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := testDB(t)
	wake := Listen(ctx, db, nil, ChannelQueryJobs)
	if !waitSignal(wake, 3*time.Second) {
		t.Fatal("listener never subscribed")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := notify(ctx, tx, ChannelQueryJobs); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if waitSignal(wake, 500*time.Millisecond) {
		t.Fatal("woken by a rolled-back notification")
	}
}

func TestListenerSurvivesItsConnectionBeingKilled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := testDB(t)
	wake := Listen(ctx, db, nil, ChannelQueryJobs)
	if !waitSignal(wake, 3*time.Second) {
		t.Fatal("listener never subscribed")
	}
	// Terminate every other backend LISTENing on the channel.
	if _, err := db.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE pid <> pg_backend_pid() AND datname = current_database() AND query LIKE 'LISTEN %'`); err != nil {
		t.Fatal(err)
	}
	// It reconnects and signals again after resubscribing.
	if !waitSignal(wake, 5*time.Second) {
		t.Fatal("listener did not reconnect")
	}
	if err := notify(ctx, db, ChannelQueryJobs); err != nil {
		t.Fatal(err)
	}
	if !waitSignal(wake, 2*time.Second) {
		t.Fatal("no wake-up after reconnect")
	}
}
