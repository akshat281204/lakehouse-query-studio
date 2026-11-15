//go:build integration

package catalog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPostgresSavedQueriesLifecycleAndUniqueness(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(`DELETE FROM saved_queries`)
	s := NewPostgresSavedQueryStore(db)
	name := fmt.Sprintf("IT %d", time.Now().UnixNano())

	a, err := s.Create(ctx, SavedQueryInput{Name: name, Description: "d", Statement: "SELECT 1"})
	if err != nil || a.ID == "" {
		t.Fatalf("create: %+v %v", a, err)
	}
	if _, err := s.Create(ctx, SavedQueryInput{Name: " " + lower(name), Statement: "SELECT 2"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate name (case-insensitive): %v", err)
	}
	got, err := s.Get(ctx, a.ID)
	if err != nil || got.Statement != "SELECT 1" || got.Description != "d" {
		t.Fatalf("get: %+v %v", got, err)
	}
	b, _ := s.Create(ctx, SavedQueryInput{Name: name + " b", Statement: "SELECT 2"})
	if _, err := s.Update(ctx, b.ID, SavedQueryInput{Name: name, Statement: "SELECT 3"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("rename onto an existing name: %v", err)
	}
	up, err := s.Update(ctx, a.ID, SavedQueryInput{Name: name + " renamed", Statement: "SELECT 9"})
	if err != nil || up.Statement != "SELECT 9" || !up.UpdatedAt.After(a.UpdatedAt) {
		t.Fatalf("update: %+v %v", up, err)
	}
	list, total, err := s.List(ctx, 0, 10)
	if err != nil || total != 2 || len(list) != 2 || list[0].ID != a.ID {
		t.Fatalf("list: %+v total=%d %v", list, total, err)
	}
	if err := s.Delete(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	for _, bad := range []string{"not-a-uuid", "", "00000000-0000-0000-0000-000000000000"} {
		if _, err := s.Get(ctx, bad); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%q): %v", bad, err)
		}
	}
	_, _ = db.Exec(`DELETE FROM saved_queries`)
}

// Concurrent creates must not overshoot the cap (a count check alone races).
func TestPostgresSavedQueryCapHoldsUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(`DELETE FROM saved_queries`)
	defer func() { _, _ = db.Exec(`DELETE FROM saved_queries`) }()
	// Fill to one below the cap directly, then race several creates for the last slot.
	if _, err := db.Exec(`INSERT INTO saved_queries (id, name, statement)
		SELECT gen_random_uuid(), 'fill ' || g, 'SELECT 1' FROM generate_series(1, $1) g`, MaxSavedQueries-1); err != nil {
		t.Fatal(err)
	}
	s := NewPostgresSavedQueryStore(db)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.Create(ctx, SavedQueryInput{Name: fmt.Sprintf("racer %d", i), Statement: "SELECT 1"}); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM saved_queries`).Scan(&n)
	if n > MaxSavedQueries || ok != 1 {
		t.Fatalf("cap violated: %d rows, %d creates succeeded (want exactly 1 for the last slot)", n, ok)
	}
}

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}
