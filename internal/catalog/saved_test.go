package catalog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSavedQueryNormalizeRules(t *testing.T) {
	ok, err := NormalizeSavedQuery(SavedQueryInput{Name: "  Revenue by region  ", Description: " d ", Statement: "SELECT 1"})
	if err != nil || ok.Name != "Revenue by region" || ok.Description != "d" {
		t.Fatalf("trim: %+v %v", ok, err)
	}
	for name, in := range map[string]SavedQueryInput{
		"no name":      {Name: "  ", Statement: "SELECT 1"},
		"long name":    {Name: strings.Repeat("x", MaxSavedNameChars+1), Statement: "SELECT 1"},
		"control char": {Name: "a\x00b", Statement: "SELECT 1"},
		"long desc":    {Name: "n", Description: strings.Repeat("x", MaxSavedDescription+1), Statement: "SELECT 1"},
		"no statement": {Name: "n", Statement: " \n"},
	} {
		if _, err := NormalizeSavedQuery(in); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// The limits count characters, not bytes.
	if _, err := NormalizeSavedQuery(SavedQueryInput{Name: strings.Repeat("é", MaxSavedNameChars), Statement: "SELECT 1"}); err != nil {
		t.Errorf("multi-byte name at the limit rejected: %v", err)
	}
}

func TestMemorySavedQueryLifecycle(t *testing.T) {
	ctx := context.Background()
	m := NewMemorySavedQueryStore()
	now := time.Unix(1000, 0)
	m.Now = func() time.Time { now = now.Add(time.Second); return now }

	a, err := m.Create(ctx, SavedQueryInput{Name: "Alpha", Statement: "SELECT 1"})
	if err != nil || a.ID == "" || !a.CreatedAt.Equal(a.UpdatedAt) {
		t.Fatalf("create: %+v %v", a, err)
	}
	if _, err := m.Create(ctx, SavedQueryInput{Name: "alpha", Statement: "SELECT 2"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("names are unique ignoring case: %v", err)
	}
	b, _ := m.Create(ctx, SavedQueryInput{Name: "Beta", Statement: "SELECT 2"})

	list, total, _ := m.List(ctx, 0, 10)
	if total != 2 || len(list) != 2 || list[0].ID != b.ID {
		t.Fatalf("most recently updated first: %+v", list)
	}
	up, err := m.Update(ctx, a.ID, SavedQueryInput{Name: "Alpha 2", Description: "x", Statement: "SELECT 3"})
	if err != nil || up.Statement != "SELECT 3" || !up.UpdatedAt.After(up.CreatedAt) {
		t.Fatalf("update: %+v %v", up, err)
	}
	if list, _, _ = m.List(ctx, 0, 10); list[0].ID != a.ID {
		t.Fatalf("an update moves the query to the top: %+v", list)
	}
	if _, err := m.Update(ctx, a.ID, SavedQueryInput{Name: "BETA", Statement: "SELECT 3"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("rename onto another's name: %v", err)
	}
	if _, err := m.Update(ctx, a.ID, SavedQueryInput{Name: "alpha 2", Statement: "SELECT 4"}); err != nil {
		t.Fatalf("keeping its own name (different case) must be allowed: %v", err)
	}
	if err := m.Delete(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get(ctx, b.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted query still readable")
	}
	if err := m.Delete(ctx, b.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := m.Update(ctx, "nope", SavedQueryInput{Name: "x", Statement: "SELECT 1"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update of a missing query: %v", err)
	}
}

func TestMemorySavedQueryCountLimitAndPaging(t *testing.T) {
	ctx := context.Background()
	m := NewMemorySavedQueryStore()
	for i := 0; i < MaxSavedQueries; i++ {
		if _, err := m.Create(ctx, SavedQueryInput{Name: "q" + strings.Repeat("x", 1) + string(rune('A'+i%26)) + itoa(i), Statement: "SELECT 1"}); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}
	if _, err := m.Create(ctx, SavedQueryInput{Name: "one too many", Statement: "SELECT 1"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("limit: %v", err)
	}
	page, total, _ := m.List(ctx, 495, 50)
	if total != MaxSavedQueries || len(page) != 5 {
		t.Fatalf("last page: total=%d len=%d", total, len(page))
	}
	if page, _, _ = m.List(ctx, 10_000, 10); len(page) != 0 {
		t.Fatal("offset past the end must be empty")
	}
	if page, _, _ = m.List(ctx, -3, 100_000); len(page) > maxSavedListPageSize {
		t.Fatal("page size must be clamped")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}
