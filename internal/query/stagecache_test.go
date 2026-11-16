package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStageCacheHitMissAndLinkIsolation(t *testing.T) {
	c, err := NewStageCache(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	dir := t.TempDir()
	mod := time.Unix(100, 0)
	loads := 0
	load := func() ([]byte, error) { loads++; return []byte("hello"), nil }

	hit, err := c.Stage(context.Background(), "k", 5, mod, filepath.Join(dir, "a", "f.parquet"), load)
	if err != nil || hit {
		t.Fatalf("first: hit=%v err=%v", hit, err)
	}
	hit, err = c.Stage(context.Background(), "k", 5, mod, filepath.Join(dir, "b", "f.parquet"), load)
	if err != nil || !hit || loads != 1 {
		t.Fatalf("second: hit=%v err=%v loads=%d", hit, err, loads)
	}
	// Removing a query's copy (its scratch cleanup) must not damage the cache.
	_ = os.RemoveAll(filepath.Join(dir, "a"))
	if b, err := os.ReadFile(filepath.Join(dir, "b", "f.parquet")); err != nil || string(b) != "hello" {
		t.Fatalf("copy b: %q %v", b, err)
	}
	if hit, err = c.Stage(context.Background(), "k", 5, mod, filepath.Join(dir, "c.parquet"), load); err != nil || !hit {
		t.Fatalf("after scratch removal: hit=%v err=%v", hit, err)
	}
	// A different timestamp is a different object.
	if hit, _ = c.Stage(context.Background(), "k", 5, mod.Add(time.Second), filepath.Join(dir, "d.parquet"), load); hit {
		t.Fatal("changed timestamp must miss")
	}
}

func TestStageCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c, err := NewStageCache(t.TempDir(), 25)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	dir := t.TempDir()
	n := 0
	put := func(key string) bool {
		n++
		hit, err := c.Stage(context.Background(), key, 10, time.Unix(1, 0), filepath.Join(dir, fmt.Sprintf("%s-%d", key, n)), func() ([]byte, error) {
			return make([]byte, 10), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return hit
	}
	put("a")
	put("b")
	put("a") // a is now most recent
	put("c") // 30 > 25 bytes: evicts b, the least recently used
	if s := c.Stats(); s.Entries != 2 || s.Bytes != 20 || s.Evictions != 1 {
		t.Fatalf("stats: %+v", s)
	}
	if !put("a") || put("b") {
		t.Fatal("expected a retained and b evicted")
	}
}

func TestStageCacheBypassesObjectsLargerThanTheBudget(t *testing.T) {
	c, err := NewStageCache(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	dest := filepath.Join(t.TempDir(), "big")
	hit, err := c.Stage(context.Background(), "k", 100, time.Unix(1, 0), dest, func() ([]byte, error) { return make([]byte, 100), nil })
	if err != nil || hit {
		t.Fatalf("hit=%v err=%v", hit, err)
	}
	if st, _ := os.Stat(dest); st == nil || st.Size() != 100 {
		t.Fatal("oversized object was not staged")
	}
	if s := c.Stats(); s.Entries != 0 {
		t.Fatalf("oversized object cached: %+v", s)
	}
}

func TestStageCacheConcurrentMissesLoadOnce(t *testing.T) {
	c, err := NewStageCache(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	dir := t.TempDir()
	var loads atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Stage(context.Background(), "k", 4, time.Unix(1, 0), filepath.Join(dir, string(rune('a'+i))), func() ([]byte, error) {
				loads.Add(1)
				time.Sleep(20 * time.Millisecond)
				return []byte("data"), nil
			})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if loads.Load() != 1 {
		t.Fatalf("loaded %d times, want 1", loads.Load())
	}
}

func TestStageCacheLoadErrorIsNotCached(t *testing.T) {
	c, _ := NewStageCache(t.TempDir(), 1<<20)
	defer func() { _ = c.Close() }()
	dest := filepath.Join(t.TempDir(), "x")
	if _, err := c.Stage(context.Background(), "k", 4, time.Unix(1, 0), dest, func() ([]byte, error) { return nil, os.ErrDeadlineExceeded }); err == nil {
		t.Fatal("expected error")
	}
	hit, err := c.Stage(context.Background(), "k", 4, time.Unix(1, 0), dest, func() ([]byte, error) { return []byte("okok"), nil })
	if err != nil || hit {
		t.Fatalf("retry: hit=%v err=%v", hit, err)
	}
}

func TestStageCacheDirectoryIsPrivateAndRemovedOnClose(t *testing.T) {
	parent := t.TempDir()
	c, _ := NewStageCache(parent, 1<<20)
	st, err := os.Stat(c.dir)
	if err != nil || st.Mode().Perm()&0o077 != 0 {
		t.Fatalf("cache dir mode: %v %v", st, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if ents, _ := os.ReadDir(parent); len(ents) != 0 {
		t.Fatalf("left behind: %v", ents)
	}
}
