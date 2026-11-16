package query

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// cacheDirPrefix names the cache directory. It deliberately does not start with
// scratchPrefix, so the stale-scratch sweep can never delete a live cache.
const cacheDirPrefix = "lqs-stagecache-"

// StageCache is a size-bounded, least-recently-used cache of dataset objects on
// local disk. A query "stages" a cached object by hard-linking it into its own
// scratch directory, so a repeated read copies no bytes, and evicting an entry
// never disturbs a query that already holds a link.
//
// An entry is keyed by object key, size and last-modified time. An object
// rewritten with a different size or timestamp is a different entry; an object
// rewritten in place with the same size within the timestamp's resolution
// would be served stale, so dataset prefixes must be treated as immutable
// (ingestion always writes new versioned prefixes).
type StageCache struct {
	dir string
	max int64

	mu       sync.Mutex
	size     int64
	entries  map[string]*cacheEntry
	lru      *list.List // front = most recently used
	inflight map[string]*flight

	hits, misses, evictions int64
}

type cacheEntry struct {
	id   string
	size int64
	el   *list.Element
}

type flight struct {
	done chan struct{}
	err  error
}

// NewStageCache creates a cache under parent holding at most maxBytes. Cache
// directories left behind by crashed workers (older than a day) are removed.
func NewStageCache(parent string, maxBytes int64) (*StageCache, error) {
	if maxBytes <= 0 {
		return nil, errors.New("stage cache size must be positive")
	}
	if ents, err := os.ReadDir(parent); err == nil {
		for _, e := range ents {
			if e.IsDir() && strings.HasPrefix(e.Name(), cacheDirPrefix) {
				if info, err := e.Info(); err == nil && time.Since(info.ModTime()) > 24*time.Hour {
					_ = os.RemoveAll(filepath.Join(parent, e.Name()))
				}
			}
		}
	}
	dir, err := os.MkdirTemp(parent, cacheDirPrefix)
	if err != nil {
		return nil, fmt.Errorf("stage cache directory: %w", err)
	}
	return &StageCache{
		dir: dir, max: maxBytes,
		entries: map[string]*cacheEntry{}, lru: list.New(), inflight: map[string]*flight{},
	}, nil
}

// Close removes the cache directory.
func (c *StageCache) Close() error { return os.RemoveAll(c.dir) }

// CacheStats is a point-in-time view of the cache.
type CacheStats struct {
	Hits, Misses, Evictions int64
	Entries                 int
	Bytes                   int64
}

// Stats reports counters and current occupancy.
func (c *StageCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{Hits: c.hits, Misses: c.misses, Evictions: c.evictions, Entries: len(c.entries), Bytes: c.size}
}

func cacheID(key string, size int64, mod time.Time) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", key, size, mod.UnixNano())))
	return hex.EncodeToString(h[:16])
}

// Stage places the object at dest (a path under a query's private scratch
// directory) and reports whether it came from the cache. load fetches the
// object's bytes on a miss; concurrent misses for one entry share a single load.
func (c *StageCache) Stage(ctx context.Context, key string, size int64, mod time.Time, dest string, load func() ([]byte, error)) (hit bool, err error) {
	if size > c.max {
		// Too large to ever keep: bypass the cache entirely.
		data, err := load()
		if err != nil {
			return false, err
		}
		return false, writeFile(dest, data)
	}
	id := cacheID(key, size, mod)
	path := filepath.Join(c.dir, id)

	for {
		c.mu.Lock()
		if e, ok := c.entries[id]; ok {
			c.lru.MoveToFront(e.el)
			c.mu.Unlock()
			if err := linkOrCopy(path, dest); err == nil {
				c.mu.Lock()
				c.hits++
				c.mu.Unlock()
				return true, nil
			}
			// The file vanished (eviction race): forget it and reload.
			c.mu.Lock()
			if cur, ok := c.entries[id]; ok && cur == e {
				c.removeLocked(e)
			}
			c.mu.Unlock()
			continue
		}
		if f, ok := c.inflight[id]; ok {
			c.mu.Unlock()
			select {
			case <-f.done:
				if f.err != nil {
					return false, f.err
				}
				continue // loaded by someone else: take the hit path
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		f := &flight{done: make(chan struct{})}
		c.inflight[id] = f
		c.misses++
		c.mu.Unlock()

		err := c.fill(id, path, size, load)
		c.mu.Lock()
		delete(c.inflight, id)
		f.err = err
		close(f.done)
		c.mu.Unlock()
		if err != nil {
			return false, err
		}
		if err := linkOrCopy(path, dest); err != nil {
			return false, err
		}
		return false, nil
	}
}

func (c *StageCache) fill(id, path string, size int64, load func() ([]byte, error)) error {
	data, err := load()
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.dir, "tmp-")
	if err != nil {
		return fmt.Errorf("stage cache: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("stage cache: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("stage cache: %w", err)
	}
	if err := os.Chmod(tmp.Name(), 0o600); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("stage cache: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("stage cache: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &cacheEntry{id: id, size: int64(len(data))}
	e.el = c.lru.PushFront(e)
	c.entries[id] = e
	c.size += e.size
	for c.size > c.max {
		back := c.lru.Back()
		if back == nil || back.Value.(*cacheEntry) == e {
			break
		}
		c.removeLocked(back.Value.(*cacheEntry))
		c.evictions++
	}
	return nil
}

func (c *StageCache) removeLocked(e *cacheEntry) {
	c.lru.Remove(e.el)
	delete(c.entries, e.id)
	c.size -= e.size
	_ = os.Remove(filepath.Join(c.dir, e.id))
}

func writeFile(dest string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if err := os.WriteFile(dest, data, 0o600); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	return nil
}

// linkOrCopy hard-links src to dest, falling back to a copy when a link is not
// possible (for example across filesystems).
func linkOrCopy(src, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if err := os.Link(src, dest); err == nil {
		return nil
	} else if errors.Is(err, os.ErrNotExist) {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("stage: %w", err)
	}
	return out.Close()
}
