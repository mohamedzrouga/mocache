// Package cache implements a capacity-bounded in-memory LRU with per-key TTL.
//
// Concurrency: every public method takes c.mu. Values are copied on Set/Get so
// callers cannot mutate entries behind the lock (and so a racing caller cannot
// observe a torn slice).
//
// Memory: item count is hard-capped. Expired entries are dropped on access and
// by an optional janitor so TTL'd keys that are never read do not occupy slots
// until LRU eviction. removeLocked nils pointers to help the GC.
package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key      string
	value    []byte
	expireAt time.Time // zero means no expiry
	elem     *list.Element
}

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	ItemCount int
}

// Cache is a capacity-bounded in-memory LRU with optional per-key TTL.
type Cache struct {
	mu        sync.Mutex
	capacity  int
	items     map[string]*entry
	order     *list.List // front = most recently used
	hits      uint64
	misses    uint64
	evictions uint64

	janitorOnce sync.Once
	closeOnce   sync.Once
	stopJanitor chan struct{}
	janitorDone chan struct{}
}

func New(capacity int) *Cache {
	if capacity < 1 {
		capacity = 1
	}
	return &Cache{
		capacity: capacity,
		items:    make(map[string]*entry, capacity),
		order:    list.New(),
	}
}

// StartJanitor periodically drops expired entries so unused TTL keys cannot
// pin memory until they happen to be the LRU victim. interval <= 0 disables it.
// Safe to call at most once; subsequent calls are no-ops.
func (c *Cache) StartJanitor(interval time.Duration) {
	if interval <= 0 {
		return
	}
	c.janitorOnce.Do(func() {
		c.stopJanitor = make(chan struct{})
		c.janitorDone = make(chan struct{})
		go c.janitorLoop(interval)
	})
}

// Close stops the janitor (if any). Idempotent; does not clear stored items.
func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		// Consume janitorOnce so a racing StartJanitor cannot spawn after drain.
		c.janitorOnce.Do(func() {})
		if c.stopJanitor != nil {
			close(c.stopJanitor)
		}
		if c.janitorDone != nil {
			<-c.janitorDone
		}
	})
}

func (c *Cache) janitorLoop(interval time.Duration) {
	defer close(c.janitorDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stopJanitor:
			return
		case <-t.C:
			c.purgeExpired()
		}
	}
}

func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	// Copy so the caller's backing array can be reused or GC'd independently.
	v := make([]byte, len(value))
	copy(v, value)

	c.mu.Lock()
	defer c.mu.Unlock()

	expireAt := time.Time{}
	if ttl > 0 {
		expireAt = time.Now().Add(ttl)
	}

	if e, ok := c.items[key]; ok {
		e.value = v
		e.expireAt = expireAt
		c.order.MoveToFront(e.elem)
		return
	}

	if c.order.Len() >= c.capacity {
		c.evictLocked()
	}
	e := &entry{key: key, value: v, expireAt: expireAt}
	e.elem = c.order.PushFront(e)
	c.items[key] = e
}

func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[key]
	if !ok || expired(e, time.Now()) {
		if ok {
			c.removeLocked(e)
		}
		c.misses++
		return nil, false
	}
	c.order.MoveToFront(e.elem)
	c.hits++
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.removeLocked(e)
	}
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
		ItemCount: c.order.Len(),
	}
}

// purgeExpired walks the recency list once. Called from the janitor; the lock
// is held for the walk so 100k items is a short, bounded pause.
func (c *Cache) purgeExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for e := c.order.Back(); e != nil; {
		prev := e.Prev()
		ent := e.Value.(*entry)
		if expired(ent, now) {
			c.removeLocked(ent)
		}
		e = prev
	}
}

func (c *Cache) evictLocked() {
	back := c.order.Back()
	if back == nil {
		return
	}
	c.removeLocked(back.Value.(*entry))
	c.evictions++
}

func (c *Cache) removeLocked(e *entry) {
	c.order.Remove(e.elem)
	delete(c.items, e.key)
	// Drop references so the value bytes and list element can be collected
	// without waiting for the entry struct itself to die.
	e.elem = nil
	e.value = nil
}

func expired(e *entry, now time.Time) bool {
	return !e.expireAt.IsZero() && now.After(e.expireAt)
}
