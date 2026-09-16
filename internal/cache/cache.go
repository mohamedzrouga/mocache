// Package cache implements a capacity-bounded in-memory LRU with per-key TTL.
//
// Concurrency: every public method takes c.mu. Values are copied on Set/Get so
// callers cannot mutate entries behind the lock (and so a racing caller cannot
// observe a torn slice).
//
// Memory: two hard caps — item count AND approximate byte cost (key+value+
// overhead). A single Set that would exceed maxBytes or maxValue is rejected
// rather than admitted. LRU eviction runs until both caps are satisfied, so
// the map cannot grow unbounded under any request pattern (the process-level
// debug.SetMemoryLimit is a second backstop in cmd/mocache).
package cache

import (
	"container/list"
	"errors"
	"regexp"
	"strings"
	"sync"
	"time"
)

// entryOverhead accounts for the list element, map bucket, and struct padding
// so byte accounting is not just len(value).
const entryOverhead = 96

var (
	// ErrTooLarge means the entry itself cannot fit the configured caps.
	ErrTooLarge = errors.New("mocache: entry exceeds memory limits")
	// ErrBadPattern means a regex could not be compiled (Go's RE2 is linear-time).
	ErrBadPattern = errors.New("mocache: invalid regex")
)

type entry struct {
	key      string
	value    []byte
	cost     int64
	expireAt time.Time // zero means no expiry
	elem     *list.Element
}

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	Hits          uint64
	Misses        uint64
	Evictions     uint64
	Invalidations uint64
	ItemCount     int
	Bytes         int64
	MaxItems      int
	MaxBytes      int64
}

// Limits are the hard memory bounds for one node. Zero MaxBytes/MaxValue/MaxKey
// pick safe defaults so a misconfigured process still cannot OOM the node.
type Limits struct {
	MaxItems int
	MaxBytes int64
	MaxValue int
	MaxKey   int
}

// Cache is a capacity-bounded in-memory LRU with optional per-key TTL.
type Cache struct {
	mu        sync.Mutex
	maxItems  int
	maxBytes  int64
	maxValue  int
	maxKey    int
	nbytes    int64
	items     map[string]*entry
	order     *list.List // front = most recently used
	hits      uint64
	misses    uint64
	evictions uint64
	invals    uint64

	janitorOnce sync.Once
	closeOnce   sync.Once
	stopJanitor chan struct{}
	janitorDone chan struct{}
}

func New(maxItems int) *Cache {
	return NewWithLimits(Limits{MaxItems: maxItems})
}

func NewWithLimits(l Limits) *Cache {
	if l.MaxItems < 1 {
		l.MaxItems = 1
	}
	if l.MaxBytes <= 0 {
		l.MaxBytes = 64 << 20 // 64 MiB payload budget if unset
	}
	if l.MaxValue <= 0 {
		l.MaxValue = 1 << 20 // 1 MiB per value
	}
	if l.MaxKey <= 0 {
		l.MaxKey = 4096
	}
	return &Cache{
		maxItems: l.MaxItems,
		maxBytes: l.MaxBytes,
		maxValue: l.MaxValue,
		maxKey:   l.MaxKey,
		items:    make(map[string]*entry),
		order:    list.New(),
	}
}

// StartJanitor periodically drops expired entries so unused TTL keys cannot
// pin memory until they happen to be the LRU victim. interval <= 0 disables it.
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

func costOf(key string, value []byte) int64 {
	return int64(len(key) + len(value) + entryOverhead)
}

func (c *Cache) Set(key string, value []byte, ttl time.Duration) error {
	if len(key) > c.maxKey || len(value) > c.maxValue {
		return ErrTooLarge
	}
	newCost := costOf(key, value)
	if newCost > c.maxBytes {
		return ErrTooLarge
	}

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
		c.nbytes -= e.cost
		e.value = v
		e.cost = newCost
		e.expireAt = expireAt
		c.nbytes += newCost
		c.order.MoveToFront(e.elem)
		c.evictWhileOverLocked()
		return nil
	}

	c.evictWhileOverLocked()
	for (c.order.Len() >= c.maxItems || c.nbytes+newCost > c.maxBytes) && c.order.Len() > 0 {
		c.evictLocked()
	}
	e := &entry{key: key, value: v, cost: newCost, expireAt: expireAt}
	e.elem = c.order.PushFront(e)
	c.items[key] = e
	c.nbytes += newCost
	return nil
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

// InvalidatePrefix deletes every key that starts with prefix. Returns the count.
func (c *Cache) InvalidatePrefix(prefix string) int {
	if prefix == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, e := range c.items {
		if strings.HasPrefix(k, prefix) {
			c.removeLocked(e)
			n++
		}
	}
	c.invals += uint64(n)
	return n
}

// InvalidateRegex deletes every key matching expr (RE2, linear time — no ReDoS).
func (c *Cache) InvalidateRegex(expr string) (int, error) {
	if len(expr) > 512 {
		return 0, ErrBadPattern
	}
	re, err := regexp.Compile(expr)
	if err != nil {
		return 0, ErrBadPattern
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, e := range c.items {
		if re.MatchString(k) {
			c.removeLocked(e)
			n++
		}
	}
	c.invals += uint64(n)
	return n, nil
}

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Hits:          c.hits,
		Misses:        c.misses,
		Evictions:     c.evictions,
		Invalidations: c.invals,
		ItemCount:     c.order.Len(),
		Bytes:         c.nbytes,
		MaxItems:      c.maxItems,
		MaxBytes:      c.maxBytes,
	}
}

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

func (c *Cache) evictWhileOverLocked() {
	for (c.order.Len() > c.maxItems || c.nbytes > c.maxBytes) && c.order.Len() > 0 {
		c.evictLocked()
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
	c.nbytes -= e.cost
	if c.nbytes < 0 {
		c.nbytes = 0
	}
	e.elem = nil
	e.value = nil
}

func expired(e *entry, now time.Time) bool {
	return !e.expireAt.IsZero() && now.After(e.expireAt)
}
