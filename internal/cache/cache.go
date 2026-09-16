package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key      string
	value    []byte
	expireAt time.Time
	elem     *list.Element
}

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
	order     *list.List
	hits      uint64
	misses    uint64
	evictions uint64
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

func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
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
	if !ok || expired(e) {
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
}

func expired(e *entry) bool {
	return !e.expireAt.IsZero() && time.Now().After(e.expireAt)
}
