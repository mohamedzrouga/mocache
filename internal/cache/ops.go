// Redis-compatible operations on the LRU.
//
// These exist because the RESP server (internal/server/resp.go) must offer
// read-modify-write commands — INCR, APPEND, SET NX, GETDEL — that clients
// expect to be atomic. Composing them from Get+Set in the server would lose
// updates under concurrency, so each one runs as a single critical section
// here, reusing setLocked for accounting and eviction.
package cache

import (
	"errors"
	"sort"
	"strconv"
	"time"
)

// scanItem pairs a key with its insertion sequence for cursor ordering.
type scanItem struct {
	seq uint64
	key string
}

var (
	// ErrNotInteger mirrors Redis's "value is not an integer or out of range".
	ErrNotInteger = errors.New("mocache: value is not an integer or out of range")
	// ErrNotFloat mirrors Redis's "value is not a valid float".
	ErrNotFloat = errors.New("mocache: value is not a valid float")
	// ErrOverflow means an INCRBY would wrap int64.
	ErrOverflow = errors.New("mocache: increment or decrement would overflow")
)

// SetOptions carries the SET modifiers. TTL <= 0 with KeepTTL false clears any
// existing expiry, which is what plain SET does in Redis.
type SetOptions struct {
	TTL     time.Duration
	KeepTTL bool
	NX      bool // only set if absent
	XX      bool // only set if present
	Get     bool // also return the previous value
}

// SetResult reports what SET did. Stored is false when NX/XX rejected the write.
type SetResult struct {
	Prev      []byte
	PrevFound bool
	Stored    bool
}

// SetWithOptions implements SET/SETNX/SETEX/GETSET in one atomic step.
func (c *Cache) SetWithOptions(key string, value []byte, opt SetOptions) (SetResult, error) {
	if err := c.fits(key, value); err != nil {
		return SetResult{}, err
	}
	v := make([]byte, len(value))
	copy(v, value)

	c.mu.Lock()
	defer c.mu.Unlock()

	var res SetResult
	e := c.liveLocked(key)
	if e != nil && opt.Get {
		res.Prev = append([]byte(nil), e.value...)
		res.PrevFound = true
	}
	if (opt.NX && e != nil) || (opt.XX && e == nil) {
		return res, nil
	}

	expireAt := time.Time{}
	switch {
	case opt.KeepTTL && e != nil:
		expireAt = e.expireAt
	case opt.TTL > 0:
		expireAt = time.Now().Add(opt.TTL)
	}
	c.setLocked(key, v, expireAt)
	res.Stored = true
	return res, nil
}

// IncrBy adds delta to the integer stored at key, creating it at 0 if absent.
// The TTL of an existing key is preserved, as in Redis.
func (c *Cache) IncrBy(key string, delta int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var cur int64
	expireAt := time.Time{}
	if e := c.liveLocked(key); e != nil {
		n, err := strconv.ParseInt(string(e.value), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur, expireAt = n, e.expireAt
	}
	// Check before adding: int64 overflow is UB-ish wraparound otherwise.
	if (delta > 0 && cur > (1<<63-1)-delta) || (delta < 0 && cur < -(1<<63)-delta) {
		return 0, ErrOverflow
	}
	next := cur + delta
	v := strconv.AppendInt(nil, next, 10)
	if err := c.fits(key, v); err != nil {
		return 0, err
	}
	c.setLocked(key, v, expireAt)
	return next, nil
}

// IncrByFloat is INCRBYFLOAT. The formatted result is returned because the
// caller replies with the string form, and re-formatting it elsewhere risks a
// different rendering of the same number.
func (c *Cache) IncrByFloat(key string, delta float64) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var cur float64
	expireAt := time.Time{}
	if e := c.liveLocked(key); e != nil {
		f, err := strconv.ParseFloat(string(e.value), 64)
		if err != nil {
			return "", ErrNotFloat
		}
		cur, expireAt = f, e.expireAt
	}
	next := cur + delta
	if isInfOrNaN(next) {
		return "", ErrNotFloat
	}
	v := []byte(formatFloat(next))
	if err := c.fits(key, v); err != nil {
		return "", err
	}
	c.setLocked(key, v, expireAt)
	return string(v), nil
}

// Append appends to the value at key and returns the new length.
func (c *Cache) Append(key string, tail []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var v []byte
	expireAt := time.Time{}
	if e := c.liveLocked(key); e != nil {
		v = make([]byte, 0, len(e.value)+len(tail))
		v = append(append(v, e.value...), tail...)
		expireAt = e.expireAt
	} else {
		v = append([]byte(nil), tail...)
	}
	if err := c.fits(key, v); err != nil {
		return 0, err
	}
	c.setLocked(key, v, expireAt)
	return len(v), nil
}

// StrLen returns the value length, or 0 when the key is absent.
func (c *Cache) StrLen(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.liveLocked(key); e != nil {
		return len(e.value)
	}
	return 0
}

// GetDel returns the value and removes the key atomically.
func (c *Cache) GetDel(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.liveLocked(key)
	if e == nil {
		c.misses++
		return nil, false
	}
	out := append([]byte(nil), e.value...)
	c.removeLocked(e)
	c.hits++
	return out, true
}

// GetEx reads a key and adjusts its expiry in the same step. persist clears the
// TTL; otherwise ttl > 0 sets one and ttl == 0 leaves the expiry untouched.
func (c *Cache) GetEx(key string, ttl time.Duration, persist bool) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.liveLocked(key)
	if e == nil {
		c.misses++
		return nil, false
	}
	switch {
	case persist:
		e.expireAt = time.Time{}
		c.emit(Mutation{Kind: MutExpire, Key: key})
	case ttl > 0:
		e.expireAt = time.Now().Add(ttl)
		c.emit(Mutation{Kind: MutExpire, Key: key, ExpireAt: e.expireAt})
	}
	c.order.MoveToFront(e.elem)
	c.hits++
	return append([]byte(nil), e.value...), true
}

// ExpireOptions are the Redis 7 EXPIRE modifiers; the zero value always applies.
type ExpireOptions struct {
	NX bool // only when no expiry is set
	XX bool // only when an expiry is set
	GT bool // only when the new expiry is later than the current one
	LT bool // only when it is earlier
}

// Expire sets an absolute expiry. A time in the past deletes the key, matching
// Redis. Returns false when the key is absent or the condition rejected it.
func (c *Cache) Expire(key string, at time.Time, opt ExpireOptions) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.liveLocked(key)
	if e == nil {
		return false
	}
	has := !e.expireAt.IsZero()
	switch {
	case opt.NX && has:
		return false
	case opt.XX && !has:
		return false
	case opt.GT && (!has || !at.After(e.expireAt)):
		return false
	case opt.LT && has && !at.Before(e.expireAt):
		return false
	}
	if !at.After(time.Now()) {
		c.removeLocked(e)
		return true
	}
	e.expireAt = at
	c.emit(Mutation{Kind: MutExpire, Key: key, ExpireAt: at})
	return true
}

// Persist removes an expiry, returning false if there was none.
func (c *Cache) Persist(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.liveLocked(key)
	if e == nil || e.expireAt.IsZero() {
		return false
	}
	e.expireAt = time.Time{}
	c.emit(Mutation{Kind: MutExpire, Key: key})
	return true
}

// TTL reports remaining lifetime. exists is false for an unknown key; hasTTL is
// false for a key stored without expiry (Redis: -1 vs -2).
func (c *Cache) TTL(key string) (d time.Duration, exists, hasTTL bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.liveLocked(key)
	if e == nil {
		return 0, false, false
	}
	if e.expireAt.IsZero() {
		return 0, true, false
	}
	return time.Until(e.expireAt), true, true
}

// Exists reports presence without counting a hit or a miss.
func (c *Cache) Exists(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.liveLocked(key) != nil
}

// Touch marks a key as recently used (Redis TOUCH) and reports whether it existed.
func (c *Cache) Touch(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.liveLocked(key)
	if e == nil {
		return false
	}
	c.order.MoveToFront(e.elem)
	return true
}

// Len is the live item count (DBSIZE).
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// Flush empties the cache (FLUSHDB) and returns how many keys were dropped.
func (c *Cache) Flush() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.order.Len()
	c.items = make(map[string]*entry, 64)
	c.order.Init()
	c.nbytes = 0
	c.invals += uint64(n)
	c.emit(Mutation{Kind: MutFlush})
	return n
}

// Keys returns every live key matching a Redis glob pattern ("*" matches all).
// limit <= 0 means unlimited; the RESP server passes one so a KEYS * on a large
// node cannot allocate an unbounded reply.
func (c *Cache) Keys(pattern string, limit int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	out := make([]string, 0, min(c.order.Len(), 1024))
	for k, e := range c.items {
		if expired(e, now) || !GlobMatch(pattern, k) {
			continue
		}
		out = append(out, k)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// Scan is a cursor walk in insertion order. The cursor is the next sequence
// number to visit, so a key present for the whole scan is returned exactly once;
// keys added during the scan may or may not appear, and a key deleted and
// re-added gets a new sequence number and can appear twice. Those are the same
// guarantees Redis gives. Returns a next cursor of 0 when the walk is complete.
func (c *Cache) Scan(cursor uint64, pattern string, count int) (uint64, []string) {
	if count <= 0 {
		count = 10
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	// Sequence numbers are sparse (deletes leave gaps), so collect the candidate
	// window first, then order it — the map itself has no usable order.
	window := make([]scanItem, 0, c.order.Len())
	for k, e := range c.items {
		if e.seq >= cursor && !expired(e, now) {
			window = append(window, scanItem{seq: e.seq, key: k})
		}
	}
	sort.Slice(window, func(i, j int) bool { return window[i].seq < window[j].seq })

	keys := make([]string, 0, count)
	var next uint64
	for i, it := range window {
		if i >= count {
			next = it.seq // resume here
			break
		}
		if GlobMatch(pattern, it.key) {
			keys = append(keys, it.key)
		}
	}
	return next, keys
}

// Type is Redis TYPE. MoCache stores opaque byte strings only.
func (c *Cache) Type(key string) string {
	if c.Exists(key) {
		return "string"
	}
	return "none"
}

// RandomKey returns an arbitrary live key (Go map order is already randomised).
func (c *Cache) RandomKey() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, e := range c.items {
		if !expired(e, now) {
			return k, true
		}
	}
	return "", false
}

// Export returns a key's value and absolute expiry without counting a hit.
//
// MIGRATE needs both under one lock: read separately, a key could be rewritten
// or expire between the value and its TTL, and it would arrive at its new node
// with a lifetime it never had.
func (c *Cache) Export(key string) (value []byte, expireAt time.Time, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.liveLocked(key)
	if e == nil {
		return nil, time.Time{}, false
	}
	v := make([]byte, len(e.value))
	copy(v, e.value)
	return v, e.expireAt, true
}

// SetAt writes a value with an absolute expiry, as Set does with a relative
// one. It is how a migrated key lands on its new node: converting to a TTL on
// the way in would restart the clock, and the key would outlive the copy it
// replaced by the length of the migration.
func (c *Cache) SetAt(key string, value []byte, expireAt time.Time) error {
	if len(key) > c.maxKey || len(value) > c.maxValue {
		return ErrTooLarge
	}
	if costOf(key, value) > c.maxBytes {
		return ErrTooLarge
	}
	v := make([]byte, len(value))
	copy(v, value)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(key, v, expireAt)
	return nil
}
