// Mutation stream for replication.
//
// A primary must be able to tell a replica what changed, in the order it
// changed. Every write path in this package funnels through setLocked and
// removeLocked, so emitting from those two places (plus Flush) captures the
// full sequence — including evictions and lazy expiry, which a replica has to
// see or it will keep serving entries the primary has already dropped.
//
// Observe is called while c.mu is held. That is deliberate: it is what makes
// the emitted order identical to the applied order. The observer must
// therefore be fast and must never call back into the cache — internal/repl's
// implementation appends to a ring buffer and does non-blocking channel sends.
package cache

import "time"

// MutationKind is the operation a replica must replay.
type MutationKind byte

const (
	MutSet    MutationKind = 1 // value (re)written; ExpireAt is absolute
	MutDelete MutationKind = 2 // removed: explicit delete, eviction, or expiry
	MutExpire MutationKind = 3 // expiry changed, value untouched; zero = persist
	MutFlush  MutationKind = 4 // whole keyspace dropped
)

func (k MutationKind) String() string {
	switch k {
	case MutSet:
		return "set"
	case MutDelete:
		return "del"
	case MutExpire:
		return "expire"
	case MutFlush:
		return "flush"
	}
	return "unknown"
}

// Mutation is one replicated change.
//
// Expiry travels as an absolute instant, never as a TTL: a relative duration
// would restart its clock on arrival and the key would outlive the primary's
// copy by the replication lag on every hop.
type Mutation struct {
	Kind     MutationKind
	Key      string
	Value    []byte
	ExpireAt time.Time
}

// Observer receives mutations in apply order.
type Observer interface {
	Observe(Mutation)
}

// SetObserver attaches the replication backlog. Passing nil detaches it.
func (c *Cache) SetObserver(o Observer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observer = o
}

// emit publishes a mutation. Caller holds c.mu.
func (c *Cache) emit(m Mutation) {
	// Replayed writes must not be re-emitted: the replica's own backlog would
	// fill with its primary's history under different offsets.
	if c.observer == nil || c.applying {
		return
	}
	c.observer.Observe(m)
}

// ApplyReplicated replays a mutation from a primary. It is the only write path
// a replica's applier uses, and the only one that does not emit.
func (c *Cache) ApplyReplicated(m Mutation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applying = true
	defer func() { c.applying = false }()

	switch m.Kind {
	case MutSet:
		if err := c.fits(m.Key, m.Value); err != nil {
			// The replica may be configured smaller than its primary. Dropping
			// the key is the honest outcome: a cache miss, not a silent
			// truncation, and the same thing the LRU would do under pressure.
			return err
		}
		v := make([]byte, len(m.Value))
		copy(v, m.Value)
		c.setLocked(m.Key, v, m.ExpireAt)
	case MutDelete:
		if e, ok := c.items[m.Key]; ok {
			c.removeLocked(e)
		}
	case MutExpire:
		if e, ok := c.items[m.Key]; ok {
			e.expireAt = m.ExpireAt
		}
	case MutFlush:
		c.items = make(map[string]*entry, 64)
		c.order.Init()
		c.nbytes = 0
	}
	return nil
}

// Peek reads an entry without touching LRU order or hit/miss counters. The
// replication snapshot uses it so that streaming a node's contents does not
// reorder its eviction queue.
func (c *Cache) Peek(key string) (value []byte, expireAt time.Time, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, present := c.items[key]
	if !present || expired(e, time.Now()) {
		return nil, time.Time{}, false
	}
	return append([]byte(nil), e.value...), e.expireAt, true
}

// KeySnapshot returns the live key names. Only names: a full-value copy of a
// large cache would double its memory at exactly the wrong moment. The
// snapshot sender walks these and Peeks each one.
func (c *Cache) KeySnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	out := make([]string, 0, c.order.Len())
	for k, e := range c.items {
		if !expired(e, now) {
			out = append(out, k)
		}
	}
	return out
}

// replSuppressed reports whether this cache is currently replaying; used only
// by tests.
func (c *Cache) replSuppressed() bool { return c.applying }
