// Package repl streams a primary's mutations to its replicas.
//
// Shape of it: the primary observes every change to its LRU (see
// internal/cache/repl.go), stamps it with an offset, and keeps a bounded
// backlog. A replica dials in, gets either a snapshot or the tail of the
// backlog, and then applies a live stream. Replication is asynchronous — a
// promoted replica can be missing the last few writes — which is the right
// trade for a cache and is stated plainly rather than implied.
//
// The backlog is bounded in bytes, like every other buffer in this codebase: a
// replica that cannot keep up is disconnected and resynchronises, rather than
// being allowed to grow the primary's heap until it dies.
package repl

import (
	"sync"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
)

// Op is one replicated mutation with its position in the stream.
//
// Value aliases the slice stored in the cache. That is safe because entries are
// replaced wholesale rather than mutated in place, and it avoids copying every
// written value once per replica.
type Op struct {
	Offset   uint64
	Kind     cache.MutationKind
	Key      string
	Value    []byte
	ExpireMs int64 // absolute unix milliseconds; 0 = no expiry
}

func (o Op) size() int64 { return int64(len(o.Key) + len(o.Value) + 64) }

// Backlog is the primary's ring of recent ops.
type Backlog struct {
	mu       sync.Mutex
	ops      []Op
	bytes    int64
	maxBytes int64
	offset   uint64
	replID   string
}

func NewBacklog(replID string, maxBytes int64) *Backlog {
	if maxBytes <= 0 {
		maxBytes = 32 << 20
	}
	return &Backlog{maxBytes: maxBytes, replID: replID}
}

// Append stamps an op with the next offset and trims the oldest entries.
func (b *Backlog) Append(op Op) Op {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.offset++
	op.Offset = b.offset
	b.ops = append(b.ops, op)
	b.bytes += op.size()
	for b.bytes > b.maxBytes && len(b.ops) > 0 {
		b.bytes -= b.ops[0].size()
		b.ops = b.ops[1:]
	}
	return op
}

// Since returns the ops after offset, and whether the backlog still reaches
// back that far. A false means the replica must take a full snapshot.
func (b *Backlog) Since(offset uint64) ([]Op, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if offset == b.offset {
		return nil, true // already current
	}
	if len(b.ops) == 0 || offset < b.ops[0].Offset-1 || offset > b.offset {
		return nil, false
	}
	out := make([]Op, 0, len(b.ops))
	for _, op := range b.ops {
		if op.Offset > offset {
			out = append(out, op)
		}
	}
	return out, true
}

func (b *Backlog) Offset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.offset
}

func (b *Backlog) ReplID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.replID
}

// Reset starts a new replication history. Called on promotion: the new primary
// has its own offset space, so every replica must resynchronise against it.
func (b *Backlog) Reset(replID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.replID = replID
	b.ops = nil
	b.bytes = 0
	b.offset = 0
}

// --- wire headers ----------------------------------------------------------

// SubRequest is what a replica sends to open a stream.
type SubRequest struct {
	NodeID string `json:"node_id"`
	ReplID string `json:"repl_id"` // empty or mismatched forces a full resync
	Offset uint64 `json:"offset"`
}

// StartHeader answers a SubRequest.
type StartHeader struct {
	Full   bool   `json:"full"`
	ReplID string `json:"repl_id"`
	Offset uint64 `json:"offset"`
}

// EntryHeader carries one snapshot entry; the value is the frame body.
type EntryHeader struct {
	Key      string `json:"k"`
	ExpireMs int64  `json:"x,omitempty"`
}

// OpHeader carries one live mutation; the value is the frame body.
type OpHeader struct {
	Offset   uint64 `json:"o"`
	Kind     byte   `json:"t"`
	Key      string `json:"k,omitempty"`
	ExpireMs int64  `json:"x,omitempty"`
}

// AckHeader is the replica reporting how far it has applied.
type AckHeader struct {
	Offset uint64 `json:"offset"`
}

func expireToMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func msToExpire(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
