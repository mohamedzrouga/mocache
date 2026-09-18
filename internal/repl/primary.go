package repl

import (
	"bufio"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohamedzrouga/mocache/internal/bus"
	"github.com/mohamedzrouga/mocache/internal/cache"
)

// subQueue bounds how far behind a replica may fall before it is dropped and
// made to resynchronise. Blocking instead would let a slow replica stall every
// write on the primary, which is the one thing replication must never do.
const subQueue = 4096

// Primary fans this node's mutations out to subscribed replicas. It implements
// cache.Observer, so attaching it is what turns replication on.
type Primary struct {
	backlog *Backlog

	mu   sync.Mutex
	subs map[*subscriber]struct{}

	connected atomic.Int64
	streamed  atomic.Uint64
	dropped   atomic.Uint64
}

func NewPrimary(backlog *Backlog) *Primary {
	return &Primary{backlog: backlog, subs: make(map[*subscriber]struct{})}
}

// Observe is called by the cache with its lock held: append, fan out, return.
// No I/O happens here.
func (p *Primary) Observe(m cache.Mutation) {
	op := p.backlog.Append(Op{
		Kind:     m.Kind,
		Key:      m.Key,
		Value:    m.Value,
		ExpireMs: expireToMs(m.ExpireAt),
	})
	p.mu.Lock()
	for s := range p.subs {
		s.enqueue(op)
	}
	p.mu.Unlock()
}

// Offset is the primary's current replication offset.
func (p *Primary) Offset() uint64 { return p.backlog.Offset() }

// ReplID identifies this primary's offset space.
func (p *Primary) ReplID() string { return p.backlog.ReplID() }

// Stats for INFO replication and /metrics.
func (p *Primary) Stats() (connected int64, streamed, dropped uint64) {
	return p.connected.Load(), p.streamed.Load(), p.dropped.Load()
}

// Replicas lists each subscriber's acknowledged offset, for INFO and WAIT.
func (p *Primary) Replicas() map[string]uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]uint64, len(p.subs))
	for s := range p.subs {
		out[s.nodeID] = s.acked.Load()
	}
	return out
}

// AckedBy counts replicas that have acknowledged at least offset — the answer
// to WAIT.
func (p *Primary) AckedBy(offset uint64) int {
	n := 0
	for _, acked := range p.Replicas() {
		if acked >= offset {
			n++
		}
	}
	return n
}

// Serve handles an inbound replication subscription; it owns conn from here.
func (p *Primary) Serve(f bus.Frame, conn net.Conn, store *cache.Cache) {
	defer conn.Close()
	var req SubRequest
	if err := f.Decode(&req); err != nil {
		_ = bus.Write(conn, bus.Errorf("bad subscribe: %v", err))
		return
	}

	s := &subscriber{
		nodeID: req.NodeID,
		conn:   conn,
		ch:     make(chan Op, subQueue),
		dirty:  make(map[string]struct{}),
		done:   make(chan struct{}),
		gone:   make(chan struct{}),
	}

	// Decide before registering, but register before snapshotting: from the
	// moment the subscriber is in the map every new op is queued, so nothing
	// can slip through the gap between "snapshot taken" and "stream started".
	ops, canContinue := p.backlog.Since(req.Offset)
	full := !canContinue || req.ReplID != p.backlog.ReplID()

	s.snapshotting = full
	p.mu.Lock()
	p.subs[s] = struct{}{}
	p.mu.Unlock()
	p.connected.Add(1)
	defer func() {
		p.mu.Lock()
		delete(p.subs, s)
		p.mu.Unlock()
		p.connected.Add(-1)
		close(s.done)
	}()

	slog.Info("replica subscribed", "replica", req.NodeID, "full_resync", full, "from_offset", req.Offset)

	start, err := bus.NewFrame(bus.TypeReplStart, StartHeader{
		Full:   full,
		ReplID: p.backlog.ReplID(),
		Offset: p.backlog.Offset(),
	}, nil)
	if err != nil || bus.Write(conn, start) != nil {
		return
	}

	go s.readAcks()

	if full {
		if err := s.sendSnapshot(store); err != nil {
			slog.Warn("snapshot to replica failed", "replica", req.NodeID, "err", err)
			return
		}
	} else {
		// Partial resync: replay the backlog tail before going live.
		for _, op := range ops {
			if err := s.write(op); err != nil {
				return
			}
		}
	}
	end, _ := bus.NewFrame(bus.TypeReplEnd, nil, nil)
	if bus.Write(conn, end) != nil {
		return
	}

	for {
		select {
		case <-s.done:
			return
		case <-s.gone:
			// The socket is gone: either the replica hung up or this node is
			// shutting down. Without this case the loop would sit in the
			// select until the next op or keepalive, holding up Close.
			return
		case op, ok := <-s.ch:
			if !ok {
				return
			}
			if err := s.write(op); err != nil {
				return
			}
			p.streamed.Add(1)
		case <-time.After(5 * time.Second):
			// Idle keepalive: an offset-only op proves the link is alive and
			// lets the replica report a current offset in gossip.
			ping, _ := bus.NewFrame(bus.TypeAck, AckHeader{Offset: p.backlog.Offset()}, nil)
			if bus.Write(conn, ping) != nil {
				return
			}
		}
	}
}

// DropAll disconnects every replica; used when this node stops being a primary.
func (p *Primary) DropAll() {
	p.mu.Lock()
	for s := range p.subs {
		_ = s.conn.Close()
	}
	p.mu.Unlock()
}

type subscriber struct {
	nodeID string
	conn   net.Conn
	ch     chan Op
	acked  atomic.Uint64
	done   chan struct{}
	// gone is closed as soon as the connection is known to be dead, so the
	// streaming loop wakes immediately instead of waiting for its next write.
	gone     chan struct{}
	goneOnce sync.Once

	mu           sync.Mutex
	dirty        map[string]struct{}
	snapshotting bool
	overflowed   bool
}

// kill marks the connection dead exactly once.
func (s *subscriber) kill() {
	s.goneOnce.Do(func() { close(s.gone) })
}

// enqueue is called with the cache lock held: it must never block.
func (s *subscriber) enqueue(op Op) {
	s.mu.Lock()
	if s.snapshotting && op.Key != "" {
		// This key changed after the snapshot began, so the snapshot copy is
		// stale: skip it there and let this op carry the truth.
		s.dirty[op.Key] = struct{}{}
	}
	s.mu.Unlock()

	select {
	case s.ch <- op:
	default:
		// Queue full: the replica is too slow. Close the socket; it will
		// reconnect and full-resync rather than receive a gap in the stream.
		s.mu.Lock()
		s.overflowed = true
		s.mu.Unlock()
		_ = s.conn.Close()
		s.kill()
	}
}

func (s *subscriber) isDirty(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.dirty[key]
	return ok
}

func (s *subscriber) endSnapshot() {
	s.mu.Lock()
	s.snapshotting = false
	s.dirty = nil
	s.mu.Unlock()
}

// sendSnapshot streams the live keyspace. Only key names are copied up front;
// values are fetched one at a time with Peek so a large cache is not duplicated
// in memory, and so snapshotting does not reorder the primary's LRU.
func (s *subscriber) sendSnapshot(store *cache.Cache) error {
	defer s.endSnapshot()
	keys := store.KeySnapshot()
	sent := 0
	for _, k := range keys {
		if s.isDirty(k) {
			continue
		}
		v, exp, ok := store.Peek(k)
		if !ok {
			continue // expired or deleted while we walked; the op stream covers it
		}
		f, err := bus.NewFrame(bus.TypeReplEntry, EntryHeader{Key: k, ExpireMs: expireToMs(exp)}, v)
		if err != nil {
			return err
		}
		_ = s.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err := bus.Write(s.conn, f); err != nil {
			return err
		}
		sent++
	}
	_ = s.conn.SetWriteDeadline(time.Time{})
	slog.Info("snapshot sent", "replica", s.nodeID, "keys", sent)
	return nil
}

func (s *subscriber) write(op Op) error {
	f, err := bus.NewFrame(bus.TypeReplOp, OpHeader{
		Offset:   op.Offset,
		Kind:     byte(op.Kind),
		Key:      op.Key,
		ExpireMs: op.ExpireMs,
	}, op.Value)
	if err != nil {
		return err
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	defer s.conn.SetWriteDeadline(time.Time{})
	return bus.Write(s.conn, f)
}

// readAcks consumes the replica's progress reports on the same socket. It is
// also the connection's liveness watcher: it is the one goroutine blocked in a
// read, so its exit is the earliest possible signal that the socket is dead.
func (s *subscriber) readAcks() {
	defer s.kill()
	r := bufio.NewReaderSize(s.conn, 4<<10)
	for {
		f, err := bus.Read(r)
		if err != nil {
			return
		}
		if f.Type != bus.TypeAck {
			continue
		}
		var ack AckHeader
		if f.Decode(&ack) == nil {
			s.acked.Store(ack.Offset)
		}
	}
}
