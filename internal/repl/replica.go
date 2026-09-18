package repl

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohamedzrouga/mocache/internal/bus"
	"github.com/mohamedzrouga/mocache/internal/cache"
)

// LinkStatus is what INFO replication reports for master_link_status.
type LinkStatus string

const (
	LinkDown LinkStatus = "down"
	LinkSync LinkStatus = "sync" // receiving a snapshot
	LinkUp   LinkStatus = "up"
)

// Replica pulls a primary's stream into the local cache. One goroutine owns the
// connection and is the only writer to the cache on a replica — client writes
// are redirected to the primary before they ever reach the LRU.
type Replica struct {
	store   *cache.Cache
	nodeID  string
	timeout time.Duration

	mu      sync.Mutex
	primary string // bus address; empty stops replication
	cancel  context.CancelFunc

	offset   atomic.Uint64
	replID   atomic.Value // string
	status   atomic.Value // LinkStatus
	synced   atomic.Uint64
	resyncs  atomic.Uint64
	lastSync atomic.Int64 // unix seconds
}

func NewReplica(store *cache.Cache, nodeID string, timeout time.Duration) *Replica {
	r := &Replica{store: store, nodeID: nodeID, timeout: timeout}
	r.status.Store(LinkDown)
	r.replID.Store("")
	return r
}

// Follow points this replica at a primary's bus address, replacing any current
// one. An empty address stops replication (used on promotion).
func (r *Replica) Follow(busAddr string) {
	r.mu.Lock()
	if r.primary == busAddr {
		r.mu.Unlock()
		return
	}
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.primary = busAddr
	if busAddr == "" {
		r.mu.Unlock()
		r.status.Store(LinkDown)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.mu.Unlock()

	slog.Info("following primary", "addr", busAddr)
	go r.loop(ctx, busAddr)
}

// Stop ends replication.
func (r *Replica) Stop() { r.Follow("") }

func (r *Replica) Offset() uint64      { return r.offset.Load() }
func (r *Replica) Status() LinkStatus  { return r.status.Load().(LinkStatus) }
func (r *Replica) ReplID() string      { return r.replID.Load().(string) }
func (r *Replica) Resyncs() uint64     { return r.resyncs.Load() }
func (r *Replica) SyncedKeys() uint64  { return r.synced.Load() }
func (r *Replica) LastSyncUnix() int64 { return r.lastSync.Load() }

func (r *Replica) PrimaryAddr() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.primary
}

// loop reconnects until cancelled. A primary that is down is not an error here:
// failure detection lives in the gossip layer, and this loop's job is only to
// be connected whenever that is possible.
func (r *Replica) loop(ctx context.Context, addr string) {
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		err := r.session(ctx, addr)
		if ctx.Err() != nil {
			return
		}
		r.status.Store(LinkDown)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("replication link lost", "primary", addr, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (r *Replica) session(ctx context.Context, addr string) error {
	conn, err := net.DialTimeout("tcp", addr, r.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	sub, err := bus.NewFrame(bus.TypeReplSub, SubRequest{
		NodeID: r.nodeID,
		ReplID: r.ReplID(),
		Offset: r.offset.Load(),
	}, nil)
	if err != nil {
		return err
	}
	if err := bus.Write(conn, sub); err != nil {
		return err
	}

	br := bufio.NewReaderSize(conn, 64<<10)
	first, err := bus.Read(br)
	if err != nil {
		return err
	}
	if err := first.Err(); err != nil {
		return err
	}
	if first.Type != bus.TypeReplStart {
		return errors.New("repl: unexpected first frame")
	}
	var start StartHeader
	if err := first.Decode(&start); err != nil {
		return err
	}

	if start.Full {
		// The local copy may hold keys the primary no longer has; a full resync
		// replaces the keyspace rather than merging into it.
		r.resyncs.Add(1)
		r.status.Store(LinkSync)
		_ = r.store.ApplyReplicated(cache.Mutation{Kind: cache.MutFlush})
		r.offset.Store(0)
	}
	r.replID.Store(start.ReplID)

	acks := time.NewTicker(500 * time.Millisecond)
	defer acks.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-acks.C:
				f, _ := bus.NewFrame(bus.TypeAck, AckHeader{Offset: r.offset.Load()}, nil)
				if bus.Write(conn, f) != nil {
					return
				}
			}
		}
	}()

	for {
		f, err := bus.Read(br)
		if err != nil {
			return err
		}
		switch f.Type {
		case bus.TypeReplEntry:
			var h EntryHeader
			if err := f.Decode(&h); err != nil {
				return err
			}
			_ = r.store.ApplyReplicated(cache.Mutation{
				Kind:     cache.MutSet,
				Key:      h.Key,
				Value:    f.Body,
				ExpireAt: msToExpire(h.ExpireMs),
			})
			r.synced.Add(1)
		case bus.TypeReplEnd:
			r.offset.Store(start.Offset)
			r.status.Store(LinkUp)
			r.lastSync.Store(time.Now().Unix())
			slog.Info("replication in sync", "primary", addr, "offset", start.Offset)
		case bus.TypeReplOp:
			var h OpHeader
			if err := f.Decode(&h); err != nil {
				return err
			}
			_ = r.store.ApplyReplicated(cache.Mutation{
				Kind:     cache.MutationKind(h.Kind),
				Key:      h.Key,
				Value:    f.Body,
				ExpireAt: msToExpire(h.ExpireMs),
			})
			r.offset.Store(h.Offset)
		case bus.TypeAck:
			// Keepalive from the primary; nothing to apply.
		case bus.TypeError:
			return f.Err()
		}
	}
}
