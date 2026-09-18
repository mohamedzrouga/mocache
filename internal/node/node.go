// Package node is the clustering runtime: it wires the cluster bus, the
// replication engine and the cluster state machine together and drives them on
// a timer.
//
// Division of labour:
//
//	internal/bus      how nodes talk
//	internal/repl     what a primary sends and a replica applies
//	internal/cluster  who owns which slot, and the rules for changing that
//	internal/node     the loops that make those three do something
//
// The rules that matter for correctness live in internal/cluster and can be
// tested without a network; what is here is scheduling and I/O.
package node

import (
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/mohamedzrouga/mocache/internal/bus"
	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/cluster"
	"github.com/mohamedzrouga/mocache/internal/repl"
)

// Options tune the failure detector and replication.
type Options struct {
	// BusAddr is where this node listens for peers. Empty derives it from the
	// announce address as client port + 10000, which is what CLUSTER NODES
	// already advertises.
	BusAddr string
	// NodeTimeout is how long a peer may be silent before it is suspected.
	// Every other timing is derived from it, as in Redis.
	NodeTimeout time.Duration
	// PingInterval defaults to NodeTimeout/4.
	PingInterval time.Duration
	// FailoverDelay is the base wait before a replica stands for election.
	FailoverDelay time.Duration
	// ReplBacklogBytes bounds the primary's replication backlog.
	ReplBacklogBytes int64
	// DialTimeout bounds one bus round trip.
	DialTimeout time.Duration
}

func (o *Options) setDefaults() {
	if o.NodeTimeout <= 0 {
		o.NodeTimeout = 5 * time.Second
	}
	if o.PingInterval <= 0 {
		o.PingInterval = o.NodeTimeout / 4
	}
	if o.PingInterval < 50*time.Millisecond {
		o.PingInterval = 50 * time.Millisecond
	}
	if o.FailoverDelay <= 0 {
		o.FailoverDelay = 500 * time.Millisecond
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = o.NodeTimeout
	}
	if o.ReplBacklogBytes <= 0 {
		o.ReplBacklogBytes = 32 << 20
	}
}

// Cluster is the running clustering layer for one node.
type Cluster struct {
	mgr   *cluster.Manager
	store *cache.Cache
	opt   Options

	backlog *repl.Backlog
	primary *repl.Primary
	replica *repl.Replica

	srv *bus.Server
	ln  net.Listener

	mu       sync.Mutex
	links    map[string]*bus.Link
	electing bool
	closed   bool
	stop     chan struct{}
	wg       sync.WaitGroup
}

func New(mgr *cluster.Manager, store *cache.Cache, opt Options) *Cluster {
	opt.setDefaults()
	me := mgr.View().Myself()
	backlog := repl.NewBacklog(cluster.NewReplID(), opt.ReplBacklogBytes)
	c := &Cluster{
		mgr:     mgr,
		store:   store,
		opt:     opt,
		backlog: backlog,
		primary: repl.NewPrimary(backlog),
		replica: repl.NewReplica(store, me.ID, opt.DialTimeout),
		links:   make(map[string]*bus.Link),
		stop:    make(chan struct{}),
	}
	c.srv = bus.NewServer(c)
	// Attaching the observer is what turns replication on. It stays attached on
	// a replica too: a replica that gets promoted needs a backlog immediately,
	// and replayed writes are suppressed at the source (cache.ApplyReplicated).
	store.SetObserver(c.primary)
	return c
}

// BusAddr is the address this node listens on for peers.
func (c *Cluster) BusAddr() string {
	if c.opt.BusAddr != "" {
		return c.opt.BusAddr
	}
	me := c.mgr.View().Myself()
	return net.JoinHostPort("", itoa(me.BusPort()))
}

// Start binds the bus listener and begins gossiping.
func (c *Cluster) Start() error {
	ln, err := net.Listen("tcp", c.BusAddr())
	if err != nil {
		return err
	}
	c.ln = ln
	go func() {
		if err := c.srv.Serve(ln); err != nil {
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()
			if !closed {
				slog.Error("cluster bus stopped", "err", err)
			}
		}
	}()
	slog.Info("cluster bus listening", "addr", ln.Addr().String(), "node_timeout", c.opt.NodeTimeout.String())

	c.reconcileRole()
	c.wg.Add(1)
	go c.loop()
	return nil
}

func (c *Cluster) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.stop)
	links := c.links
	c.links = make(map[string]*bus.Link)
	c.mu.Unlock()

	c.replica.Stop()
	c.primary.DropAll()
	for _, l := range links {
		l.Close()
	}
	if c.ln != nil {
		_ = c.ln.Close()
	}
	c.srv.Close()
	c.wg.Wait()
	c.store.SetObserver(nil)
}

// Primary exposes the replication source (INFO, WAIT, metrics).
func (c *Cluster) Primary() *repl.Primary { return c.primary }

// Replica exposes the replication sink.
func (c *Cluster) Replica() *repl.Replica { return c.replica }

// loop is the cluster cron: gossip, then act on what it learned.
func (c *Cluster) loop() {
	defer c.wg.Done()
	t := time.NewTicker(c.opt.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.pingRound()
			c.detectFailures()
			c.reconcileRole()
			c.maybeFailover()
		}
	}
}

// pingRound exchanges gossip with every peer. Each node pings independently, so
// a pair of nodes has two links and neither has to be "the" initiator.
func (c *Cluster) pingRound() {
	view := c.mgr.View()
	me := view.Myself()
	c.mgr.SetReplOffset(c.myOffset())

	header := PingHeader{From: me.ID, Epoch: c.mgr.CurrentEpoch(), Nodes: c.mgr.Gossip()}
	frame, err := bus.NewFrame(bus.TypePing, header, nil)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, peer := range view.Nodes() {
		if peer.ID == me.ID {
			continue
		}
		wg.Add(1)
		go func(peer *cluster.Node) {
			defer wg.Done()
			reply, err := c.link(peer.BusAddr()).Do(frame)
			if err != nil {
				c.mgr.NoteUnreachable(peer.ID, c.opt.NodeTimeout)
				return
			}
			var pong PingHeader
			if reply.Type != bus.TypePong || reply.Decode(&pong) != nil {
				c.mgr.NoteUnreachable(peer.ID, c.opt.NodeTimeout)
				return
			}
			c.mgr.NoteContact(peer.ID, pong.ReplOffset, pong.ConfigEpoch)
			c.mgr.MergeGossip(pong.From, pong.Epoch, pong.Nodes)
		}(peer)
	}
	wg.Wait()
}

// detectFailures promotes local suspicion to a cluster-wide decision once a
// majority of primaries agree, and tells everyone.
func (c *Cluster) detectFailures() {
	view := c.mgr.View()
	me := view.Myself()
	for _, n := range view.Nodes() {
		if n.ID == me.ID || n.Fail {
			continue
		}
		if !c.mgr.ShouldFail(n.ID, c.opt.NodeTimeout*2) {
			continue
		}
		if c.mgr.MarkFail(n.ID) {
			slog.Warn("node marked failed", "node", n.Addr(), "node_id", n.ID)
			c.broadcast(bus.TypeFail, FailHeader{NodeID: n.ID, From: me.ID})
		}
	}
}

// reconcileRole makes the replication wiring match the current topology: a
// replica follows its primary, a primary follows nobody.
func (c *Cluster) reconcileRole() {
	if addr := c.mgr.PrimaryBusAddr(); addr != "" {
		if c.replica.PrimaryAddr() != addr {
			// We have just become a replica (or changed primary). Drop anyone
			// streaming from us: our data is about to be replaced wholesale.
			c.primary.DropAll()
		}
		c.replica.Follow(addr)
		return
	}
	if c.replica.PrimaryAddr() != "" {
		c.replica.Stop()
	}
}

func (c *Cluster) myOffset() uint64 {
	if c.mgr.View().Myself().Role == cluster.RoleReplica {
		return c.replica.Offset()
	}
	return c.primary.Offset()
}

func (c *Cluster) link(addr string) *bus.Link {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.links[addr]
	if !ok {
		l = bus.NewLink(addr, c.opt.DialTimeout)
		c.links[addr] = l
	}
	return l
}

func (c *Cluster) broadcast(t bus.Type, header any) {
	f, err := bus.NewFrame(t, header, nil)
	if err != nil {
		return
	}
	view := c.mgr.View()
	me := view.Myself()
	for _, n := range view.Nodes() {
		if n.ID == me.ID {
			continue
		}
		go func(addr string) { _, _ = c.link(addr).Do(f) }(n.BusAddr())
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

var jitter = rand.New(rand.NewSource(time.Now().UnixNano()))
var jitterMu sync.Mutex

func randDelay(max time.Duration) time.Duration {
	jitterMu.Lock()
	defer jitterMu.Unlock()
	return time.Duration(jitter.Int63n(int64(max)))
}
