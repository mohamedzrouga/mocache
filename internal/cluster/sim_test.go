package cluster

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

// A deterministic discrete-event simulation of a cluster under an adversarial
// network.
//
// internal/node/cluster_test.go runs real nodes over loopback. That proves the
// wiring works, but it can only stop a node: it cannot delay one ping past
// another, cut the cluster in half, skew a clock, or replay the schedule that
// produced a failure. Those are exactly the conditions under which two nodes
// come to own one slot, so they are the conditions the voting rules need to be
// tested under.
//
// Here there is no network and no wall clock. Every node's *Manager is the real
// one; what is modelled is the runtime around it — the cron loop, the gossip
// round trip, rank-delayed elections, FAIL and UPDATE broadcasts, crashes and
// restarts — driven from one seeded event queue on one goroutine. A run is a
// pure function of its seed, so a failing seed can be replayed.
//
// The model mirrors internal/node's loop (pingRound, detectFailures,
// maybeFailover, runElection) and internal/node's Serve. It is a model of that
// code, not that code: changing the real loop means changing this one, or the
// simulation quietly starts proving something about a runtime we no longer run.
// What is deliberately not modelled is the bus framing and the replication
// stream — those carry no ownership decisions.

// --- clock -----------------------------------------------------------------

// newSimManager builds a Manager on simulated time. Rewiring the clock after
// construction also has to reset lastPong, which NewManager stamped with the
// real one.
func newSimManager(t *testing.T, peers []string, announce string, clock func() time.Time) *Manager {
	t.Helper()
	m, err := NewManager(peers, announce)
	if err != nil {
		t.Fatalf("manager for %s: %v", announce, err)
	}
	m.mu.Lock()
	m.clock = clock
	now := clock()
	for _, n := range m.nodes {
		n.lastPong = now
	}
	m.mu.Unlock()
	return m
}

// --- event queue -----------------------------------------------------------

type event struct {
	at  time.Time
	seq uint64
	fn  func()
}

// Ties break on seq, the order events were scheduled in, so the queue is a
// total order rather than a set the heap may permute differently next run.
type eventQueue []*event

func (q eventQueue) Len() int      { return len(q) }
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at.Equal(q[j].at) {
		return q[i].seq < q[j].seq
	}
	return q[i].at.Before(q[j].at)
}
func (q *eventQueue) Push(x any) { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

// --- options ---------------------------------------------------------------

type simOptions struct {
	peers []string

	nodeTimeout   time.Duration
	pingInterval  time.Duration
	failoverDelay time.Duration
	dialTimeout   time.Duration

	minLatency time.Duration
	maxLatency time.Duration
	loss       float64       // probability one message is dropped outright
	maxSkew    time.Duration // per-node clock offset, drawn once at startup
}

func (o *simOptions) setDefaults() {
	if o.nodeTimeout <= 0 {
		o.nodeTimeout = 2 * time.Second
	}
	if o.pingInterval <= 0 {
		o.pingInterval = o.nodeTimeout / 4
	}
	if o.failoverDelay <= 0 {
		o.failoverDelay = 200 * time.Millisecond
	}
	if o.dialTimeout <= 0 {
		o.dialTimeout = o.nodeTimeout
	}
	if o.maxLatency <= 0 {
		o.maxLatency = 20 * time.Millisecond
	}
	if o.minLatency <= 0 {
		o.minLatency = time.Millisecond
	}
}

// seeds is how many schedules a scenario explores. Short mode takes a sample —
// enough to catch a regression on a laptop, while the full sweep is what runs
// under -race in CI.
func seeds(n int) int64 {
	if testing.Short() && n > 4 {
		return 4
	}
	return int64(n)
}

// threeShards is the smallest configuration that can fail over at all: three
// primaries, so a majority exists, each with one replica.
func threeShards() []string {
	return []string{
		"10.0.0.1:6379=0-5460",
		"10.0.0.2:6379=5461-10922",
		"10.0.0.3:6379=10923-16383",
		"10.0.0.4:6379=replica-of:10.0.0.1:6379",
		"10.0.0.5:6379=replica-of:10.0.0.2:6379",
		"10.0.0.6:6379=replica-of:10.0.0.3:6379",
	}
}

// --- messages --------------------------------------------------------------

type msgKind int

const (
	msgPing msgKind = iota
	msgVoteReq
	msgFail
	msgUpdate
)

// ping mirrors node.PingHeader; pong is the same shape, as on the wire.
type ping struct {
	from       string
	epoch      uint64
	replOffset uint64
	nodes      []GossipNode
}

type update struct {
	nodeID string
	epoch  uint64
}

// --- nodes -----------------------------------------------------------------

type simNode struct {
	idx     int
	id      string
	addr    string
	mgr     *Manager
	sim     *sim
	up      bool
	skew    time.Duration
	offset  uint64 // replication offset, the input to failover ranking
	lag     uint64 // how far behind its primary this replica runs
	gen     uint64 // bumped on crash and restart; stale callbacks check it
	electng bool
}

func (n *simNode) now() time.Time { return n.sim.now.Add(n.skew) }

// tick is internal/node's cron: gossip, then act on what it learned. The body
// blocks on the ping round there, so the next tick is the first one due after
// this one finishes, exactly as a time.Ticker with a slow consumer behaves.
func (n *simNode) tick() {
	gen := n.gen
	n.advanceOffset()
	n.mgr.SetReplOffset(n.offset)
	n.pingRound(func() {
		if n.gen != gen || !n.up {
			return
		}
		n.detectFailures()
		n.maybeFailover()
		n.sim.scheduleTick(n)
	})
}

// advanceOffset models replication progress. A primary writes; a replica
// follows its primary at a fixed lag and stops when it cannot reach it. The
// absolute numbers are meaningless — the ordering between siblings is the point,
// because that is what ranks failover candidates.
func (n *simNode) advanceOffset() {
	me := n.mgr.View().Myself()
	if me.Role == RolePrimary && len(me.Slots) > 0 {
		n.offset += 1 + uint64(n.sim.rnd.Intn(64))
		return
	}
	p := n.sim.byID[me.PrimaryO]
	if p == nil || !p.up || !n.sim.linked(n, p) {
		return
	}
	if p.offset > n.lag && p.offset-n.lag > n.offset {
		n.offset = p.offset - n.lag
	}
}

// pingRound exchanges gossip with every peer and calls done once every exchange
// has either answered or timed out — the wg.Wait() in internal/node.
func (n *simNode) pingRound(done func()) {
	view := n.mgr.View()
	me := view.Myself()
	// The frame is built once, before the round: every peer is told the same
	// thing, as in internal/node.
	out := ping{from: me.ID, epoch: n.mgr.CurrentEpoch(), replOffset: n.offset, nodes: n.mgr.Gossip()}

	peers := make([]*simNode, 0, len(view.Nodes()))
	for _, p := range view.Nodes() {
		if p.ID != me.ID {
			if sn := n.sim.byID[p.ID]; sn != nil {
				peers = append(peers, sn)
			}
		}
	}
	if len(peers) == 0 {
		done()
		return
	}

	outstanding := len(peers)
	settle := func() {
		outstanding--
		if outstanding == 0 {
			done()
		}
	}
	for _, p := range peers {
		peer := p
		n.sim.rpc(n, peer, msgPing, out,
			func(reply any) {
				pong := reply.(ping)
				n.mgr.NoteContact(pong.from, pong.replOffset, 0)
				n.mgr.MergeGossip(pong.from, pong.epoch, pong.nodes)
				settle()
			},
			func() {
				n.mgr.NoteUnreachable(peer.id, n.sim.opt.nodeTimeout)
				settle()
			})
	}
}

// detectFailures turns local suspicion into a cluster-wide decision once a
// majority of primaries agree, and tells everyone.
func (n *simNode) detectFailures() {
	view := n.mgr.View()
	me := view.Myself()
	for _, p := range view.Nodes() {
		if p.ID == me.ID || p.Fail {
			continue
		}
		if !n.mgr.ShouldFail(p.ID, n.sim.opt.nodeTimeout*2) {
			continue
		}
		if n.mgr.MarkFail(p.ID) {
			n.sim.logf("%s marks %s FAIL", n.short(), n.sim.shortID(p.ID))
			n.broadcast(msgFail, p.ID)
		}
	}
}

// maybeFailover stands for election when our primary has been declared failed.
// The rank-based delay is what usually leaves the most up-to-date replica
// standing alone, so the vote is not split.
func (n *simNode) maybeFailover() {
	primaryID, rank, ok := n.mgr.FailoverCandidacy(n.offset)
	if !ok || n.electng {
		return
	}
	n.electng = true
	gen := n.gen
	delay := n.sim.opt.failoverDelay +
		time.Duration(rank)*n.sim.opt.failoverDelay*2 +
		time.Duration(n.sim.rnd.Int63n(int64(n.sim.opt.failoverDelay)))

	n.sim.after(delay, func() {
		if n.gen != gen || !n.up {
			return
		}
		// Another replica may have been promoted while we waited.
		if _, _, still := n.mgr.FailoverCandidacy(n.offset); !still {
			n.electng = false
			return
		}
		n.runElection(primaryID, false, gen)
	})
}

// runElection asks every slot-owning primary for a vote and promotes on a
// majority. It resolves when every request has answered or timed out.
func (n *simNode) runElection(primaryID string, force bool, gen uint64) {
	view := n.mgr.View()
	me := view.Myself()
	epoch := n.mgr.NextEpoch()
	req := VoteRequest{
		Epoch: epoch, NodeID: me.ID, PrimaryID: primaryID,
		ReplOffset: n.offset, Force: force,
	}

	voters := 0
	for _, p := range view.Nodes() {
		if p.Role == RolePrimary && len(p.Slots) > 0 {
			voters++
		}
	}
	needed := voters/2 + 1

	var targets []*simNode
	for _, p := range view.Nodes() {
		if p.ID == me.ID || p.ID == primaryID || p.Role != RolePrimary || len(p.Slots) == 0 {
			continue
		}
		if sn := n.sim.byID[p.ID]; sn != nil {
			targets = append(targets, sn)
		}
	}

	n.sim.logf("%s stands for election epoch=%d needed=%d asks=%d", n.short(), epoch, needed, len(targets))

	granted := 0
	outstanding := len(targets)
	finish := func() {
		n.electng = false
		if n.gen != gen || !n.up {
			return
		}
		if granted < needed {
			n.sim.logf("%s lost election epoch=%d granted=%d", n.short(), epoch, granted)
			return
		}
		n.promoteSelf(epoch)
	}
	if outstanding == 0 {
		finish()
		return
	}
	settle := func() {
		outstanding--
		if outstanding == 0 {
			finish()
		}
	}
	for _, target := range targets {
		n.sim.rpc(n, target, msgVoteReq, req,
			func(reply any) {
				vote := reply.(VoteReply)
				if vote.Granted && vote.Epoch == epoch {
					granted++
				}
				settle()
			},
			settle)
	}
}

func (n *simNode) promoteSelf(epoch uint64) {
	me := n.mgr.View().Myself()
	if !n.mgr.Promote(me.ID, epoch) {
		n.sim.logf("%s promotion rejected epoch=%d (stale)", n.short(), epoch)
		return
	}
	n.sim.recordPromotion(n, epoch)
	n.sim.logf("%s PROMOTED epoch=%d slots=%d", n.short(), epoch, n.mgr.View().Myself().SlotsCount())
	n.broadcast(msgUpdate, update{nodeID: me.ID, epoch: epoch})
}

// broadcast is fire and forget: internal/node sends these and ignores the ack.
func (n *simNode) broadcast(kind msgKind, payload any) {
	view := n.mgr.View()
	me := view.Myself()
	for _, p := range view.Nodes() {
		if p.ID == me.ID {
			continue
		}
		if sn := n.sim.byID[p.ID]; sn != nil {
			n.sim.send(n, sn, kind, payload)
		}
	}
}

// serve mirrors internal/node's Serve. It returns the reply and whether there
// is one; FAIL and UPDATE are acked on the wire but the ack carries nothing.
func (n *simNode) serve(kind msgKind, payload any) (any, bool) {
	switch kind {
	case msgPing:
		p := payload.(ping)
		n.mgr.NoteContact(p.from, p.replOffset, 0)
		n.mgr.MergeGossip(p.from, p.epoch, p.nodes)
		n.mgr.SetReplOffset(n.offset)
		me := n.mgr.View().Myself()
		return ping{
			from: me.ID, epoch: n.mgr.CurrentEpoch(),
			replOffset: n.offset, nodes: n.mgr.Gossip(),
		}, true

	case msgVoteReq:
		req := payload.(VoteRequest)
		reply := n.mgr.ConsiderVote(req)
		if reply.Granted {
			n.sim.recordVote(n, req)
		}
		return reply, true

	case msgFail:
		n.mgr.MarkFail(payload.(string))
		return nil, false

	case msgUpdate:
		u := payload.(update)
		if n.mgr.Promote(u.nodeID, u.epoch) {
			n.sim.recordPromotion(n.sim.byID[u.nodeID], u.epoch)
		}
		return nil, false
	}
	panic("unknown message")
}

func (n *simNode) short() string { return n.addr[:strings.IndexByte(n.addr, ':')] }

// --- the simulation --------------------------------------------------------

type sim struct {
	t     *testing.T
	seed  int64
	opt   simOptions
	rnd   *rand.Rand
	now   time.Time
	start time.Time
	queue eventQueue
	seq   uint64

	nodes []*simNode
	byID  map[string]*simNode // keyed by node ID *and* announce address

	// part[i] is node i's side of the network; nodes on different sides cannot
	// exchange messages.
	part []int

	trace   []string
	votes   map[string]string // voter|epoch -> candidate
	promos  map[uint64]string // config epoch -> the node that took it
	lastOwn []map[string]uint64
	failed  bool

	// faultsUntil is when the last scheduled scenario step fires. Settling is
	// only meaningful after them: a cluster is trivially "converged" in the
	// moment between a node dying and anyone noticing.
	faultsUntil time.Duration
}

func newSim(t *testing.T, seed int64, opt simOptions) *sim {
	t.Helper()
	opt.setDefaults()
	if len(opt.peers) == 0 {
		opt.peers = threeShards()
	}
	s := &sim{
		t:      t,
		seed:   seed,
		opt:    opt,
		rnd:    rand.New(rand.NewSource(seed)),
		now:    time.Unix(1700000000, 0),
		byID:   make(map[string]*simNode),
		votes:  make(map[string]string),
		promos: make(map[uint64]string),
	}
	s.start = s.now

	for i, spec := range opt.peers {
		addr, _, _ := strings.Cut(spec, "=")
		addr = strings.TrimSpace(addr)
		n := &simNode{idx: i, addr: addr, id: NodeID(addr), sim: s, up: true}
		if opt.maxSkew > 0 {
			// Signed offset: half the nodes run early, half late.
			n.skew = time.Duration(s.rnd.Int63n(int64(2*opt.maxSkew))) - opt.maxSkew
		}
		n.lag = uint64(s.rnd.Intn(256))
		n.mgr = newSimManager(t, opt.peers, addr, n.now)
		s.nodes = append(s.nodes, n)
		s.byID[n.id] = n
		s.byID[addr] = n
	}
	s.part = make([]int, len(s.nodes))
	s.lastOwn = make([]map[string]uint64, len(s.nodes))
	for i := range s.lastOwn {
		s.lastOwn[i] = make(map[string]uint64)
	}

	// Stagger the first tick: real nodes are not synchronised, and a lockstep
	// cron would hide every ordering bug that depends on them not being.
	for _, n := range s.nodes {
		s.after(time.Duration(s.rnd.Int63n(int64(s.opt.pingInterval))), n.tick)
	}
	return s
}

// --- scheduling ------------------------------------------------------------

func (s *sim) after(d time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.queue, &event{at: s.now.Add(d), seq: s.seq, fn: fn})
}

// at schedules a scenario step relative to the start of the run.
func (s *sim) at(d time.Duration, fn func()) {
	if d > s.faultsUntil {
		s.faultsUntil = d
	}
	s.seq++
	heap.Push(&s.queue, &event{at: s.start.Add(d), seq: s.seq, fn: fn})
}

func (s *sim) elapsed() time.Duration { return s.now.Sub(s.start) }

func (s *sim) scheduleTick(n *simNode) {
	gen := n.gen
	// The first tick boundary strictly after now, so a round that overran its
	// interval does not fire a burst of catch-up ticks.
	elapsed := s.now.Sub(s.start)
	next := (elapsed/s.opt.pingInterval + 1) * s.opt.pingInterval
	s.after(next-elapsed, func() {
		if n.gen == gen && n.up {
			n.tick()
		}
	})
}

// run drains the queue up to the deadline, checking the safety invariants after
// every single event — a violation that exists for one event is a violation.
func (s *sim) run(d time.Duration) {
	deadline := s.start.Add(d)
	for s.queue.Len() > 0 && !s.failed {
		ev := heap.Pop(&s.queue).(*event)
		if ev.at.After(deadline) {
			heap.Push(&s.queue, ev)
			break
		}
		s.now = ev.at
		ev.fn()
		s.checkSafety()
	}
	s.now = deadline
}

// --- network ---------------------------------------------------------------

func (s *sim) linked(a, b *simNode) bool { return s.part[a.idx] == s.part[b.idx] }

func (s *sim) latency() time.Duration {
	spread := s.opt.maxLatency - s.opt.minLatency
	if spread <= 0 {
		return s.opt.minLatency
	}
	return s.opt.minLatency + time.Duration(s.rnd.Int63n(int64(spread)))
}

func (s *sim) drops() bool { return s.opt.loss > 0 && s.rnd.Float64() < s.opt.loss }

// rpc models one bus round trip. Latency is drawn per message, so replies
// overtake each other and a round's answers arrive in an order unrelated to the
// order it asked in. Reachability is evaluated at delivery, which is what lets a
// partition form or heal while a message is in flight. The caller sees exactly
// the two outcomes bus.Link.Do gives: a reply, or a timeout.
func (s *sim) rpc(from, to *simNode, kind msgKind, payload any, onReply func(any), onFail func()) {
	done := false
	fromGen, toGen := from.gen, to.gen

	s.after(s.opt.dialTimeout, func() {
		if done {
			return
		}
		done = true
		if from.up && from.gen == fromGen {
			onFail()
		}
	})

	if s.drops() {
		return
	}
	s.after(s.latency(), func() {
		if done || !to.up || to.gen != toGen || !s.linked(from, to) {
			return
		}
		reply, hasReply := to.serve(kind, payload)
		if !hasReply || s.drops() {
			return
		}
		s.after(s.latency(), func() {
			if done || !from.up || from.gen != fromGen || !s.linked(from, to) {
				return
			}
			done = true
			onReply(reply)
		})
	})
}

// send is a one-way message: the sender never learns whether it arrived.
func (s *sim) send(from, to *simNode, kind msgKind, payload any) {
	toGen := to.gen
	if s.drops() {
		return
	}
	s.after(s.latency(), func() {
		if !to.up || to.gen != toGen || !s.linked(from, to) {
			return
		}
		to.serve(kind, payload)
	})
}

// --- faults ----------------------------------------------------------------

// partition splits the cluster into sides named by address prefix. Every node
// not named stays on side 0.
func (s *sim) partition(sides ...[]string) {
	for i := range s.part {
		s.part[i] = 0
	}
	for side, members := range sides {
		for _, addr := range members {
			n := s.byID[addr]
			if n == nil {
				s.t.Fatalf("partition: unknown node %q", addr)
			}
			s.part[n.idx] = side + 1
		}
	}
	s.logf("PARTITION %v", sides)
}

func (s *sim) heal() {
	for i := range s.part {
		s.part[i] = 0
	}
	s.logf("HEAL")
}

// crash stops a node dead: it sends nothing, answers nothing, and every
// callback it was waiting on is abandoned.
func (s *sim) crash(addr string) {
	n := s.byID[addr]
	n.up = false
	n.gen++
	s.logf("CRASH %s", n.short())
}

// restart brings a node back the way a process restart does: with the state it
// was configured with, not the state it died holding. This is the returning
// primary — it believes it still owns its slots until gossip tells it otherwise.
func (s *sim) restart(addr string) {
	n := s.byID[addr]
	n.gen++
	n.up = true
	n.offset = 0
	n.mgr = newSimManager(s.t, s.opt.peers, n.addr, n.now)
	n.electng = false
	s.lastOwn[n.idx] = make(map[string]uint64)
	s.logf("RESTART %s", n.short())
	s.after(time.Duration(s.rnd.Int63n(int64(s.opt.pingInterval))), n.tick)
}

// reshard opens a slot migration the way redis-cli drives it: IMPORTING on the
// destination, MIGRATING on the source. Nothing moves yet — the two markers
// only change who answers for keys that are or are not there, and the
// simulation has no keys. What it does model is the window in which two nodes
// are both party to one slot.
func (s *sim) reshard(slot int, fromAddr, toAddr string) {
	src, dst := s.byID[fromAddr], s.byID[toAddr]
	if src == nil || dst == nil {
		s.t.Fatalf("reshard: unknown node %q or %q", fromAddr, toAddr)
	}
	if err := dst.mgr.SetSlotImporting(slot, src.id); err != nil {
		s.logf("reshard %d: importing refused: %v", slot, err)
		return
	}
	if err := src.mgr.SetSlotMigrating(slot, dst.id); err != nil {
		s.logf("reshard %d: migrating refused: %v", slot, err)
		return
	}
	s.logf("RESHARD slot %d  %s -> %s", slot, src.short(), dst.short())
}

// commitReshard closes the window: the destination takes the slot at a raised
// config epoch, and the source gives it up. Everyone else finds out by gossip,
// the same way they find out about a failover.
func (s *sim) commitReshard(slot int, fromAddr, toAddr string) {
	src, dst := s.byID[fromAddr], s.byID[toAddr]
	if err := dst.mgr.SetSlotOwner(slot, dst.id); err != nil {
		s.logf("reshard %d: commit on destination refused: %v", slot, err)
		return
	}
	if err := src.mgr.SetSlotOwner(slot, dst.id); err != nil {
		s.logf("reshard %d: commit on source refused: %v", slot, err)
	}
	s.logf("RESHARD COMMIT slot %d -> %s at epoch %d",
		slot, dst.short(), dst.mgr.View().Myself().ConfigEpoch)
}

// --- reporting -------------------------------------------------------------

func (s *sim) logf(format string, a ...any) {
	s.trace = append(s.trace, fmt.Sprintf("%8s  %s", s.now.Sub(s.start).Round(time.Millisecond), fmt.Sprintf(format, a...)))
}

func (s *sim) shortID(id string) string {
	if n := s.byID[id]; n != nil {
		return n.short()
	}
	return id[:8]
}

// fail reports a violation with the schedule that produced it. Stopping the run
// keeps the trace short enough to read.
func (s *sim) fail(format string, a ...any) {
	if s.failed {
		return
	}
	s.failed = true
	s.t.Errorf("seed %d: %s\n\ntrace:\n%s\n\nstate:\n%s",
		s.seed, fmt.Sprintf(format, a...), strings.Join(s.trace, "\n"), s.dump())
}

func (s *sim) dump() string {
	var b strings.Builder
	for _, n := range s.nodes {
		me := n.mgr.View().Myself()
		state := "up"
		if !n.up {
			state = "DOWN"
		}
		fmt.Fprintf(&b, "  %-9s %-4s side=%d role=%-6s epoch=%d slots=%v offset=%d\n",
			n.short(), state, s.part[n.idx], me.Role, me.ConfigEpoch, me.Slots, n.offset)
	}
	return b.String()
}

// --- what the simulation is for --------------------------------------------

// claim is one node's belief about itself: the slots it would serve, and the
// config epoch it would serve them at.
type claim struct {
	node  *simNode
	slots []SlotRange
	epoch uint64
}

// serving lists the nodes that would answer for slots right now. A node that is
// down still holds an opinion, but it answers nobody, so it cannot split the
// keyspace; only nodes that are up count.
func (s *sim) serving() []claim {
	var out []claim
	for _, n := range s.nodes {
		if !n.up {
			continue
		}
		me := n.mgr.View().Myself()
		if me.Role != RolePrimary || len(me.Slots) == 0 {
			continue
		}
		out = append(out, claim{node: n, slots: me.Slots, epoch: me.ConfigEpoch})
	}
	return out
}

// checkSafety asserts the properties that make the cluster safe. It runs after
// every event, so a violation is caught in the state that produced it rather
// than at the end of the run when gossip has already papered over it.
func (s *sim) checkSafety() {
	if s.failed {
		return
	}

	// 1. No two nodes serve the same slot at the same config epoch.
	//
	// Two owners at *different* epochs is normal and expected: a partitioned
	// primary keeps serving stale data until it can be told otherwise, which is
	// the trade a cache makes. What must never happen is two owners the epoch
	// rule cannot separate, because then nothing decides which one stands down.
	live := s.serving()
	for i := 0; i < len(live); i++ {
		for j := i + 1; j < len(live); j++ {
			if live[i].epoch == live[j].epoch && slotsOverlap(live[i].slots, live[j].slots) {
				s.fail("%s and %s both serve %v at config epoch %d",
					live[i].node.short(), live[j].node.short(), live[i].slots, live[i].epoch)
				return
			}
		}
	}

	// 2. Within one node's view, the config epoch owning a slot never goes
	//    backwards. A view that regresses has accepted a claim the epoch rule
	//    should have rejected, and would route to a node that already stood down.
	for _, n := range s.nodes {
		if !n.up {
			continue
		}
		seen := s.lastOwn[n.idx]
		for _, p := range n.mgr.View().Nodes() {
			if p.Role != RolePrimary || len(p.Slots) == 0 {
				continue
			}
			for _, r := range p.Slots {
				key := r.String()
				if prev, ok := seen[key]; ok && p.ConfigEpoch < prev {
					s.fail("%s: owner of slots %s went from config epoch %d back to %d",
						n.short(), key, prev, p.ConfigEpoch)
					return
				}
				seen[key] = p.ConfigEpoch
			}
		}
	}
}

// recordVote asserts the rule the whole scheme rests on: a primary grants at
// most one vote per epoch. Two grants in one epoch is how two candidates both
// reach a majority.
func (s *sim) recordVote(voter *simNode, req VoteRequest) {
	key := fmt.Sprintf("%s|%d", voter.id, req.Epoch)
	if prev, ok := s.votes[key]; ok {
		s.fail("%s granted two votes in epoch %d: to %s and to %s",
			voter.short(), req.Epoch, s.shortID(prev), s.shortID(req.NodeID))
		return
	}
	s.votes[key] = req.NodeID
	s.logf("%s votes for %s epoch=%d", voter.short(), s.shortID(req.NodeID), req.Epoch)
}

// recordPromotion asserts that a config epoch is claimed by one node only.
// Majorities intersect and each voter votes once, so two nodes reaching a
// majority in one epoch is impossible — unless the counting is wrong.
func (s *sim) recordPromotion(winner *simNode, epoch uint64) {
	if winner == nil {
		return
	}
	if prev, ok := s.promos[epoch]; ok && prev != winner.id {
		s.fail("config epoch %d claimed by both %s and %s",
			epoch, s.shortID(prev), winner.short())
		return
	}
	s.promos[epoch] = winner.id
}

// --- convergence -----------------------------------------------------------

// converged reports whether every reachable node agrees on who owns every slot
// and the whole keyspace is covered. This is the liveness side: safety is kept
// by refusing to act, and a cluster that never recovers satisfies it perfectly.
func (s *sim) converged() (bool, string) {
	var reference []string
	var refNode *simNode
	for _, n := range s.nodes {
		if !n.up {
			continue
		}
		v := n.mgr.View()
		if v.AssignedSlots() != SlotCount {
			return false, fmt.Sprintf("%s sees %d/%d slots assigned", n.short(), v.AssignedSlots(), SlotCount)
		}
		// What a cluster client actually validates: the advertised ranges must
		// sum to exactly 16384. Two nodes each still listing a slot they no
		// longer share pushes this over, and go-redis rejects the topology
		// outright rather than picking one.
		advertised := 0
		for _, p := range v.Primaries() {
			advertised += p.SlotsCount()
		}
		if advertised != SlotCount {
			return false, fmt.Sprintf("%s advertises %d slots across its primaries, want %d",
				n.short(), advertised, SlotCount)
		}
		owners := s.ownerSummary(v)
		if refNode == nil {
			reference, refNode = owners, n
			continue
		}
		if strings.Join(owners, ",") != strings.Join(reference, ",") {
			return false, fmt.Sprintf("%s and %s disagree:\n    %s\n    %s",
				refNode.short(), n.short(), strings.Join(reference, " "), strings.Join(owners, " "))
		}
	}
	return true, ""
}

// ownerSummary is the slot map as a client would read it: which node owns which
// ranges. Sampling the boundaries of every range is enough — ownership only
// changes at a range edge.
func (s *sim) ownerSummary(v *Topology) []string {
	var out []string
	var cur *Node
	start := 0
	for slot := 0; slot < SlotCount; slot++ {
		o := v.OwnerOf(slot)
		if slot == 0 {
			cur = o
			continue
		}
		if o != cur {
			out = append(out, fmt.Sprintf("%d-%d:%s", start, slot-1, s.ownerName(cur)))
			cur, start = o, slot
		}
	}
	out = append(out, fmt.Sprintf("%d-%d:%s", start, SlotCount-1, s.ownerName(cur)))
	sort.Strings(out)
	return out
}

func (s *sim) ownerName(n *Node) string {
	if n == nil {
		return "none"
	}
	return s.shortID(n.ID)
}

// runUntilStable advances the simulation until the cluster has settled, or
// fails the test with the reason it never did.
//
// Settled means three things at once, because convergence alone is trivially
// true in the gap between a node dying and anyone noticing: every scheduled
// fault has fired, every live node agrees on the slot map, and that has stayed
// true with no further promotions for several failure-detection windows.
func (s *sim) runUntilStable(limit time.Duration) {
	step := s.opt.pingInterval
	settle := 4 * s.opt.nodeTimeout
	stableSince := time.Duration(-1)
	lastPromos := -1
	why := "never converged"

	for waited := time.Duration(0); waited < limit; waited += step {
		s.run(s.elapsed() + step)
		if s.failed {
			return
		}
		ok, reason := s.converged()
		if !ok || len(s.promos) != lastPromos || s.elapsed() < s.faultsUntil {
			lastPromos = len(s.promos)
			stableSince = -1
			if !ok {
				why = reason
			}
			continue
		}
		if stableSince < 0 {
			stableSince = s.elapsed()
			continue
		}
		if s.elapsed()-stableSince >= settle {
			return
		}
	}
	s.fail("did not settle within %s: %s", limit, why)
}
