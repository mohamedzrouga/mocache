package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Manager owns the live cluster state and every transition it can make:
// health, config epochs, votes, promotion and demotion. It does no I/O — the
// runtime in internal/node drives it — so the rules that decide who owns a slot
// can be tested without a network.
//
// Readers never touch this lock: the RESP server reads an immutable *Topology
// snapshot published atomically after each change.
type Manager struct {
	mu      sync.Mutex
	me      string
	nodes   map[string]*liveNode
	enabled bool

	// currentEpoch is the highest election epoch seen anywhere in the cluster;
	// lastVote* record the one vote we may cast per epoch.
	currentEpoch  uint64
	lastVoteEpoch uint64
	lastVoteFor   string

	view atomic.Pointer[Topology]

	failovers atomic.Uint64

	// clock is time.Now everywhere but the partition simulation, which needs a
	// run to be reproducible from its seed. Every deadline below is a local
	// duration (silence since lastPong, age of a suspicion report), never a
	// timestamp compared against another node's, so nodes may disagree about
	// what time it is without disagreeing about who is alive.
	clock func() time.Time

	// migrating and importing are the two halves of a live reshard, held per
	// slot exactly as Redis holds them: local to the two nodes involved and
	// never gossiped. Only the final SETSLOT NODE travels, as a raised config
	// epoch, so a reshard abandoned halfway leaves no trace anywhere else.
	migrating map[int]string // slot -> destination node ID
	importing map[int]string // slot -> source node ID
}

type liveNode struct {
	id          string
	host        string
	port        int
	role        Role
	primaryAddr string
	slots       []SlotRange
	configEpoch uint64

	lastPong   time.Time
	pfail      bool
	fail       bool
	linkUp     bool
	replOffset uint64
	// reports records which nodes told us this one looks dead, and when. A
	// single opinion never removes a primary; a majority does.
	reports map[string]time.Time
}

func (n *liveNode) addr() string { return joinHostPort(n.host, n.port) }

// NewManager builds cluster state from peer specifications.
func NewManager(peers []string, announce string) (*Manager, error) {
	topo, err := Parse(peers, announce)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		me: topo.Myself().ID, nodes: make(map[string]*liveNode), enabled: true, clock: time.Now,
		migrating: make(map[int]string), importing: make(map[int]string),
	}
	now := m.clock()
	for _, n := range topo.Nodes() {
		m.nodes[n.ID] = &liveNode{
			id: n.ID, host: n.Host, port: n.Port, role: n.Role,
			primaryAddr: n.PrimaryO, slots: n.Slots,
			lastPong: now, reports: make(map[string]time.Time),
		}
	}
	m.rebuildLocked()
	return m, nil
}

// StandaloneManager is the non-cluster view: one node, every slot, no bus.
func StandaloneManager(host string, port int) *Manager {
	m := &Manager{
		me: NodeID(joinHostPort(host, port)), nodes: make(map[string]*liveNode), clock: time.Now,
		migrating: make(map[int]string), importing: make(map[int]string),
	}
	m.nodes[m.me] = &liveNode{
		id: m.me, host: host, port: port, lastPong: m.clock(),
		reports: make(map[string]time.Time),
	}
	m.rebuildLocked()
	return m
}

// View returns the current immutable snapshot.
func (m *Manager) View() *Topology { return m.view.Load() }

// Enabled reports cluster mode.
func (m *Manager) Enabled() bool { return m.enabled }

// Failovers counts promotions this node has applied, for /metrics.
func (m *Manager) Failovers() uint64 { return m.failovers.Load() }

// NewReplID generates a replication stream identifier. A promoted primary takes
// a fresh one so replicas cannot mistake its offsets for the old primary's.
func NewReplID() string {
	var b [20]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// rebuildLocked publishes a new snapshot. Caller holds m.mu.
func (m *Manager) rebuildLocked() {
	t := &Topology{byID: make(map[string]*Node, len(m.nodes)), enabled: m.enabled}
	for _, ln := range m.nodes {
		n := &Node{
			ID: ln.id, Host: ln.host, Port: ln.port, Role: ln.role,
			PrimaryO: ln.primaryAddr, Slots: append([]SlotRange(nil), ln.slots...),
			ConfigEpoch: ln.configEpoch, ReplOffset: ln.replOffset,
			PFail: ln.pfail, Fail: ln.fail, LinkUp: ln.linkUp,
		}
		t.nodes = append(t.nodes, n)
		t.byID[n.ID] = n
		if n.ID == m.me {
			t.me = n
		}
	}
	sort.Slice(t.nodes, func(i, j int) bool { return t.nodes[i].Addr() < t.nodes[j].Addr() })

	// Assign owners in config-epoch order so the highest epoch wins any
	// overlap. State should never contain two claims on one slot, but while
	// gossip converges it briefly can, and what we serve must still be a single
	// unambiguous answer rather than whichever node happened to be iterated last.
	byEpoch := append([]*Node(nil), t.nodes...)
	sort.SliceStable(byEpoch, func(i, j int) bool { return byEpoch[i].ConfigEpoch < byEpoch[j].ConfigEpoch })
	for _, n := range byEpoch {
		if n.Role != RolePrimary {
			continue
		}
		for _, r := range n.Slots {
			for s := r.Start; s <= r.End; s++ {
				t.owner[s] = n
			}
		}
	}
	// The snapshot owns its own copy: readers hold a *Topology for the length
	// of a command and must not see a reshard change under them.
	if len(m.migrating) > 0 || len(m.importing) > 0 {
		t.migrating = make(map[int]string, len(m.migrating))
		t.importing = make(map[int]string, len(m.importing))
		for k, v := range m.migrating {
			t.migrating[k] = v
		}
		for k, v := range m.importing {
			t.importing[k] = v
		}
	}
	m.view.Store(t)
}

// --- health ---------------------------------------------------------------

// NoteContact records a successful exchange with a peer: it is alive, so any
// suspicion of it is dropped.
func (m *Manager) NoteContact(id string, replOffset, configEpoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.nodes[id]
	if n == nil {
		return
	}
	n.lastPong = m.clock()
	n.linkUp = true
	n.replOffset = replOffset
	changed := n.pfail || n.fail
	n.pfail, n.fail = false, false
	if changed {
		n.reports = make(map[string]time.Time)
	}
	if changed {
		m.rebuildLocked()
	}
}

// NoteUnreachable records a failed exchange and marks the node PFAIL once it
// has been silent for longer than timeout.
func (m *Manager) NoteUnreachable(id string, timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.nodes[id]
	if n == nil {
		return
	}
	n.linkUp = false
	if !n.pfail && m.clock().Sub(n.lastPong) > timeout {
		n.pfail = true
		m.rebuildLocked()
	}
}

// GossipNode is one node's state as told by a peer.
type GossipNode struct {
	ID          string      `json:"id"`
	Host        string      `json:"h"`
	Port        int         `json:"p"`
	Role        byte        `json:"r"`
	PrimaryAddr string      `json:"pa,omitempty"`
	Slots       []SlotRange `json:"s,omitempty"`
	ConfigEpoch uint64      `json:"ce"`
	ReplOffset  uint64      `json:"ro"`
	PFail       bool        `json:"pf,omitempty"`
	Fail        bool        `json:"f,omitempty"`
}

// Gossip returns what we know, to be sent in a PING or PONG.
func (m *Manager) Gossip() []GossipNode {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]GossipNode, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, GossipNode{
			ID: n.id, Host: n.host, Port: n.port, Role: byte(n.role),
			PrimaryAddr: n.primaryAddr, Slots: n.slots,
			ConfigEpoch: n.configEpoch, ReplOffset: n.replOffset,
			PFail: n.pfail, Fail: n.fail,
		})
	}
	// Sorted, not map order: the receiver applies these claims in the order they
	// arrive, so a stable order is what makes a simulation run reproducible from
	// its seed and a packet capture diffable against the next one.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MergeGossip folds a peer's view into ours. Two things travel this way:
// suspicion (which accumulates into a majority) and configuration (a higher
// config epoch always wins, which is how a promotion reaches everyone).
func (m *Manager) MergeGossip(from string, epoch uint64, nodes []GossipNode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if epoch > m.currentEpoch {
		m.currentEpoch = epoch
	}
	changed := false
	for _, g := range nodes {
		if g.ID == m.me {
			// Never take another node's word about whether we are alive — we
			// are the authority on that. Configuration is the opposite case:
			// see adoptSelfClaimLocked.
			if m.adoptSelfClaimLocked(g) {
				changed = true
			}
			continue
		}
		n := m.nodes[g.ID]
		if n == nil {
			n = &liveNode{id: g.ID, host: g.Host, port: g.Port, lastPong: m.clock(), reports: make(map[string]time.Time)}
			m.nodes[g.ID] = n
			changed = true
		}
		if g.ConfigEpoch > n.configEpoch {
			n.role = Role(g.Role)
			n.primaryAddr = g.PrimaryAddr
			n.slots = g.Slots
			n.configEpoch = g.ConfigEpoch
			changed = true
			// Slots move as a unit: whoever holds them at the highest config
			// epoch is the owner, and every older claim on them is void. Doing
			// this here is what makes the topology self-healing — a node that
			// missed the promotion broadcast still converges through gossip
			// instead of advertising two owners for one slot forever.
			if len(n.slots) > 0 && m.claimSlotsLocked(n) {
				changed = true
			}
		}
		// Which primary a replica follows carries no config epoch, so it cannot
		// travel by the rule above — yet it has to travel, or a voter would
		// refuse to vote for a candidate whose primary it still thinks is
		// someone else. A node is the authority on the primary it chose, so
		// only its own word counts; a relayed copy could be third-hand and
		// stale. Restricting this to a node we already credit with no slots is
		// what keeps it from undoing a promotion the node itself has forgotten
		// — see adoptSelfClaimLocked for the other half of that case.
		if from == g.ID && Role(g.Role) == RoleReplica && len(g.Slots) == 0 &&
			len(n.slots) == 0 && n.primaryAddr != g.PrimaryAddr {
			n.role = RoleReplica
			n.primaryAddr = g.PrimaryAddr
			changed = true
		}
		if g.ReplOffset > n.replOffset {
			n.replOffset = g.ReplOffset
		}
		switch {
		case g.Fail && !n.fail:
			// FAIL is a cluster-wide decision already taken by a majority
			// somewhere; adopting it keeps every node's view consistent.
			n.fail = true
			changed = true
		case g.PFail || g.Fail:
			n.reports[from] = m.clock()
		default:
			delete(n.reports, from)
		}
	}
	if m.checkSelfDemotionLocked() {
		changed = true
	}
	if changed {
		m.rebuildLocked()
	}
}

// checkSelfDemotionLocked handles the returning-primary case: while we were
// away, a replica was promoted and took our slots at a higher config epoch.
// Continuing to serve them would mean two nodes answering for one slot with
// different data — exactly the split the epoch mechanism exists to prevent —
// so we stand down and become a replica of whoever holds them now.
// claimSlotsLocked voids older claims on the slots owner now holds, demoting
// any node left with nothing to a replica of the new owner.
func (m *Manager) claimSlotsLocked(owner *liveNode) bool {
	changed := false
	claimed := setOf(owner.slots)
	for _, n := range m.nodes {
		if n.id == owner.id || len(n.slots) == 0 || n.configEpoch >= owner.configEpoch {
			continue
		}
		held := setOf(n.slots)
		if !held.intersects(claimed) {
			continue
		}
		held.sub(claimed)
		n.slots = held.ranges()
		// Only a node left with nothing becomes a replica. A failover moves a
		// whole shard and lands here; a reshard moves one slot, and the node
		// that gave it up must keep serving the rest of its keyspace.
		if len(n.slots) == 0 {
			n.role = RoleReplica
			n.primaryAddr = owner.addr()
		}
		changed = true
	}
	return changed
}

// adoptSelfClaimLocked takes back a promotion this node won and then forgot.
//
// A node keeps no state across a restart: it comes back with the role its
// -cluster-peer flags describe, which for a replica that was promoted is the
// wrong one. The rest of the cluster still holds its promotion at a higher
// config epoch, and neither side would ever have corrected the other, because
// gossip about ourselves used to be discarded wholesale. Clients were then sent
// to a node that disowned the slots and redirected them back — a redirect loop
// that no amount of waiting resolved.
//
// Adopting is safe for the same reason the epoch rule is: the claim already won
// a majority vote, and it is taken only when nothing we know of holds those
// slots at an equal or higher epoch — otherwise a stale gossip entry could
// resurrect a claim we have already lost. Coming back as an empty primary is
// what a cache does anyway; it refills on misses.
func (m *Manager) adoptSelfClaimLocked(g GossipNode) bool {
	me := m.nodes[m.me]
	if me == nil || Role(g.Role) != RolePrimary || len(g.Slots) == 0 {
		return false
	}
	if g.ConfigEpoch <= me.configEpoch {
		return false
	}
	for _, n := range m.nodes {
		if n.id == m.me || n.role != RolePrimary || len(n.slots) == 0 {
			continue
		}
		if n.configEpoch >= g.ConfigEpoch && slotsOverlap(n.slots, g.Slots) {
			return false
		}
	}

	me.role = RolePrimary
	me.slots = g.Slots
	me.primaryAddr = ""
	me.configEpoch = g.ConfigEpoch
	if g.ConfigEpoch > m.currentEpoch {
		// Our next election must not reuse an epoch already spent on this claim.
		m.currentEpoch = g.ConfigEpoch
	}
	m.claimSlotsLocked(me)
	return true
}

func (m *Manager) checkSelfDemotionLocked() bool {
	me := m.nodes[m.me]
	if me == nil || me.role != RolePrimary || len(me.slots) == 0 {
		return false
	}
	// Give up exactly the slots someone else holds at a higher epoch — a
	// reshard takes one, a failover takes them all — and follow the highest
	// claimant, not the first one found: only the highest survives the same
	// rule everywhere else, so following a lower one would mean replicating
	// from a node that is itself about to stand down.
	mine := setOf(me.slots)
	var winner *liveNode
	lost := false
	for _, n := range m.nodes {
		if n.id == m.me || n.role != RolePrimary || n.configEpoch <= me.configEpoch || len(n.slots) == 0 {
			continue
		}
		theirs := setOf(n.slots)
		if !mine.intersects(theirs) {
			continue
		}
		mine.sub(theirs)
		lost = true
		if winner == nil || n.configEpoch > winner.configEpoch ||
			(n.configEpoch == winner.configEpoch && n.id < winner.id) {
			winner = n
		}
	}
	if !lost {
		return false
	}
	me.slots = mine.ranges()
	if len(me.slots) == 0 {
		me.role = RoleReplica
		me.primaryAddr = winner.addr()
		me.configEpoch = 0
	}
	return true
}

// ShouldFail reports whether enough primaries suspect a node for us to declare
// it failed. "Enough" is a majority of primaries, counting our own suspicion.
func (m *Manager) ShouldFail(id string, window time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.nodes[id]
	if n == nil || n.fail || !n.pfail {
		return false
	}
	// Only slot-owning primaries get a say, ourselves included. A replica can
	// suspect a node but cannot make the cluster agree on its own; it learns
	// the decision through gossip instead.
	votes := 0
	if m.isPrimaryLocked(m.me) {
		votes++
	}
	cutoff := m.clock().Add(-window)
	for reporter, at := range n.reports {
		if at.After(cutoff) && m.isPrimaryLocked(reporter) {
			votes++
		}
	}
	return votes > m.primaryCountLocked()/2
}

// MarkFail records the cluster-wide failure decision.
func (m *Manager) MarkFail(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.nodes[id]
	if n == nil || n.fail {
		return false
	}
	n.fail = true
	m.rebuildLocked()
	return true
}

func (m *Manager) isPrimaryLocked(id string) bool {
	n := m.nodes[id]
	return n != nil && n.role == RolePrimary && len(n.slots) > 0
}

func (m *Manager) primaryCountLocked() int {
	n := 0
	for _, node := range m.nodes {
		if node.role == RolePrimary && len(node.slots) > 0 {
			n++
		}
	}
	return n
}

// --- elections -------------------------------------------------------------

// VoteRequest is a replica asking to take over its primary's slots.
type VoteRequest struct {
	Epoch      uint64 `json:"epoch"`
	NodeID     string `json:"node_id"`
	PrimaryID  string `json:"primary_id"`
	ReplOffset uint64 `json:"repl_offset"`
	Force      bool   `json:"force,omitempty"` // manual CLUSTER FAILOVER
}

// VoteReply answers one.
type VoteReply struct {
	Epoch   uint64 `json:"epoch"`
	Granted bool   `json:"granted"`
	Reason  string `json:"reason,omitempty"`
	VoterID string `json:"voter_id"`
}

// NextEpoch claims the next election epoch for our own candidacy.
func (m *Manager) NextEpoch() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentEpoch++
	return m.currentEpoch
}

func (m *Manager) CurrentEpoch() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentEpoch
}

// ConsiderVote decides whether to grant a promotion vote.
//
// The three conditions are what keep two nodes from owning one slot: one vote
// per epoch, only from a primary, and only for a replica of a primary that this
// voter also believes has failed. A manual failover skips the failure check
// because an operator asked for it explicitly.
func (m *Manager) ConsiderVote(req VoteRequest) VoteReply {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply := VoteReply{Epoch: req.Epoch, VoterID: m.me}

	if req.Epoch > m.currentEpoch {
		m.currentEpoch = req.Epoch
	}
	if !m.isPrimaryLocked(m.me) {
		reply.Reason = "voter is not a slot-owning primary"
		return reply
	}
	if req.Epoch <= m.lastVoteEpoch {
		reply.Reason = "already voted in this epoch"
		return reply
	}
	candidate := m.nodes[req.NodeID]
	if candidate == nil {
		reply.Reason = "unknown candidate"
		return reply
	}
	failed := m.nodes[req.PrimaryID]
	if failed == nil {
		reply.Reason = "unknown primary"
		return reply
	}
	if candidate.primaryAddr != failed.addr() {
		reply.Reason = "candidate is not a replica of that primary"
		return reply
	}
	if !req.Force && !failed.fail {
		reply.Reason = "primary is not marked failed here"
		return reply
	}

	m.lastVoteEpoch = req.Epoch
	m.lastVoteFor = req.NodeID
	reply.Granted = true
	return reply
}

// Promote makes a node the primary for its former primary's slots at a new
// config epoch. It is used both when we win an election and when we learn of
// someone else's promotion, so both paths apply exactly the same transition.
//
// Returns false when the claim is stale — a node whose config epoch is not
// higher than the current owner's must not take the slots.
func (m *Manager) Promote(nodeID string, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	winner := m.nodes[nodeID]
	if winner == nil {
		return false
	}
	old := m.nodeByAddrLocked(winner.primaryAddr)
	if old == nil {
		return false
	}
	if epoch <= old.configEpoch {
		return false
	}

	winner.role = RolePrimary
	winner.slots = old.slots
	winner.primaryAddr = ""
	winner.configEpoch = epoch

	// The old primary becomes a replica of the winner. When it comes back it
	// will see this configuration, demote itself and resynchronise instead of
	// serving data nobody else believes in.
	old.role = RoleReplica
	old.slots = nil
	old.primaryAddr = winner.addr()

	// Every other replica of the old primary now follows the winner.
	for _, n := range m.nodes {
		if n.id != winner.id && n.role == RoleReplica && n.primaryAddr == old.addr() {
			n.primaryAddr = winner.addr()
		}
	}
	m.claimSlotsLocked(winner)
	if epoch > m.currentEpoch {
		m.currentEpoch = epoch
	}
	m.failovers.Add(1)
	m.rebuildLocked()
	return true
}

func (m *Manager) nodeByAddrLocked(addr string) *liveNode {
	for _, n := range m.nodes {
		if n.addr() == addr {
			return n
		}
	}
	return nil
}

// SetReplOffset records our own replication progress so it is gossiped.
func (m *Manager) SetReplOffset(offset uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n := m.nodes[m.me]; n != nil {
		n.replOffset = offset
	}
}

// FailoverCandidacy reports whether we should try to take over, and our rank
// among the siblings that could. Rank 0 is the most up-to-date replica; a
// higher rank waits longer, so the best candidate usually wins uncontested.
func (m *Manager) FailoverCandidacy(myOffset uint64) (primaryID string, rank int, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	me := m.nodes[m.me]
	if me == nil || me.role != RoleReplica {
		return "", 0, false
	}
	primary := m.nodeByAddrLocked(me.primaryAddr)
	if primary == nil || !primary.fail {
		return "", 0, false
	}
	for _, n := range m.nodes {
		if n.id == m.me || n.role != RoleReplica || n.primaryAddr != me.primaryAddr {
			continue
		}
		if n.fail || n.pfail {
			continue // a sibling we cannot reach is not competing
		}
		if n.replOffset > myOffset {
			rank++
		}
	}
	return primary.id, rank, true
}

// PrimaryBusAddr returns the bus address this node should replicate from, or ""
// if it is a primary.
func (m *Manager) PrimaryBusAddr() string {
	v := m.View()
	me := v.Myself()
	if me == nil || me.Role != RoleReplica {
		return ""
	}
	p := v.PrimaryOf(me)
	if p == nil {
		return ""
	}
	return p.BusAddr()
}

// Demote makes this node a replica of another: a returning primary that has
// been replaced, an operator running CLUSTER REPLICATE, or a spare moving to
// cover an orphaned shard.
//
// A node that already replicates something may be re-pointed — that is what
// replica migration is — but a node that still owns slots may not, because
// giving them up silently would strand that part of the keyspace.
func (m *Manager) Demote(primaryID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	me, p := m.nodes[m.me], m.nodes[primaryID]
	if me == nil || p == nil || p.id == m.me || len(me.slots) > 0 {
		return false
	}
	if me.role == RoleReplica && me.primaryAddr == p.addr() {
		return true // already following it; CLUSTER REPLICATE is idempotent
	}
	me.role = RoleReplica
	me.slots = nil
	me.primaryAddr = p.addr()
	m.rebuildLocked()
	return true
}

func joinHostPort(host string, port int) string {
	return netJoinHostPort(host, port)
}
