package cluster

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
)

// Role of a node within its shard.
type Role int

const (
	RolePrimary Role = iota
	RoleReplica
)

func (r Role) String() string {
	if r == RoleReplica {
		return "slave" // the wire word CLUSTER NODES uses; clients parse it
	}
	return "master"
}

// SlotRange is an inclusive span of the 16384-slot keyspace.
type SlotRange struct{ Start, End int }

func (s SlotRange) String() string {
	if s.Start == s.End {
		return strconv.Itoa(s.Start)
	}
	return strconv.Itoa(s.Start) + "-" + strconv.Itoa(s.End)
}

// Node is one cache process in the cluster view. A Node inside a Topology is a
// read-only snapshot: the Manager rebuilds the whole view rather than mutating
// nodes in place, so a reader never sees a half-applied topology change.
type Node struct {
	ID       string // 40 hex chars, like Redis; derived from Addr so every node agrees
	Host     string
	Port     int
	Role     Role
	PrimaryO string // announce address of the primary, when this node is a replica
	Slots    []SlotRange

	// Live state, meaningful once the cluster bus is running.
	ConfigEpoch uint64 // version of this node's slot claim; higher wins
	ReplOffset  uint64 // replication progress, used to rank failover candidates
	PFail       bool   // we have not heard from it within cluster-node-timeout
	Fail        bool   // a majority of primaries agree it is gone
	LinkUp      bool   // our bus link to it is currently connected
}

// Healthy reports whether the node is believed to be serving.
func (n *Node) Healthy() bool { return !n.Fail && !n.PFail }

// BusAddr is the cluster-bus address: client port + 10000, as Redis does.
func (n *Node) BusAddr() string { return net.JoinHostPort(n.Host, strconv.Itoa(n.BusPort())) }

// Addr is the announce address other nodes and clients connect to.
func (n *Node) Addr() string { return net.JoinHostPort(n.Host, strconv.Itoa(n.Port)) }

// BusPort is the cluster bus port. Redis uses client port + 10000 and clients
// expect to see it in CLUSTER NODES even when no bus is running yet.
func (n *Node) BusPort() int { return n.Port + 10000 }

// OwnsSlot reports whether this node serves the slot.
func (n *Node) OwnsSlot(slot int) bool {
	for _, r := range n.Slots {
		if slot >= r.Start && slot <= r.End {
			return true
		}
	}
	return false
}

// SlotsCount is how many of the 16384 slots this node serves.
func (n *Node) SlotsCount() int {
	total := 0
	for _, r := range n.Slots {
		total += r.End - r.Start + 1
	}
	return total
}

// Topology is an immutable cluster view. It is built once at startup from
// -cluster-peer flags: every node is given the same list, so all of them agree
// without any gossip. Phase two replaces this with a live membership view; the
// read-side API (OwnerOf, Myself, Nodes) is what the RESP server depends on and
// is meant to survive that change.
type Topology struct {
	nodes   []*Node
	me      *Node
	byID    map[string]*Node
	owner   [SlotCount]*Node
	enabled bool

	// Reshard state for slots this node is a party to. Nil on a node not
	// resharding, which is the common case.
	migrating map[int]string // slot -> destination node ID
	importing map[int]string // slot -> source node ID
}

// Migrating returns the node ID a slot is leaving this node for, or "" when it
// is not being migrated away. While a slot is migrating, keys that are still
// here are served here and keys that are gone are answered with ASK.
func (t *Topology) Migrating(slot int) string {
	if t == nil || t.migrating == nil {
		return ""
	}
	return t.migrating[slot]
}

// Importing returns the node ID a slot is arriving from, or "" when it is not
// being imported. While a slot is importing, this node serves it only for
// clients that sent ASKING; everyone else is sent back to the current owner.
func (t *Topology) Importing(slot int) string {
	if t == nil || t.importing == nil {
		return ""
	}
	return t.importing[slot]
}

// Resharding reports whether any slot here is mid-migration, which CLUSTER INFO
// and the reshard-safety checks need to know.
func (t *Topology) Resharding() bool {
	return t != nil && (len(t.migrating) > 0 || len(t.importing) > 0)
}

// MigratingSlots and ImportingSlots are the per-slot markers CLUSTER NODES
// renders as [slot->-nodeid] and [slot-<-nodeid].
func (t *Topology) MigratingSlots() map[int]string {
	if t == nil {
		return nil
	}
	return t.migrating
}

func (t *Topology) ImportingSlots() map[int]string {
	if t == nil {
		return nil
	}
	return t.importing
}

// Disabled is the standalone view: no slots, no redirects, CLUSTER INFO reports
// cluster_enabled:0. This is the default, and what a plain redis.Redis() client
// expects to find.
func Disabled(host string, port int) *Topology {
	me := &Node{ID: NodeID(net.JoinHostPort(host, strconv.Itoa(port))), Host: host, Port: port}
	return &Topology{nodes: []*Node{me}, me: me, byID: map[string]*Node{me.ID: me}}
}

// NodeID derives a stable 40-hex-character node ID from an announce address.
// Redis generates a random ID and gossips it; deriving it means every node and
// every restart computes the same ID for the same address, which is what makes
// a static cluster view consistent without a membership protocol.
func NodeID(addr string) string {
	sum := sha1.Sum([]byte("mocache-node:" + addr))
	return hex.EncodeToString(sum[:])
}

// Parse builds a topology from peer specifications and identifies which entry
// is this node, by matching announce.
//
//	10.0.0.1:6379=0-5460
//	10.0.0.2:6379=5461-10922,12000-12100
//	10.0.0.4:6379=replica-of:10.0.0.1:6379
func Parse(peers []string, announce string) (*Topology, error) {
	if len(peers) == 0 {
		return nil, errors.New("cluster: no peers")
	}
	t := &Topology{byID: make(map[string]*Node), enabled: true}
	replicaOf := make(map[string]string)

	for _, spec := range peers {
		addr, rest, ok := strings.Cut(spec, "=")
		if !ok {
			return nil, fmt.Errorf("cluster: peer %q must be host:port=slots", spec)
		}
		addr = strings.TrimSpace(addr)
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("cluster: peer %q: %w", spec, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("cluster: peer %q: bad port", spec)
		}
		n := &Node{ID: NodeID(addr), Host: host, Port: port}

		if primary, isReplica := strings.CutPrefix(strings.TrimSpace(rest), "replica-of:"); isReplica {
			n.Role = RoleReplica
			n.PrimaryO = strings.TrimSpace(primary)
			replicaOf[addr] = n.PrimaryO
		} else if ranges, err := parseRanges(rest); err != nil {
			return nil, fmt.Errorf("cluster: peer %q: %w", spec, err)
		} else {
			n.Slots = ranges
		}

		if _, dup := t.byID[n.ID]; dup {
			return nil, fmt.Errorf("cluster: duplicate peer %q", addr)
		}
		t.nodes = append(t.nodes, n)
		t.byID[n.ID] = n
	}

	for _, n := range t.nodes {
		if n.Role != RoleReplica {
			continue
		}
		if t.byAddr(n.PrimaryO) == nil {
			return nil, fmt.Errorf("cluster: replica %s points at unknown primary %q", n.Addr(), n.PrimaryO)
		}
	}

	// Ownership must be unambiguous: a slot served by two primaries means
	// clients would see different data depending on which one they asked.
	for _, n := range t.nodes {
		for _, r := range n.Slots {
			for s := r.Start; s <= r.End; s++ {
				if prev := t.owner[s]; prev != nil {
					return nil, fmt.Errorf("cluster: slot %d claimed by both %s and %s", s, prev.Addr(), n.Addr())
				}
				t.owner[s] = n
			}
		}
	}

	t.me = t.byAddr(announce)
	if t.me == nil {
		return nil, fmt.Errorf("cluster: announce address %q is not among the peers", announce)
	}
	sort.Slice(t.nodes, func(i, j int) bool { return t.nodes[i].Addr() < t.nodes[j].Addr() })
	return t, nil
}

func parseRanges(s string) ([]SlotRange, error) {
	var out []SlotRange
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		start, err := strconv.Atoi(strings.TrimSpace(lo))
		if err != nil {
			return nil, fmt.Errorf("bad slot %q", part)
		}
		end := start
		if isRange {
			if end, err = strconv.Atoi(strings.TrimSpace(hi)); err != nil {
				return nil, fmt.Errorf("bad slot range %q", part)
			}
		}
		if start < 0 || end >= SlotCount || start > end {
			return nil, fmt.Errorf("slot range %q outside 0-%d", part, SlotCount-1)
		}
		out = append(out, SlotRange{Start: start, End: end})
	}
	if len(out) == 0 {
		return nil, errors.New("no slots assigned")
	}
	return out, nil
}

func (t *Topology) byAddr(addr string) *Node {
	for _, n := range t.nodes {
		if n.Addr() == addr {
			return n
		}
	}
	return nil
}

// Enabled reports whether cluster mode is on (CLUSTER INFO cluster_enabled).
func (t *Topology) Enabled() bool { return t != nil && t.enabled }

// Myself is this node's entry; never nil.
func (t *Topology) Myself() *Node { return t.me }

// Nodes returns every node, sorted by address.
func (t *Topology) Nodes() []*Node { return t.nodes }

// OwnerOf returns the primary serving a slot, or nil when the slot is unassigned.
func (t *Topology) OwnerOf(slot int) *Node {
	if slot < 0 || slot >= SlotCount {
		return nil
	}
	return t.owner[slot]
}

// Mine reports whether this node should answer for the slot itself. In
// standalone mode every slot is ours. A replica answers for its primary's slots
// only after the client sends READONLY, which the server tracks per connection.
func (t *Topology) Mine(slot int) bool {
	if !t.Enabled() {
		return true
	}
	owner := t.OwnerOf(slot)
	return owner != nil && owner.ID == t.me.ID
}

// ServedBy returns the primary for a slot, following a replica back to it.
func (t *Topology) PrimaryOf(n *Node) *Node {
	if n == nil || n.Role != RoleReplica {
		return n
	}
	return t.byAddr(n.PrimaryO)
}

// ReplicasOf lists the replicas configured for a primary.
func (t *Topology) ReplicasOf(primary *Node) []*Node {
	var out []*Node
	for _, n := range t.nodes {
		if n.Role == RoleReplica && n.PrimaryO == primary.Addr() {
			out = append(out, n)
		}
	}
	return out
}

// Primaries lists slot-owning nodes, in slot order — the order CLUSTER SLOTS
// and CLUSTER SHARDS present shards in.
func (t *Topology) Primaries() []*Node {
	var out []*Node
	for _, n := range t.nodes {
		if n.Role == RolePrimary && len(n.Slots) > 0 {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slots[0].Start < out[j].Slots[0].Start })
	return out
}

// AssignedSlots counts slots with an owner; a cluster is only "ok" at 16384.
func (t *Topology) AssignedSlots() int {
	n := 0
	for _, o := range t.owner {
		if o != nil {
			n++
		}
	}
	return n
}

// State is "ok" only when the whole keyspace is covered, matching Redis: with a
// gap, some keys would have nowhere to go and clients must not treat the
// cluster as usable.
func (t *Topology) State() string {
	if t.AssignedSlots() == SlotCount {
		return "ok"
	}
	return "fail"
}

// netJoinHostPort exists so manager.go can build addresses without importing
// net directly; it must format identically to Node.Addr or IDs will not match.
func netJoinHostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}
