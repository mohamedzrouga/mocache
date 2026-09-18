package node

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/mohamedzrouga/mocache/internal/bus"
	"github.com/mohamedzrouga/mocache/internal/cluster"
	"github.com/mohamedzrouga/mocache/internal/server"
)

// The transport half of live slot migration, and automatic replica migration.
//
// MIGRATE is a client command, so its shape is Redis's; what travels between
// the two nodes is ours, over the cluster bus. A batch is one frame: the keys
// and their absolute expiries in the JSON header, the values concatenated in
// the body, so a migration pays no base64 tax — the same split replication uses.

// MigrateHeader is the control half of a TypeMigrate frame.
type MigrateHeader struct {
	Replace bool          `json:"replace,omitempty"`
	Keys    []MigrateItem `json:"keys"`
}

// MigrateItem describes one key; its value is the next Len bytes of the body.
type MigrateItem struct {
	Key string `json:"k"`
	// ExpireMs is an absolute unix millisecond instant, 0 for no expiry. It is
	// absolute for the same reason replication's is: a relative TTL would
	// restart its clock here and outlive the copy it replaced.
	ExpireMs int64 `json:"x,omitempty"`
	Len      int   `json:"n"`
}

// MigrateKeys implements server.Runtime: ship keys to the node that is
// importing their slot. The whole batch is one frame and one round trip, so the
// destination either has all of it or none of it.
func (c *Cluster) MigrateKeys(addr string, keys []server.MigratedKey, replace bool, timeout time.Duration) error {
	view := c.mgr.View()
	var dest *cluster.Node
	for _, n := range view.Nodes() {
		if n.Addr() == addr {
			dest = n
			break
		}
	}
	if dest == nil {
		return fmt.Errorf("unknown target node %s", addr)
	}
	if dest.ID == view.Myself().ID {
		return errors.New("Target instance is myself")
	}

	h := MigrateHeader{Replace: replace, Keys: make([]MigrateItem, 0, len(keys))}
	var body []byte
	for _, k := range keys {
		var exp int64
		if !k.ExpireAt.IsZero() {
			exp = k.ExpireAt.UnixMilli()
		}
		h.Keys = append(h.Keys, MigrateItem{Key: k.Key, ExpireMs: exp, Len: len(k.Value)})
		body = append(body, k.Value...)
	}
	f, err := bus.NewFrame(bus.TypeMigrate, h, body)
	if err != nil {
		return err
	}
	reply, err := c.link(dest.BusAddr()).DoTimeout(f, timeout)
	if err != nil {
		return err
	}
	return reply.Err()
}

// applyMigrated stores an inbound batch. It writes through the normal cache
// path, not the replication one, so the keys are replicated onward to this
// node's own replicas — a migrated slot must be as durable as any other.
func (c *Cluster) applyMigrated(f bus.Frame) bus.Frame {
	var h MigrateHeader
	if err := f.Decode(&h); err != nil {
		return bus.Errorf("bad migrate: %v", err)
	}
	// Validate the whole batch before applying any of it: a length that does
	// not add up would otherwise leave half the keys written and half lost,
	// with the source already free to delete its copies.
	total := 0
	for _, k := range h.Keys {
		if k.Len < 0 {
			return bus.Errorf("bad migrate: negative value length")
		}
		total += k.Len
	}
	if total != len(f.Body) {
		return bus.Errorf("bad migrate: body is %d bytes, keys describe %d", len(f.Body), total)
	}
	if !h.Replace {
		for _, k := range h.Keys {
			if c.store.Exists(k.Key) {
				return bus.Errorf("BUSYKEY Target key name already exists.")
			}
		}
	}

	off := 0
	for _, k := range h.Keys {
		value := f.Body[off : off+k.Len]
		off += k.Len
		var exp time.Time
		if k.ExpireMs > 0 {
			exp = time.UnixMilli(k.ExpireMs)
		}
		if err := c.store.SetAt(k.Key, value, exp); err != nil {
			return bus.Errorf("migrate: %v", err)
		}
	}
	return ack()
}

// maybeMigrateReplica moves this node to a shard that has lost all of its
// replicas, when its own shard has one to spare.
//
// A shard with no replica cannot fail over, so it is one crash away from taking
// its slots offline until an operator intervenes. Redis calls this replica
// migration and it is the reason a cluster survives a second failure without
// anyone being paged. The move is a local decision every replica makes from the
// same gossiped view, so it needs no coordination — but it does need to be
// deterministic, or every spare in the cluster would pile onto one orphan at
// once and strip its own shard bare.
func (c *Cluster) maybeMigrateReplica() {
	if !c.opt.ReplicaMigration {
		return
	}
	view := c.mgr.View()
	me := view.Myself()
	if me.Role != cluster.RoleReplica || me.SlotsCount() > 0 {
		return
	}
	primary := view.PrimaryOf(me)
	if primary == nil || !primary.Healthy() {
		// Our own shard is in trouble; it needs us here, and a failover may be
		// about to make us its primary.
		return
	}
	if !c.isSpareReplica(view, primary, me) {
		return
	}
	orphan := orphanedPrimary(view)
	if orphan == nil {
		return
	}
	slog.Warn("replica migration: covering an orphaned shard",
		"from", primary.Addr(), "to", orphan.Addr(), "slots", orphan.SlotsCount())
	if err := c.Replicate(orphan.ID); err != nil {
		slog.Warn("replica migration failed", "err", err)
	}
}

// isSpareReplica reports whether this node may leave its shard: only when a
// healthy sibling stays behind, and only for one of them. Sorting by node ID
// and taking the second makes both nodes reach the same answer without talking
// to each other — the first stays as cover, the second is the spare.
func (c *Cluster) isSpareReplica(view *cluster.Topology, primary, me *cluster.Node) bool {
	var healthy []string
	for _, r := range view.ReplicasOf(primary) {
		if r.Healthy() {
			healthy = append(healthy, r.ID)
		}
	}
	if len(healthy) < 2 {
		return false
	}
	sort.Strings(healthy)
	return healthy[1] == me.ID
}

// orphanedPrimary is a healthy slot-owning primary with no healthy replica.
// Ties break on node ID so every spare in the cluster picks the same one.
func orphanedPrimary(view *cluster.Topology) *cluster.Node {
	var found *cluster.Node
	for _, p := range view.Primaries() {
		if !p.Healthy() {
			continue
		}
		covered := false
		for _, r := range view.ReplicasOf(p) {
			if r.Healthy() {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		if found == nil || p.ID < found.ID {
			found = p
		}
	}
	return found
}
