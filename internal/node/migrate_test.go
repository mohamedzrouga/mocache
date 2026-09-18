package node

import (
	"strings"
	"testing"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cluster"
	"github.com/mohamedzrouga/mocache/internal/server"
)

// Live slot migration and replica migration over the real bus.

const helloSlot = 866 // cluster.KeySlot("hello")

// The whole reshard dance, as redis-cli --cluster drives it: open the window on
// both nodes, move the data, commit, and let gossip carry the new owner to
// everyone who was not a party to it.
func TestReshardMovesSlotAndDataEndToEnd(t *testing.T) {
	nodes, addrs := threeShardsOneReplica(t)
	a, b, d := nodes["A"], nodes["B"], nodes["D"]
	aID, bID := cluster.NodeID(addrs["A"]), cluster.NodeID(addrs["B"])

	if cluster.KeySlot("hello") != helloSlot {
		t.Fatalf("precondition: slot of \"hello\" is %d", cluster.KeySlot("hello"))
	}
	if err := a.store.Set("hello", []byte("world"), time.Hour); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "replica has the key", func() bool {
		_, _, ok := d.store.Peek("hello")
		return ok
	})

	// 1. Open the window. Ownership does not change yet.
	if err := b.mgr.SetSlotImporting(helloSlot, aID); err != nil {
		t.Fatalf("importing: %v", err)
	}
	if err := a.mgr.SetSlotMigrating(helloSlot, bID); err != nil {
		t.Fatalf("migrating: %v", err)
	}
	if owner := a.mgr.View().OwnerOf(helloSlot); owner == nil || owner.ID != aID {
		t.Fatal("marking the slot gave it away before the data moved")
	}

	// 2. Move the data.
	value, expiry, ok := a.store.Peek("hello")
	if !ok {
		t.Fatal("key vanished before the migration")
	}
	entry := []server.MigratedKey{{Key: "hello", Value: value, ExpireAt: expiry}}
	if err := a.cl.MigrateKeys(addrs["B"], entry, false, 2*time.Second); err != nil {
		t.Fatalf("MigrateKeys: %v", err)
	}
	a.store.Delete("hello")

	got, gotExpiry, ok := b.store.Peek("hello")
	if !ok || string(got) != "world" {
		t.Fatalf("destination has %q (present: %v)", got, ok)
	}
	// The absolute instant, not a restarted duration.
	if gotExpiry.IsZero() || gotExpiry.Sub(expiry).Abs() > time.Second {
		t.Fatalf("expiry arrived as %v, want %v", gotExpiry, expiry)
	}

	// 3. Commit on both sides.
	if err := b.mgr.SetSlotOwner(helloSlot, bID); err != nil {
		t.Fatalf("commit on destination: %v", err)
	}
	if err := a.mgr.SetSlotOwner(helloSlot, bID); err != nil {
		t.Fatalf("commit on source: %v", err)
	}

	// Everyone converges, including the nodes that were never told.
	waitFor(t, 10*time.Second, "every node agrees on the new owner", func() bool {
		for _, n := range nodes {
			owner := n.mgr.View().OwnerOf(helloSlot)
			if owner == nil || owner.ID != bID {
				return false
			}
		}
		return true
	})

	// The source keeps the rest of its shard: a reshard moves one slot, not a
	// whole keyspace.
	if got := a.slots(); got != 5460 {
		t.Fatalf("source holds %d slots after the reshard, want 5460", got)
	}
	if a.role() != cluster.RolePrimary {
		t.Fatalf("source was demoted by a single-slot move: %s", a.role())
	}
	if got := b.slots(); got != 5463 {
		t.Fatalf("destination holds %d slots, want 5463", got)
	}
	// Coverage must still be exact or cluster clients reject the topology.
	total := 0
	for _, p := range a.mgr.View().Primaries() {
		total += p.SlotsCount()
	}
	if total != cluster.SlotCount {
		t.Fatalf("advertised coverage %d, want %d", total, cluster.SlotCount)
	}
}

func TestMigrateRefusesToOverwriteWithoutReplace(t *testing.T) {
	nodes, addrs := threeShardsOneReplica(t)
	a, b := nodes["A"], nodes["B"]

	if err := b.store.Set("hello", []byte("theirs"), 0); err != nil {
		t.Fatal(err)
	}
	entry := []server.MigratedKey{{Key: "hello", Value: []byte("ours")}}

	err := a.cl.MigrateKeys(addrs["B"], entry, false, 2*time.Second)
	if err == nil || !strings.HasPrefix(err.Error(), "BUSYKEY") {
		t.Fatalf("overwrote an existing key without REPLACE: err=%v", err)
	}
	if v, _, _ := b.store.Peek("hello"); string(v) != "theirs" {
		t.Fatalf("destination value changed to %q despite the refusal", v)
	}

	if err := a.cl.MigrateKeys(addrs["B"], entry, true, 2*time.Second); err != nil {
		t.Fatalf("MIGRATE REPLACE: %v", err)
	}
	if v, _, _ := b.store.Peek("hello"); string(v) != "ours" {
		t.Fatalf("REPLACE left %q", v)
	}
}

// A batch is all or nothing: the source deletes its copies once the call
// returns, so a partially applied batch would lose keys outright.
func TestMigrateBatchIsRejectedWholesale(t *testing.T) {
	nodes, addrs := threeShardsOneReplica(t)
	a, b := nodes["A"], nodes["B"]

	if err := b.store.Set("second", []byte("theirs"), 0); err != nil {
		t.Fatal(err)
	}
	batch := []server.MigratedKey{
		{Key: "first", Value: []byte("a")},
		{Key: "second", Value: []byte("b")}, // collides
	}
	if err := a.cl.MigrateKeys(addrs["B"], batch, false, 2*time.Second); err == nil {
		t.Fatal("a colliding batch was accepted")
	}
	if b.store.Exists("first") {
		t.Fatal("half the batch was applied before the collision was found")
	}
}

// A shard with no replica cannot fail over. When another shard has a spare,
// that spare moves to cover it — and leaves cover behind, so the move never
// creates the problem it is solving.
func TestReplicaMigrationCoversOrphanedShard(t *testing.T) {
	addrs := map[string]string{
		"A": freeAddr(t), "B": freeAddr(t), "C": freeAddr(t),
		"D": freeAddr(t), "E": freeAddr(t),
	}
	specs := []string{
		addrs["A"] + "=0-5460",
		addrs["B"] + "=5461-10922",
		addrs["C"] + "=10923-16383",
		addrs["D"] + "=replica-of:" + addrs["A"],
		addrs["E"] + "=replica-of:" + addrs["A"],
	}
	nodes := startClusterWith(t, []string{"A", "B", "C", "D", "E"}, specs, addrs,
		func(o *Options) { o.ReplicaMigration = true })

	followerOf := func(n *testNode) string {
		v := n.mgr.View()
		p := v.PrimaryOf(v.Myself())
		if p == nil {
			return ""
		}
		return p.Addr()
	}

	waitFor(t, 15*time.Second, "a spare replica covered an orphaned shard", func() bool {
		return followerOf(nodes["D"]) != addrs["A"] || followerOf(nodes["E"]) != addrs["A"]
	})

	// Exactly one moved: the shard it left still has cover, which is the whole
	// point of only ever releasing a spare.
	stayed := 0
	for _, name := range []string{"D", "E"} {
		if followerOf(nodes[name]) == addrs["A"] {
			stayed++
		}
	}
	if stayed != 1 {
		t.Fatalf("%d of A's two replicas stayed behind, want exactly 1", stayed)
	}

	// And the shard it moved to is one that had none.
	moved := nodes["D"]
	if followerOf(moved) == addrs["A"] {
		moved = nodes["E"]
	}
	target := followerOf(moved)
	if target != addrs["B"] && target != addrs["C"] {
		t.Fatalf("spare moved to %s, which was not an orphaned shard", target)
	}

	// The choice has to reach the other nodes, or a voter would refuse to vote
	// for this replica when its new primary dies.
	waitFor(t, 10*time.Second, "the new follow target reaches the cluster", func() bool {
		for _, n := range nodes {
			for _, p := range n.mgr.View().Nodes() {
				if p.Addr() == moved.addr && p.PrimaryO != target {
					return false
				}
			}
		}
		return true
	})
}
