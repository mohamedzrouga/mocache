package cluster

import (
	"reflect"
	"testing"
)

func TestSlotSetRoundTrip(t *testing.T) {
	in := []SlotRange{{Start: 0, End: 10}, {Start: 12, End: 12}, {Start: 100, End: 200}}
	if got := setOf(in).ranges(); !reflect.DeepEqual(got, in) {
		t.Fatalf("round trip gave %v, want %v", got, in)
	}
	// Adjacent ranges merge; that is the form a node advertises.
	merged := setOf([]SlotRange{{Start: 0, End: 5}, {Start: 6, End: 9}}).ranges()
	if want := []SlotRange{{Start: 0, End: 9}}; !reflect.DeepEqual(merged, want) {
		t.Fatalf("merge gave %v, want %v", merged, want)
	}
	// Removing from the middle splits, which is exactly what one migrated slot
	// does to a contiguous range.
	split := removeSlot([]SlotRange{{Start: 0, End: 9}}, 4)
	if want := []SlotRange{{Start: 0, End: 3}, {Start: 5, End: 9}}; !reflect.DeepEqual(split, want) {
		t.Fatalf("split gave %v, want %v", split, want)
	}
	if got := addSlot(split, 4); !reflect.DeepEqual(got, []SlotRange{{Start: 0, End: 9}}) {
		t.Fatalf("re-adding the slot gave %v", got)
	}
	// Claims outside the keyspace are dropped rather than wrapping into it.
	if n := setOf([]SlotRange{{Start: -5, End: 2}, {Start: SlotCount - 1, End: SlotCount + 9}}).count(); n != 4 {
		t.Fatalf("out-of-range claim contributed %d slots, want 4", n)
	}
}

func TestSetSlotMigratingRequiresOwnership(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379") // owns 5461-10922
	dest := idOf("10.0.0.3:6379")

	if err := m.SetSlotMigrating(0, dest); err == nil {
		t.Fatal("migrated a slot this node does not own")
	}
	if err := m.SetSlotMigrating(6000, "nonexistent"); err == nil {
		t.Fatal("migrated a slot to a node we do not know")
	}
	if err := m.SetSlotMigrating(6000, dest); err != nil {
		t.Fatalf("SETSLOT MIGRATING: %v", err)
	}
	if got := m.View().Migrating(6000); got != dest {
		t.Fatalf("migrating marker = %q", got)
	}
	// The marker changes nothing about ownership — that is the whole point.
	if owner := m.View().OwnerOf(6000); owner == nil || owner.ID != m.View().Myself().ID {
		t.Fatal("marking a slot migrating gave it away early")
	}

	if err := m.SetSlotStable(6000); err != nil {
		t.Fatalf("SETSLOT STABLE: %v", err)
	}
	if got := m.View().Migrating(6000); got != "" {
		t.Fatalf("STABLE left a marker: %q", got)
	}
}

func TestSetSlotImportingRefusedForOwnedSlot(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	if err := m.SetSlotImporting(6000, idOf("10.0.0.1:6379")); err == nil {
		t.Fatal("imported a slot this node already owns")
	}
	if err := m.SetSlotImporting(0, idOf("10.0.0.1:6379")); err != nil {
		t.Fatalf("SETSLOT IMPORTING: %v", err)
	}
	if got := m.View().Importing(0); got != idOf("10.0.0.1:6379") {
		t.Fatalf("importing marker = %q", got)
	}
	// Importing does not make us the owner either; only ASKING clients reach us.
	if owner := m.View().OwnerOf(0); owner == nil || owner.ID != idOf("10.0.0.1:6379") {
		t.Fatal("importing a slot took ownership of it early")
	}
}

// One slot moves; the source keeps everything else. Before slot claims were set
// arithmetic, taking a single slot voided the old owner's entire claim and
// demoted it to a replica — a reshard would have taken 5460 slots offline.
func TestSetSlotNodeMovesOneSlotOnly(t *testing.T) {
	dest := idOf("10.0.0.2:6379")
	m := shardedCluster(t, "10.0.0.2:6379") // the importing node
	if err := m.SetSlotImporting(100, idOf("10.0.0.1:6379")); err != nil {
		t.Fatalf("importing: %v", err)
	}
	before := m.View().Myself().ConfigEpoch

	if err := m.SetSlotOwner(100, dest); err != nil {
		t.Fatalf("SETSLOT NODE: %v", err)
	}
	v := m.View()
	if owner := v.OwnerOf(100); owner == nil || owner.ID != dest {
		t.Fatal("slot 100 did not move")
	}
	if v.Importing(100) != "" || v.Migrating(100) != "" {
		t.Fatal("committing the move left a marker behind")
	}
	// Taking a slot has to outrank every claim on it, or gossip would carry the
	// old owner's version straight back.
	if v.Myself().ConfigEpoch <= before {
		t.Fatalf("config epoch did not rise: %d", v.Myself().ConfigEpoch)
	}

	src := nodeWithID(v, idOf("10.0.0.1:6379"))
	if src.Role != RolePrimary {
		t.Fatalf("source was demoted by a single-slot move: %s", src.Role)
	}
	if src.SlotsCount() != 5460 {
		t.Fatalf("source holds %d slots, want 5460 (it started with 5461)", src.SlotsCount())
	}
	if src.OwnsSlot(100) {
		t.Fatal("source still claims the migrated slot")
	}
	// Coverage is what a cluster client validates, and it must still be exact.
	total := 0
	for _, n := range v.Primaries() {
		total += n.SlotsCount()
	}
	if total != SlotCount {
		t.Fatalf("advertised coverage %d, want %d", total, SlotCount)
	}
}

// A third node learns about the move the same way it learns about a failover:
// a higher config epoch in gossip. It must subtract exactly the slot that moved.
func TestReshardPropagatesByConfigEpoch(t *testing.T) {
	observer := shardedCluster(t, "10.0.0.3:6379")
	dest := idOf("10.0.0.2:6379")

	observer.MergeGossip(dest, 0, []GossipNode{{
		ID: dest, Host: "10.0.0.2", Port: 6379, Role: byte(RolePrimary),
		// 5461-10922 as before, plus slot 100 taken from node 1.
		Slots: []SlotRange{{Start: 100, End: 100}, {Start: 5461, End: 10922}},
		// One above the source's, which is 0 here.
		ConfigEpoch: 1,
	}})

	v := observer.View()
	if owner := v.OwnerOf(100); owner == nil || owner.ID != dest {
		t.Fatal("observer did not move slot 100 to its new owner")
	}
	src := nodeWithID(v, idOf("10.0.0.1:6379"))
	if src.Role != RolePrimary || src.SlotsCount() != 5460 {
		t.Fatalf("observer stripped the source: role=%s slots=%d", src.Role, src.SlotsCount())
	}
	if owner := v.OwnerOf(0); owner == nil || owner.ID != src.ID {
		t.Fatal("observer moved slots the reshard never touched")
	}
}

// The source of a reshard applies the same subtraction to itself when it sees
// the destination's higher epoch, instead of standing down wholesale.
func TestReshardSourceKeepsRemainingSlots(t *testing.T) {
	src := shardedCluster(t, "10.0.0.1:6379") // owns 0-5460
	dest := idOf("10.0.0.2:6379")

	src.MergeGossip(dest, 0, []GossipNode{{
		ID: dest, Host: "10.0.0.2", Port: 6379, Role: byte(RolePrimary),
		Slots:       []SlotRange{{Start: 100, End: 100}, {Start: 5461, End: 10922}},
		ConfigEpoch: 1,
	}})

	me := src.View().Myself()
	if me.Role != RolePrimary {
		t.Fatalf("source demoted itself over one slot: %s", me.Role)
	}
	if me.SlotsCount() != 5460 || me.OwnsSlot(100) {
		t.Fatalf("source kept %d slots (owns 100: %v)", me.SlotsCount(), me.OwnsSlot(100))
	}
}

// Losing the *last* slot is still a demotion: that is a failover, and the node
// has to start following whoever took the shard.
func TestLosingEverySlotStillDemotes(t *testing.T) {
	src := shardedCluster(t, "10.0.0.1:6379")
	winner := idOf("10.0.0.4:6379")

	src.MergeGossip(idOf("10.0.0.2:6379"), 1, []GossipNode{{
		ID: winner, Host: "10.0.0.4", Port: 6379, Role: byte(RolePrimary),
		Slots: []SlotRange{{Start: 0, End: 5460}}, ConfigEpoch: 1,
	}})

	me := src.View().Myself()
	if me.Role != RoleReplica || me.SlotsCount() != 0 {
		t.Fatalf("role=%s slots=%d, want a replica with none", me.Role, me.SlotsCount())
	}
	if me.PrimaryO != "10.0.0.4:6379" {
		t.Fatalf("follows %q", me.PrimaryO)
	}
}

func TestBumpEpochOutranksEveryKnownClaim(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	m.MergeGossip(idOf("10.0.0.3:6379"), 0, []GossipNode{{
		ID: idOf("10.0.0.3:6379"), Host: "10.0.0.3", Port: 6379, Role: byte(RolePrimary),
		Slots: []SlotRange{{Start: 10923, End: 16383}}, ConfigEpoch: 41,
	}})
	if got := m.BumpConfigEpoch(); got != 42 {
		t.Fatalf("bumped to %d, want one above the highest known claim", got)
	}
	if got := m.View().Myself().ConfigEpoch; got != 42 {
		t.Fatalf("own config epoch %d after bump", got)
	}
}

func nodeWithID(v *Topology, id string) *Node {
	for _, n := range v.Nodes() {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// A replica that changes primary — CLUSTER REPLICATE, or replica migration
// covering an orphaned shard — has to be believed, because that choice carries
// no config epoch and so cannot travel by the "highest epoch wins" rule. A
// voter that still thinks a candidate replicates someone else refuses its vote,
// which would leave the shard it just moved to unable to fail over.
func TestReplicaSelfDeclaredPrimaryPropagates(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	replica := idOf("10.0.0.4:6379")

	m.MergeGossip(replica, 0, []GossipNode{{
		ID: replica, Host: "10.0.0.4", Port: 6379,
		Role: byte(RoleReplica), PrimaryAddr: "10.0.0.3:6379",
	}})
	if got := nodeWithID(m.View(), replica).PrimaryO; got != "10.0.0.3:6379" {
		t.Fatalf("replica still follows %q", got)
	}

	// Only the node's own word: a third party relaying a stale copy must not
	// flip it back.
	m.MergeGossip(idOf("10.0.0.1:6379"), 0, []GossipNode{{
		ID: replica, Host: "10.0.0.4", Port: 6379,
		Role: byte(RoleReplica), PrimaryAddr: "10.0.0.1:6379",
	}})
	if got := nodeWithID(m.View(), replica).PrimaryO; got != "10.0.0.3:6379" {
		t.Fatalf("a relayed claim overrode the node's own: %q", got)
	}
}

// The same rule must not undo a promotion: a node that restarted and forgot it
// was promoted gossips "I am a replica", and the cluster has to keep holding
// its claim until it takes it back.
func TestSelfDeclaredReplicaCannotVoidItsOwnPromotion(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	promoted := idOf("10.0.0.4:6379")
	if !m.Promote(promoted, 1) {
		t.Fatal("setup: promotion rejected")
	}

	m.MergeGossip(promoted, 0, []GossipNode{{
		ID: promoted, Host: "10.0.0.4", Port: 6379,
		Role: byte(RoleReplica), PrimaryAddr: "10.0.0.1:6379",
	}})

	n := nodeWithID(m.View(), promoted)
	if n.Role != RolePrimary || n.SlotsCount() == 0 {
		t.Fatalf("a restarted node's amnesia erased its promotion: role=%s slots=%d", n.Role, n.SlotsCount())
	}
}
