package cluster

import (
	"testing"
	"time"
)

// The voting rules decide whether two nodes can end up owning one slot, so they
// are tested here directly — no network, no timing, just the state machine.

func shardedCluster(t *testing.T, me string) *Manager {
	t.Helper()
	m, err := NewManager([]string{
		"10.0.0.1:6379=0-5460",
		"10.0.0.2:6379=5461-10922",
		"10.0.0.3:6379=10923-16383",
		"10.0.0.4:6379=replica-of:10.0.0.1:6379",
	}, me)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	return m
}

func idOf(addr string) string { return NodeID(addr) }

func TestVoteRequiresFailedPrimary(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379") // a voter
	req := VoteRequest{
		Epoch:     1,
		NodeID:    idOf("10.0.0.4:6379"),
		PrimaryID: idOf("10.0.0.1:6379"),
	}
	if reply := m.ConsiderVote(req); reply.Granted {
		t.Fatal("granted a vote while the primary is healthy")
	}
	// FORCE is the manual path: an operator has taken responsibility.
	req.Force = true
	req.Epoch = 2
	if reply := m.ConsiderVote(req); !reply.Granted {
		t.Fatalf("refused a forced vote: %s", reply.Reason)
	}
}

func TestOneVotePerEpoch(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	m.MarkFail(idOf("10.0.0.1:6379"))

	first := m.ConsiderVote(VoteRequest{Epoch: 5, NodeID: idOf("10.0.0.4:6379"), PrimaryID: idOf("10.0.0.1:6379")})
	if !first.Granted {
		t.Fatalf("first vote refused: %s", first.Reason)
	}
	// A second candidate in the same epoch must be refused, or both could
	// reach a majority and claim the same slots.
	second := m.ConsiderVote(VoteRequest{Epoch: 5, NodeID: idOf("10.0.0.3:6379"), PrimaryID: idOf("10.0.0.1:6379")})
	if second.Granted {
		t.Fatal("granted two votes in one epoch")
	}
	// The same candidate in a later epoch is fine: that is a fresh election.
	third := m.ConsiderVote(VoteRequest{Epoch: 6, NodeID: idOf("10.0.0.4:6379"), PrimaryID: idOf("10.0.0.1:6379")})
	if !third.Granted {
		t.Fatalf("refused a vote in a new epoch: %s", third.Reason)
	}
}

func TestOnlyPrimariesVote(t *testing.T) {
	m := shardedCluster(t, "10.0.0.4:6379") // the replica itself
	m.MarkFail(idOf("10.0.0.1:6379"))
	reply := m.ConsiderVote(VoteRequest{Epoch: 1, NodeID: idOf("10.0.0.4:6379"), PrimaryID: idOf("10.0.0.1:6379")})
	if reply.Granted {
		t.Fatal("a replica cast a vote")
	}
}

func TestVoteRefusedForUnrelatedCandidate(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	m.MarkFail(idOf("10.0.0.1:6379"))
	// Node 3 is a primary of its own shard, not a replica of node 1.
	reply := m.ConsiderVote(VoteRequest{Epoch: 1, NodeID: idOf("10.0.0.3:6379"), PrimaryID: idOf("10.0.0.1:6379")})
	if reply.Granted {
		t.Fatalf("granted a vote to a node that does not replicate that primary")
	}
}

func TestPromoteTransfersSlotsAndDemotesOldPrimary(t *testing.T) {
	m := shardedCluster(t, "10.0.0.4:6379")
	replica, primary := idOf("10.0.0.4:6379"), idOf("10.0.0.1:6379")

	if !m.Promote(replica, 1) {
		t.Fatal("promotion rejected")
	}
	v := m.View()
	me := v.Myself()
	if me.Role != RolePrimary || me.SlotsCount() != 5461 {
		t.Fatalf("promoted node owns %d slots as %s", me.SlotsCount(), me.Role)
	}
	if owner := v.OwnerOf(0); owner == nil || owner.ID != replica {
		t.Fatal("slot 0 did not move to the promoted node")
	}
	old := v.Nodes()
	for _, n := range old {
		if n.ID == primary {
			if n.Role != RoleReplica || n.SlotsCount() != 0 {
				t.Fatalf("old primary kept its slots: role=%s slots=%d", n.Role, n.SlotsCount())
			}
			if n.PrimaryO != "10.0.0.4:6379" {
				t.Fatalf("old primary does not follow the winner: %q", n.PrimaryO)
			}
		}
	}
	// Coverage must remain complete, or clients would see CLUSTERDOWN.
	if v.AssignedSlots() != SlotCount || v.State() != "ok" {
		t.Fatalf("after failover: %d slots assigned, state %s", v.AssignedSlots(), v.State())
	}
	// A stale claim at the same epoch must not be applied.
	if m.Promote(idOf("10.0.0.2:6379"), 1) {
		t.Fatal("applied a claim that did not raise the config epoch")
	}
}

// A primary that returns after being replaced must stand down as soon as it
// sees a higher config epoch for its slots.
func TestSelfDemotionOnHigherEpoch(t *testing.T) {
	m := shardedCluster(t, "10.0.0.1:6379") // the old primary, just restarted
	if m.View().Myself().Role != RolePrimary {
		t.Fatal("should start as a primary")
	}
	// Gossip arrives saying node 4 owns 0-5460 at config epoch 1.
	m.MergeGossip(idOf("10.0.0.2:6379"), 1, []GossipNode{{
		ID:          idOf("10.0.0.4:6379"),
		Host:        "10.0.0.4",
		Port:        6379,
		Role:        byte(RolePrimary),
		Slots:       []SlotRange{{Start: 0, End: 5460}},
		ConfigEpoch: 1,
	}})
	me := m.View().Myself()
	if me.Role != RoleReplica || me.SlotsCount() != 0 {
		t.Fatalf("returning primary kept its slots: role=%s slots=%d", me.Role, me.SlotsCount())
	}
	if me.PrimaryO != "10.0.0.4:6379" {
		t.Fatalf("returning primary follows %q", me.PrimaryO)
	}
}

func TestFailureNeedsMajorityOfPrimaries(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	target := idOf("10.0.0.1:6379")
	replica := idOf("10.0.0.4:6379")
	third := idOf("10.0.0.3:6379")

	m.NoteUnreachable(target, 0) // our own suspicion: 1 of 3 primaries
	if m.ShouldFail(target, time.Hour) {
		t.Fatal("declared failure on one primary's opinion")
	}
	// A replica agreeing is not enough: replicas do not get a vote.
	m.MergeGossip(replica, 0, []GossipNode{{ID: target, Host: "10.0.0.1", Port: 6379, PFail: true}})
	if m.ShouldFail(target, time.Hour) {
		t.Fatal("counted a replica's report toward the majority")
	}
	// A second primary agreeing makes it 2 of 3.
	m.MergeGossip(third, 0, []GossipNode{{ID: target, Host: "10.0.0.1", Port: 6379, PFail: true}})
	if !m.ShouldFail(target, time.Hour) {
		t.Fatal("a majority of primaries agreed and it still was not enough")
	}
}

// Regression: a node that missed the promotion broadcast used to keep the old
// primary's slot claim forever, because gossip only adopted *higher* config
// epochs and nothing ever voided the stale one. The cluster then advertised two
// owners for the same slots — CLUSTER SLOTS summed to more than 16384, and
// go-redis refused the topology.
func TestStaleSlotClaimIsVoidedByGossip(t *testing.T) {
	m := shardedCluster(t, "10.0.0.2:6379")
	oldPrimary, promoted := idOf("10.0.0.1:6379"), idOf("10.0.0.4:6379")

	// This node never saw the promotion: it still believes node 1 owns 0-5460.
	if owner := m.View().OwnerOf(0); owner == nil || owner.ID != oldPrimary {
		t.Fatal("precondition: node 1 should own slot 0")
	}

	// Gossip arrives describing node 4 as the owner at a higher epoch, while
	// node 1 still advertises itself as a primary at its original epoch.
	m.MergeGossip(idOf("10.0.0.3:6379"), 1, []GossipNode{
		{ID: promoted, Host: "10.0.0.4", Port: 6379, Role: byte(RolePrimary),
			Slots: []SlotRange{{Start: 0, End: 5460}}, ConfigEpoch: 1},
		{ID: oldPrimary, Host: "10.0.0.1", Port: 6379, Role: byte(RolePrimary),
			Slots: []SlotRange{{Start: 0, End: 5460}}, ConfigEpoch: 0},
	})

	v := m.View()
	owners := 0
	for _, n := range v.Nodes() {
		if n.Role == RolePrimary && n.SlotsCount() > 0 {
			owners++
		}
	}
	if owners != 3 {
		t.Fatalf("%d slot-owning primaries after convergence, want 3", owners)
	}
	if owner := v.OwnerOf(0); owner == nil || owner.ID != promoted {
		t.Fatal("slot 0 did not move to the higher config epoch")
	}
	// The total advertised coverage is what a cluster client validates.
	total := 0
	for _, n := range v.Primaries() {
		total += n.SlotsCount()
	}
	if total != SlotCount {
		t.Fatalf("advertised slot coverage is %d, want %d", total, SlotCount)
	}
	for _, n := range v.Nodes() {
		if n.ID == oldPrimary && (n.Role != RoleReplica || n.SlotsCount() != 0) {
			t.Fatalf("stale claim survived: role=%s slots=%d", n.Role, n.SlotsCount())
		}
	}
}

// A node that restarts after being promoted has no memory of it: its flags
// still say "replica-of". The cluster remembers, and gossip is the only thing
// that can tell it. Before this was handled, the node disowned slots every
// other node redirected clients to, and the two views never reconciled — gossip
// about ourselves was discarded wholesale.
func TestAdoptsOwnPromotionAfterRestart(t *testing.T) {
	m := shardedCluster(t, "10.0.0.4:6379") // the promoted replica, just restarted
	if m.View().Myself().Role != RoleReplica {
		t.Fatal("precondition: should start from its configured role")
	}

	m.MergeGossip(idOf("10.0.0.2:6379"), 1, []GossipNode{{
		ID: idOf("10.0.0.4:6379"), Host: "10.0.0.4", Port: 6379,
		Role:  byte(RolePrimary),
		Slots: []SlotRange{{Start: 0, End: 5460}}, ConfigEpoch: 1,
	}})

	me := m.View().Myself()
	if me.Role != RolePrimary || me.SlotsCount() != 5461 {
		t.Fatalf("did not take back its own promotion: role=%s slots=%d", me.Role, me.SlotsCount())
	}
	if me.ConfigEpoch != 1 {
		t.Fatalf("config epoch %d, want the 1 it was elected at", me.ConfigEpoch)
	}
	// The old primary's claim on those slots is void, or coverage would exceed
	// 16384 and cluster clients would reject the topology.
	total := 0
	for _, n := range m.View().Primaries() {
		total += n.SlotsCount()
	}
	if total != SlotCount {
		t.Fatalf("advertised coverage %d, want %d", total, SlotCount)
	}
}

// The flip side: a claim about ourselves that we have already lost must not be
// resurrected by a node still gossiping it. Only the highest epoch may win, and
// that rule does not bend because the claim happens to be about us.
func TestDoesNotAdoptSupersededSelfClaim(t *testing.T) {
	m := shardedCluster(t, "10.0.0.4:6379")

	// Someone else already holds 0-5460 at epoch 2.
	m.MergeGossip(idOf("10.0.0.2:6379"), 2, []GossipNode{{
		ID: idOf("10.0.0.3:6379"), Host: "10.0.0.3", Port: 6379,
		Role:  byte(RolePrimary),
		Slots: []SlotRange{{Start: 0, End: 5460}}, ConfigEpoch: 2,
	}})

	// A straggler still describes us as the owner at epoch 1.
	m.MergeGossip(idOf("10.0.0.2:6379"), 2, []GossipNode{{
		ID: idOf("10.0.0.4:6379"), Host: "10.0.0.4", Port: 6379,
		Role:  byte(RolePrimary),
		Slots: []SlotRange{{Start: 0, End: 5460}}, ConfigEpoch: 1,
	}})

	if me := m.View().Myself(); me.Role == RolePrimary && me.SlotsCount() > 0 {
		t.Fatalf("resurrected a claim already lost at a higher epoch: slots=%d epoch=%d",
			me.SlotsCount(), me.ConfigEpoch)
	}
	if owner := m.View().OwnerOf(0); owner == nil || owner.ID != idOf("10.0.0.3:6379") {
		t.Fatal("slot 0 left the highest config epoch")
	}
}
