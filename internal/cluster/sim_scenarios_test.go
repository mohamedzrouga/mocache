package cluster

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// Scenarios for the deterministic simulation in sim_test.go. Each one names the
// property it is there to defend; the harness checks the safety invariants after
// every event regardless, so a scenario that exercises a new schedule is worth
// adding even when its own assertions are weak.

// A cluster nobody disturbs must not invent a failover. Failure detectors that
// are too eager show up here first: gossip timing, not a fault, would be the
// only thing that could promote anyone.
func TestSimQuietClusterNeverFailsOver(t *testing.T) {
	for seed := int64(0); seed < seeds(8); seed++ {
		s := newSim(t, seed, simOptions{})
		s.run(30 * time.Second)
		if len(s.promos) != 0 {
			s.fail("promoted with no fault injected: %v", s.promos)
		}
		if ok, why := s.converged(); !ok {
			s.fail("quiet cluster did not agree on the slot map: %s", why)
		}
	}
}

// One primary dies; exactly one replica takes its slots, and everyone ends up
// agreeing who that was.
func TestSimPrimaryCrashPromotesExactlyOneReplica(t *testing.T) {
	for seed := int64(0); seed < seeds(8); seed++ {
		s := newSim(t, seed, simOptions{})
		s.at(2*time.Second, func() { s.crash("10.0.0.1:6379") })
		s.runUntilStable(90 * time.Second)
		if s.failed {
			continue
		}
		if len(s.promos) != 1 {
			s.fail("want exactly one promotion, got %d: %v", len(s.promos), s.promos)
			continue
		}
		if owner := s.ownerOf(0); owner != "10.0.0.4" {
			s.fail("slot 0 owned by %s, want the dead primary's replica", owner)
		}
	}
}

// A partitioned primary keeps serving its own side until it is told otherwise —
// that is the trade a cache makes — but the moment the partition heals it must
// see the higher config epoch and stand down. Two owners at one epoch, or a
// primary that never stands down, both fail here.
func TestSimIsolatedPrimaryStandsDownAfterHeal(t *testing.T) {
	for seed := int64(0); seed < seeds(8); seed++ {
		s := newSim(t, seed, simOptions{})
		s.at(2*time.Second, func() { s.partition([]string{"10.0.0.1:6379"}) })
		s.run(40 * time.Second)
		if s.failed {
			continue
		}
		if len(s.promos) != 1 {
			s.fail("majority side did not promote: %v", s.promos)
			continue
		}
		// While partitioned, the old primary still believes it owns its slots.
		// That is expected: safety says the two cannot share a config epoch.
		if me := s.byID["10.0.0.1:6379"].mgr.View().Myself(); me.Role != RolePrimary {
			s.fail("isolated primary stood down while still partitioned")
		}

		s.at(s.now.Sub(s.start), func() { s.heal() })
		s.runUntilStable(90 * time.Second)
		if s.failed {
			continue
		}
		if me := s.byID["10.0.0.1:6379"].mgr.View().Myself(); me.Role != RoleReplica || me.SlotsCount() != 0 {
			s.fail("returning primary kept its slots: role=%s slots=%d", me.Role, me.SlotsCount())
		}
	}
}

// A candidate that can only reach a minority of primaries must not promote,
// however sure it is. FORCE is used to get the election to actually run: it
// skips the "primary is marked failed" precondition, which a minority side
// cannot satisfy on its own, and leaves the vote counting as the only thing
// standing between this node and a second owner for slots 0-5460.
//
// The other side of the cut does its own, legitimate work — .2 is isolated
// there too, so .5 fails over for it — which is why the assertion is about the
// candidate rather than about the cluster having been quiet.
func TestSimMinorityCannotPromote(t *testing.T) {
	for seed := int64(0); seed < seeds(8); seed++ {
		s := newSim(t, seed, simOptions{})
		s.at(2*time.Second, func() {
			// .4 (replica of .1) can reach only .2 — one vote of the two needed.
			s.partition([]string{"10.0.0.4:6379", "10.0.0.2:6379"})
		})
		s.at(4*time.Second, func() {
			n := s.byID["10.0.0.4:6379"]
			primary := n.mgr.View().PrimaryOf(n.mgr.View().Myself())
			n.runElection(primary.ID, true, n.gen)
		})
		s.run(30 * time.Second)
		if s.failed {
			continue
		}
		candidate := s.byID["10.0.0.4:6379"]
		for epoch, winner := range s.promos {
			if winner == candidate.id {
				s.fail("a minority candidate promoted itself at epoch %d", epoch)
			}
		}
		if me := candidate.mgr.View().Myself(); me.Role != RoleReplica || me.SlotsCount() != 0 {
			s.fail("candidate took slots without a majority: role=%s slots=%d", me.Role, me.SlotsCount())
		}
		// .1 never failed and is on the majority side: its slots must not have
		// moved at all.
		if owner := s.ownerOf(0); owner != "10.0.0.1" {
			s.fail("slot 0 moved to %s while its primary was healthy", owner)
		}
	}
}

// Clocks are never synchronised. Every deadline in the Manager is a local
// duration, so a node that thinks it is an hour ahead must still detect failure
// and vote the same way — if any comparison ever crosses nodes, this is what
// catches it.
func TestSimClockSkewDoesNotBreakFailureDetection(t *testing.T) {
	for seed := int64(0); seed < seeds(8); seed++ {
		s := newSim(t, seed, simOptions{maxSkew: 45 * time.Minute})
		s.at(2*time.Second, func() { s.crash("10.0.0.2:6379") })
		s.runUntilStable(90 * time.Second)
		if s.failed {
			continue
		}
		if len(s.promos) != 1 {
			s.fail("skewed clocks changed the outcome: %d promotions %v", len(s.promos), s.promos)
			continue
		}
		if owner := s.ownerOf(6000); owner != "10.0.0.5" {
			s.fail("slot 6000 owned by %s, want the dead primary's replica", owner)
		}
	}
}

// Every shard loses its primary, one after another. Each promotion raises the
// config epoch, and the invariant that matters is that the keyspace still has
// exactly one owner per slot at the end.
func TestSimSuccessiveFailoversKeepOneOwnerPerSlot(t *testing.T) {
	for seed := int64(0); seed < seeds(6); seed++ {
		s := newSim(t, seed, simOptions{})
		s.at(2*time.Second, func() { s.crash("10.0.0.1:6379") })
		s.at(30*time.Second, func() { s.crash("10.0.0.2:6379") })
		s.at(60*time.Second, func() { s.crash("10.0.0.3:6379") })
		s.runUntilStable(180 * time.Second)
		if s.failed {
			continue
		}
		if len(s.promos) != 3 {
			s.fail("want three promotions, got %d: %v", len(s.promos), s.promos)
			continue
		}
		for slot, want := range map[int]string{0: "10.0.0.4", 6000: "10.0.0.5", 12000: "10.0.0.6"} {
			if got := s.ownerOf(slot); got != want {
				s.fail("slot %d owned by %s, want %s", slot, got, want)
			}
		}
	}
}

// The reason this harness exists: messages that are delayed and reordered rather
// than dropped, under partitions that form and heal while other messages are in
// flight. No assertion here beyond the safety invariants the harness checks
// after every event — the schedules are the test.
func TestSimDelayedReorderedAndPartitionedNetwork(t *testing.T) {
	for seed := int64(0); seed < seeds(40); seed++ {
		s := newSim(t, seed, simOptions{
			// A spread this wide means a reply routinely overtakes one sent
			// several rounds earlier, so nothing may depend on arrival order.
			minLatency: time.Millisecond,
			maxLatency: 900 * time.Millisecond,
			loss:       0.15,
			maxSkew:    5 * time.Second,
		})
		s.randomFaults(2*time.Second, 6*time.Second, 6)
		s.run(120 * time.Second)
		if s.failed {
			continue
		}
		// Heal everything and let it settle: safety is easy to keep by doing
		// nothing, so the run only counts if the cluster also recovers.
		s.at(s.now.Sub(s.start), func() {
			s.heal()
			for _, n := range s.nodes {
				if !n.up {
					s.restart(n.addr)
				}
			}
		})
		s.runUntilStable(180 * time.Second)
	}
}

// --- helpers ---------------------------------------------------------------

// ownerOf is the slot's owner as the first live node sees it.
func (s *sim) ownerOf(slot int) string {
	for _, n := range s.nodes {
		if !n.up {
			continue
		}
		if o := n.mgr.View().OwnerOf(slot); o != nil {
			return s.shortID(o.ID)
		}
		return "none"
	}
	return "none"
}

// randomFaults schedules n fault steps, each drawn from the same set an operator
// would recognise: cut the network somewhere, heal it, kill a node, bring one
// back. The draw is seeded, so the schedule is part of the reproducible run.
func (s *sim) randomFaults(start, every time.Duration, n int) {
	// A slice, not a set: which node comes back has to be a function of the
	// seed, and ranging over a map would make it a function of the runtime's
	// hash seed instead.
	var down []string
	for i := 0; i < n; i++ {
		at := start + time.Duration(i)*every
		switch s.rnd.Intn(4) {
		case 0:
			// Split the cluster at a random point in the address order.
			cut := 1 + s.rnd.Intn(len(s.nodes)-1)
			var side []string
			for _, n := range s.nodes[:cut] {
				side = append(side, n.addr)
			}
			s.at(at, func() { s.partition(side) })
		case 1:
			s.at(at, func() { s.heal() })
		case 2:
			victim := s.nodes[s.rnd.Intn(len(s.nodes))].addr
			if slices.Contains(down, victim) {
				continue
			}
			down = append(down, victim)
			s.at(at, func() { s.crash(victim) })
		case 3:
			if len(down) == 0 {
				continue
			}
			victim := down[0] // longest dead comes back first
			down = down[1:]
			s.at(at, func() { s.restart(victim) })
		}
	}
}

func (s *sim) String() string { return fmt.Sprintf("sim(seed=%d)", s.seed) }

// A replica is promoted and then restarts. It comes back with the role its
// flags describe — a replica — having forgotten a promotion the rest of the
// cluster still remembers at a higher config epoch. Nothing reconciled that:
// gossip about ourselves was discarded, so the node disowned slots every other
// node was redirecting clients to, and the redirect loop was permanent.
//
// This is the case the harness was built to find; it needs a crash *after* a
// failover, which the loopback tests have no way to arrange.
func TestSimPromotedReplicaSurvivesItsOwnRestart(t *testing.T) {
	for seed := int64(0); seed < seeds(8); seed++ {
		s := newSim(t, seed, simOptions{})
		s.at(2*time.Second, func() { s.crash("10.0.0.1:6379") })
		s.runUntilStable(90 * time.Second)
		if s.failed {
			continue
		}
		if owner := s.ownerOf(0); owner != "10.0.0.4" {
			s.fail("precondition: slot 0 owned by %s, want 10.0.0.4", owner)
			continue
		}

		at := s.elapsed()
		s.at(at, func() { s.crash("10.0.0.4:6379") })
		s.at(at+2*time.Second, func() { s.restart("10.0.0.4:6379") })
		s.runUntilStable(120 * time.Second)
		if s.failed {
			continue
		}

		me := s.byID["10.0.0.4:6379"].mgr.View().Myself()
		if me.Role != RolePrimary || me.SlotsCount() != 5461 {
			s.fail("restarted node disowned the slots it was promoted for: role=%s slots=%d epoch=%d",
				me.Role, me.SlotsCount(), me.ConfigEpoch)
			continue
		}
		// It must take the slots back at the epoch it won them at, not a new
		// one: inventing an epoch would be a promotion nobody voted for.
		if me.ConfigEpoch != 1 {
			s.fail("restarted node came back at config epoch %d, want the 1 it was elected at", me.ConfigEpoch)
		}
	}
}
