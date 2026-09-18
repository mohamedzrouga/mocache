package node

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/mohamedzrouga/mocache/internal/bus"
	"github.com/mohamedzrouga/mocache/internal/cluster"
)

// maybeFailover stands for election when our primary has been declared failed.
//
// The delay before standing is rank-based: the replica with the most complete
// replication offset waits the shortest time, so the best candidate normally
// wins uncontested and the others never ask. Ties are broken by jitter rather
// than by a race, because two candidates asking at the same instant can split
// the vote and leave the shard without a primary for another round.
func (c *Cluster) maybeFailover() {
	primaryID, rank, ok := c.mgr.FailoverCandidacy(c.replica.Offset())
	if !ok {
		return
	}
	c.mu.Lock()
	if c.electing || c.closed {
		c.mu.Unlock()
		return
	}
	c.electing = true
	c.mu.Unlock()

	delay := c.opt.FailoverDelay + time.Duration(rank)*c.opt.FailoverDelay*2 + randDelay(c.opt.FailoverDelay)
	slog.Warn("primary failed, standing for election", "primary_id", primaryID, "rank", rank, "in", delay.String())

	go func() {
		defer func() {
			c.mu.Lock()
			c.electing = false
			c.mu.Unlock()
		}()
		select {
		case <-c.stop:
			return
		case <-time.After(delay):
		}
		// Re-check: another replica may have been promoted while we waited, in
		// which case there is nothing to stand for.
		if _, _, still := c.mgr.FailoverCandidacy(c.replica.Offset()); !still {
			slog.Info("election no longer needed", "primary_id", primaryID)
			return
		}
		if err := c.runElection(primaryID, false); err != nil {
			slog.Warn("failover election failed", "err", err)
		}
	}()
}

// runElection asks every primary for a vote and promotes on a majority.
func (c *Cluster) runElection(primaryID string, force bool) error {
	view := c.mgr.View()
	me := view.Myself()
	epoch := c.mgr.NextEpoch()

	req := cluster.VoteRequest{
		Epoch:      epoch,
		NodeID:     me.ID,
		PrimaryID:  primaryID,
		ReplOffset: c.replica.Offset(),
		Force:      force,
	}
	frame, err := bus.NewFrame(bus.TypeVoteRequest, req, nil)
	if err != nil {
		return err
	}

	voters := 0
	for _, n := range view.Nodes() {
		if n.Role == cluster.RolePrimary && len(n.Slots) > 0 {
			voters++
		}
	}
	needed := voters/2 + 1

	var (
		mu      sync.Mutex
		granted int
		wg      sync.WaitGroup
	)
	for _, n := range view.Nodes() {
		if n.ID == me.ID || n.ID == primaryID || n.Role != cluster.RolePrimary || len(n.Slots) == 0 {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			reply, err := c.link(addr).Do(frame)
			if err != nil {
				return
			}
			var vote cluster.VoteReply
			if reply.Decode(&vote) != nil || !vote.Granted || vote.Epoch != epoch {
				return
			}
			mu.Lock()
			granted++
			mu.Unlock()
		}(n.BusAddr())
	}
	wg.Wait()

	slog.Info("election result", "epoch", epoch, "granted", granted, "needed", needed, "primaries", voters)
	if granted < needed {
		return errors.New("not enough votes")
	}
	return c.promoteSelf(epoch)
}

// promoteSelf takes the slots and starts serving them.
func (c *Cluster) promoteSelf(epoch uint64) error {
	me := c.mgr.View().Myself()
	if !c.mgr.Promote(me.ID, epoch) {
		return errors.New("promotion rejected: stale config epoch")
	}
	// Stop following, and start a fresh replication history: our offsets are
	// our own, and every replica must resynchronise against them rather than
	// assume the old primary's stream continues here.
	c.replica.Stop()
	c.backlog.Reset(cluster.NewReplID())

	slog.Warn("promoted to primary", "node_id", me.ID, "config_epoch", epoch, "slots", c.mgr.View().Myself().SlotsCount())
	c.broadcast(bus.TypeUpdate, UpdateHeader{NodeID: me.ID, Epoch: epoch, From: me.ID})
	c.reconcileRole()
	return nil
}

// Failover implements CLUSTER FAILOVER.
//
//	(none)   stand for election with the primaries' agreement
//	FORCE    same, but without waiting for the primary to be marked failed
//	TAKEOVER promote without asking anyone — the operator has accepted the risk
func (c *Cluster) Failover(force, takeover bool) error {
	view := c.mgr.View()
	me := view.Myself()
	if me.Role != cluster.RoleReplica {
		return errors.New("You should send CLUSTER FAILOVER to a replica")
	}
	primary := view.PrimaryOf(me)
	if primary == nil {
		return errors.New("I'm a replica but my master is unknown to me")
	}
	if takeover {
		// Deliberately unsafe: no votes, no majority. Documented as such
		// because it is the one command here that can split the keyspace.
		return c.promoteSelf(c.mgr.NextEpoch())
	}
	if !force && !primary.Fail {
		return errors.New("Master is not failing; use FORCE to failover anyway")
	}
	return c.runElection(primary.ID, true)
}
