package node

import (
	"errors"
	"net"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cluster"
	"github.com/mohamedzrouga/mocache/internal/server"
)

// This file implements server.Runtime. The RESP layer talks to the clustering
// layer only through that interface, so INFO, WAIT and CLUSTER FAILOVER report
// what is actually happening rather than what was configured.
var _ server.Runtime = (*Cluster)(nil)

// ReplicationStatus reports the live stream state.
func (c *Cluster) ReplicationStatus() server.ReplicationStatus {
	v := c.mgr.View()
	me := v.Myself()
	st := server.ReplicationStatus{
		Role:      "master",
		ReplID:    c.backlog.ReplID(),
		Offset:    c.primary.Offset(),
		Failovers: c.mgr.Failovers(),
	}
	if me.Role == cluster.RoleReplica {
		st.Role = "slave"
		st.Offset = c.replica.Offset()
		st.ReplID = c.replica.ReplID()
		st.MasterLinkStatus = string(c.replica.Status())
		st.FullResyncs = c.replica.Resyncs()
		st.SyncedKeys = c.replica.SyncedKeys()
		if p := v.PrimaryOf(me); p != nil {
			st.MasterHost, st.MasterPort = p.Host, p.Port
		}
		return st
	}

	for id, offset := range c.primary.Replicas() {
		addr := id
		if n := nodeByID(v, id); n != nil {
			addr = n.Addr()
		}
		st.Replicas = append(st.Replicas, server.ReplicaStatus{ID: id, Addr: addr, Offset: offset})
	}
	st.ConnectedReplicas = len(st.Replicas)
	return st
}

func nodeByID(v *cluster.Topology, id string) *cluster.Node {
	for _, n := range v.Nodes() {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// MasterOffset is this node's replication offset.
func (c *Cluster) MasterOffset() uint64 { return c.myOffset() }

// WaitAcked polls until n replicas have acknowledged offset. Polling rather
// than signalling keeps the acknowledgement path — which runs per replica, per
// 500ms — free of per-waiter bookkeeping.
func (c *Cluster) WaitAcked(offset uint64, n int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		got := c.primary.AckedBy(offset)
		if got >= n || time.Now().After(deadline) {
			return got
		}
		select {
		case <-c.stop:
			return got
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Replicate implements CLUSTER REPLICATE: turn this node into a replica of
// another. Refused while we still own slots — giving them up silently would
// strand that part of the keyspace.
func (c *Cluster) Replicate(nodeID string) error {
	v := c.mgr.View()
	me := v.Myself()
	if me.SlotsCount() > 0 {
		return errors.New("To set a master the node must be empty and without assigned slots")
	}
	if nodeID == me.ID {
		return errors.New("Can't replicate myself")
	}
	target := nodeByID(v, nodeID)
	if target == nil {
		return errors.New("Unknown node " + nodeID)
	}
	if target.Role != cluster.RolePrimary {
		return errors.New("I can only replicate a master, not a replica")
	}
	if !c.mgr.Demote(nodeID) {
		return errors.New("Could not set replication target")
	}
	c.primary.DropAll()
	c.reconcileRole()
	return nil
}

// busAddrOf is used by tests and by the CLUSTER NODES link-state column.
func busAddrOf(host string, port int) string {
	return net.JoinHostPort(host, itoa(port+10000))
}
