package node

import (
	"log/slog"
	"net"

	"github.com/mohamedzrouga/mocache/internal/bus"
	"github.com/mohamedzrouga/mocache/internal/cluster"
)

// PingHeader is the gossip payload carried by both PING and PONG.
type PingHeader struct {
	From        string               `json:"from"`
	Epoch       uint64               `json:"epoch"`
	ConfigEpoch uint64               `json:"config_epoch"`
	ReplOffset  uint64               `json:"repl_offset"`
	Nodes       []cluster.GossipNode `json:"nodes"`
}

// FailHeader announces a cluster-wide failure decision.
type FailHeader struct {
	NodeID string `json:"node_id"`
	From   string `json:"from"`
}

// UpdateHeader announces a new slot owner at a higher config epoch.
type UpdateHeader struct {
	NodeID string `json:"node_id"`
	Epoch  uint64 `json:"epoch"`
	From   string `json:"from"`
}

// Serve answers an inbound bus request. Everything here is fast and
// non-blocking: the peer is waiting on the other end of the socket.
func (c *Cluster) Serve(f bus.Frame) bus.Frame {
	switch f.Type {
	case bus.TypePing:
		var ping PingHeader
		if err := f.Decode(&ping); err != nil {
			return bus.Errorf("bad ping: %v", err)
		}
		c.mgr.NoteContact(ping.From, ping.ReplOffset, ping.ConfigEpoch)
		c.mgr.MergeGossip(ping.From, ping.Epoch, ping.Nodes)
		c.mgr.SetReplOffset(c.myOffset())

		me := c.mgr.View().Myself()
		pong, err := bus.NewFrame(bus.TypePong, PingHeader{
			From:        me.ID,
			Epoch:       c.mgr.CurrentEpoch(),
			ConfigEpoch: me.ConfigEpoch,
			ReplOffset:  c.myOffset(),
			Nodes:       c.mgr.Gossip(),
		}, nil)
		if err != nil {
			return bus.Errorf("pong: %v", err)
		}
		return pong

	case bus.TypeFail:
		var h FailHeader
		if err := f.Decode(&h); err != nil {
			return bus.Errorf("bad fail: %v", err)
		}
		// The sender already gathered a majority; adopting its decision keeps
		// every node's view of who is alive identical.
		if c.mgr.MarkFail(h.NodeID) {
			slog.Warn("node reported failed by peer", "node_id", h.NodeID, "reporter", h.From)
		}
		return ack()

	case bus.TypeVoteRequest:
		var req cluster.VoteRequest
		if err := f.Decode(&req); err != nil {
			return bus.Errorf("bad vote request: %v", err)
		}
		reply := c.mgr.ConsiderVote(req)
		if reply.Granted {
			slog.Info("granted failover vote", "candidate", req.NodeID, "epoch", req.Epoch)
		} else {
			slog.Info("refused failover vote", "candidate", req.NodeID, "epoch", req.Epoch, "reason", reply.Reason)
		}
		out, err := bus.NewFrame(bus.TypeVoteGrant, reply, nil)
		if err != nil {
			return bus.Errorf("vote reply: %v", err)
		}
		return out

	case bus.TypeUpdate:
		var h UpdateHeader
		if err := f.Decode(&h); err != nil {
			return bus.Errorf("bad update: %v", err)
		}
		if c.mgr.Promote(h.NodeID, h.Epoch) {
			slog.Info("applied new slot configuration", "owner", h.NodeID, "config_epoch", h.Epoch)
			// If that promotion took our slots, Promote has already demoted us;
			// reconcile so replication starts following the new primary.
			c.reconcileRole()
		}
		return ack()

	case bus.TypeMigrate:
		return c.applyMigrated(f)

	case bus.TypeHello:
		return ack()
	}
	return bus.Errorf("unsupported frame type %d", f.Type)
}

// Stream serves a replication subscription; it owns the connection.
func (c *Cluster) Stream(f bus.Frame, conn net.Conn) {
	if c.mgr.View().Myself().Role == cluster.RoleReplica {
		// Chained replication is not supported: a replica has no stream of its
		// own to offer, and pretending otherwise would hand out stale offsets.
		_ = bus.Write(conn, bus.Errorf("not a primary"))
		_ = conn.Close()
		return
	}
	c.primary.Serve(f, conn, c.store)
}

func ack() bus.Frame {
	f, _ := bus.NewFrame(bus.TypeAck, nil, nil)
	return f
}
