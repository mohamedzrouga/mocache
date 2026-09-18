package server

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/cluster"
	"github.com/mohamedzrouga/mocache/internal/obs"
	"github.com/mohamedzrouga/mocache/internal/resp"
)

// Runtime is the clustering layer (internal/node) as this server needs it.
// Declared here as an interface so the RESP server does not depend on the
// replication and failover machinery — and so tests can run without it.
type Runtime interface {
	// Failover implements CLUSTER FAILOVER on a replica.
	Failover(force, takeover bool) error
	// ReplicationStatus feeds INFO replication.
	ReplicationStatus() ReplicationStatus
	// WaitAcked blocks until n replicas acknowledge offset, or timeout, and
	// returns how many did. It is the answer to WAIT.
	WaitAcked(offset uint64, n int, timeout time.Duration) int
	// MasterOffset is this node's current replication offset.
	MasterOffset() uint64
	// Replicate turns this node into a replica of another (CLUSTER REPLICATE).
	Replicate(nodeID string) error
}

// ReplicaStatus is one connected replica.
type ReplicaStatus struct {
	ID     string
	Addr   string
	Offset uint64
}

// ReplicationStatus is the live replication picture for INFO.
type ReplicationStatus struct {
	Role              string
	MasterHost        string
	MasterPort        int
	MasterLinkStatus  string
	ReplID            string
	Offset            uint64
	ConnectedReplicas int
	Replicas          []ReplicaStatus
	FullResyncs       uint64
	SyncedKeys        uint64
	Failovers         uint64
}

// RESPOptions configures the Redis-compatible listener.
type RESPOptions struct {
	// Cluster is the live cluster state used for MOVED redirects and the
	// CLUSTER commands. Nil means standalone: every slot is served locally.
	Cluster *cluster.Manager
	// Runtime is the replication/failover layer. Nil disables the commands
	// that need it, which is how the HTTP-only and test configurations run.
	Runtime Runtime
	// Password, when set, requires AUTH (or HELLO ... AUTH) before any command.
	Password string
	// Idle closes a connection that has sent nothing for this long. Zero
	// disables it, which is Redis's default (`timeout 0`).
	Idle     time.Duration
	MaxConns int
	// KeysLimit caps how many keys one KEYS reply may contain, so a client
	// cannot make the node allocate the whole keyspace at once.
	KeysLimit int
}

// RESPServer speaks the Redis protocol on its own listener, backed by the same
// LRU as the HTTP and RPC front ends. Connections are tracked so drain can
// close them instead of waiting for clients to notice (Redis clients hold
// connections open indefinitely, so an idle timeout would not help shutdown).
type RESPServer struct {
	cache *cache.Cache
	gate  *Gate
	opt   RESPOptions
	mgr   *cluster.Manager

	nextID atomic.Uint64
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
	closed bool
}

func NewRESP(c *cache.Cache, g *Gate, opt RESPOptions) *RESPServer {
	if opt.MaxConns <= 0 {
		opt.MaxConns = defaultMaxConns
	}
	if opt.KeysLimit <= 0 {
		opt.KeysLimit = 100_000
	}
	mgr := opt.Cluster
	if mgr == nil {
		mgr = cluster.StandaloneManager("127.0.0.1", 6379)
	}
	if g == nil {
		g = NewGate()
		g.SetReady(true)
	}
	return &RESPServer{cache: c, gate: g, opt: opt, mgr: mgr, conns: make(map[net.Conn]struct{})}
}

// Serve accepts until the listener is closed.
func (s *RESPServer) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		if !s.track(conn) {
			// Over the connection cap: tell the client why instead of a bare RST.
			_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
			_ = conn.Close()
			continue
		}
		s.wg.Add(1)
		go func(nc net.Conn) {
			defer s.wg.Done()
			defer s.untrack(nc)
			defer nc.Close()
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("resp panic", "panic", rec)
				}
			}()
			s.serveConn(nc)
		}(conn)
	}
}

// Close closes tracked sockets to unblock in-flight reads, then waits for the
// handler goroutines. The listener must be closed by the caller.
func (s *RESPServer) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *RESPServer) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.conns) >= s.opt.MaxConns {
		return false
	}
	s.conns[c] = struct{}{}
	obs.RESPConns(1)
	return true
}

func (s *RESPServer) untrack(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[c]; ok {
		delete(s.conns, c)
		obs.RESPConns(-1)
	}
}

// respConn is the per-connection state the protocol requires: negotiated RESP
// version, auth status, client name, and the cluster READONLY flag.
type respConn struct {
	s        *RESPServer
	conn     net.Conn
	r        *resp.Reader
	w        *resp.Writer
	id       uint64
	name     string
	lib      string
	libVer   string
	authed   bool
	readonly bool
	quit     bool
}

func (s *RESPServer) serveConn(nc net.Conn) {
	if tcp, ok := nc.(*net.TCPConn); ok {
		// Latency, not throughput: a GET reply must not wait for more data.
		_ = tcp.SetNoDelay(true)
	}
	c := &respConn{
		s:      s,
		conn:   nc,
		r:      resp.NewReader(nc),
		w:      resp.NewWriter(nc),
		id:     s.nextID.Add(1),
		authed: s.opt.Password == "",
	}

	for !c.quit {
		if s.opt.Idle > 0 {
			_ = nc.SetReadDeadline(time.Now().Add(s.opt.Idle))
		}
		args, err := c.r.ReadCommand()
		if err != nil {
			if !errors.Is(err, io.EOF) && !isClosedConn(err) {
				// Protocol errors are the client's fault; say so before hanging up.
				c.w.Errorf("ERR Protocol error: %v", err)
				_ = c.w.Flush()
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		start := time.Now()
		failed := c.dispatch(args)
		obs.RecordRESP(time.Since(start), failed)
		if err := c.w.Flush(); err != nil {
			return
		}
	}
}

// dispatch routes one command and reports whether it produced an error reply.
func (c *respConn) dispatch(args [][]byte) bool {
	name := strings.ToUpper(string(args[0]))
	spec, ok := commands[name]
	if !ok {
		c.w.Errorf("ERR unknown command '%s', with args beginning with: %s",
			string(args[0]), quoteArgs(args[1:], 3))
		return true
	}
	if !c.authed && !spec.noAuth {
		c.w.Error("NOAUTH Authentication required.")
		return true
	}
	if !spec.arityOK(len(args)) {
		c.w.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(name))
		return true
	}
	// Route before executing: a redirect must not have side effects.
	if code, msg := c.route(spec, args); code != routeLocal {
		c.w.Error(msg)
		if code == routeMoved {
			obs.RecordMoved()
		}
		return true
	}
	return spec.run(c, args)
}

type routeCode int

const (
	routeLocal routeCode = iota
	routeMoved
	routeCrossSlot
	routeDown
)

// route enforces Redis Cluster key routing: every key in a command must hash to
// one slot, and that slot must be served here. Anything else is an error reply
// the client knows how to act on — MOVED makes it retry elsewhere and refresh
// its slot map, CROSSSLOT tells it to split the command.
func (c *respConn) route(spec *cmdSpec, args [][]byte) (routeCode, string) {
	if !c.s.view().Enabled() {
		return routeLocal, ""
	}
	keys := spec.keysOf(args)
	if len(keys) == 0 {
		return routeLocal, ""
	}
	slot := cluster.KeySlot(string(keys[0]))
	for _, k := range keys[1:] {
		if cluster.KeySlot(string(k)) != slot {
			return routeCrossSlot, "CROSSSLOT Keys in request don't hash to the same slot"
		}
	}
	if c.s.view().Mine(slot) {
		return routeLocal, ""
	}
	owner := c.s.view().OwnerOf(slot)
	if owner == nil {
		return routeDown, "CLUSTERDOWN Hash slot not served"
	}
	// A replica may serve its primary's slots for reads once the client has
	// sent READONLY; writes always go to the primary.
	if c.readonly && !spec.write {
		me := c.s.view().Myself()
		if me.Role == cluster.RoleReplica && me.PrimaryO == owner.Addr() {
			return routeLocal, ""
		}
	}
	return routeMoved, "MOVED " + itoa(slot) + " " + owner.Addr()
}

func isClosedConn(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")
}

// Version is the MoCache build version reported by INFO.
var Version = "0.1.0"

var pid = os.Getpid()

func (s *RESPServer) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// view is the current cluster snapshot. Read per command: a failover can change
// slot ownership between two commands on the same connection, and the next
// MOVED must point at the new owner.
func (s *RESPServer) view() *cluster.Topology { return s.mgr.View() }

// globMatchString exposes the cache's glob matcher to CONFIG GET patterns.
func globMatchString(pattern, s string) bool { return cache.GlobMatch(pattern, s) }
