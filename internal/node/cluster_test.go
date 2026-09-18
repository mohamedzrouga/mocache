package node

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/cluster"
)

// These tests run real nodes over real TCP on loopback: the bus, the
// replication stream and the election all execute as they would in production.
// Timings are compressed (node timeout 400ms rather than 5s) so a failover
// completes in about a second.

type testNode struct {
	name  string
	addr  string
	store *cache.Cache
	mgr   *cluster.Manager
	cl    *Cluster
}

func (n *testNode) role() cluster.Role { return n.mgr.View().Myself().Role }

func (n *testNode) slots() int { return n.mgr.View().Myself().SlotsCount() }

// freeAddr finds a loopback port whose bus port (client + 10000) is also free,
// since the bus address is derived from the announce address.
func freeAddr(t *testing.T) string {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		if port+10000 > 65535 {
			continue
		}
		busLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+10000))
		if err != nil {
			continue
		}
		_ = busLn.Close()
		return fmt.Sprintf("127.0.0.1:%d", port)
	}
	t.Fatal("no free port pair")
	return ""
}

func testOptions() Options {
	return Options{
		NodeTimeout:   400 * time.Millisecond,
		PingInterval:  80 * time.Millisecond,
		FailoverDelay: 150 * time.Millisecond,
		DialTimeout:   300 * time.Millisecond,
	}
}

// startCluster builds nodes from peer specs and starts every one of them.
func startCluster(t *testing.T, names []string, specs []string, addrs map[string]string) map[string]*testNode {
	return startClusterWith(t, names, specs, addrs, nil)
}

// startClusterWith is startCluster with a hook to vary one option, so a test
// that needs a non-default (replica migration, say) does not have to duplicate
// the whole harness.
func startClusterWith(t *testing.T, names []string, specs []string, addrs map[string]string, tune func(*Options)) map[string]*testNode {
	t.Helper()
	nodes := make(map[string]*testNode, len(names))
	for _, name := range names {
		store := cache.NewWithLimits(cache.Limits{MaxItems: 10000, MaxBytes: 8 << 20, MaxValue: 1 << 20, MaxKey: 256})
		mgr, err := cluster.NewManager(specs, addrs[name])
		if err != nil {
			t.Fatalf("%s: manager: %v", name, err)
		}
		opt := testOptions()
		opt.BusAddr = busAddrOf(hostOf(addrs[name]), portOf(addrs[name]))
		if tune != nil {
			tune(&opt)
		}
		cl := New(mgr, store, opt)
		if err := cl.Start(); err != nil {
			t.Fatalf("%s: start: %v", name, err)
		}
		nodes[name] = &testNode{name: name, addr: addrs[name], store: store, mgr: mgr, cl: cl}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.cl.Close()
			n.store.Close()
		}
	})
	return nodes
}

func hostOf(addr string) string { h, _, _ := net.SplitHostPort(addr); return h }

func portOf(addr string) int {
	_, p, _ := net.SplitHostPort(addr)
	n := 0
	for _, c := range p {
		n = n*10 + int(c-'0')
	}
	return n
}

// waitFor polls until cond holds, failing the test with msg on timeout.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// threeShardsOneReplica lays out A/B/C as primaries and D as A's replica.
func threeShardsOneReplica(t *testing.T) (map[string]*testNode, map[string]string) {
	t.Helper()
	addrs := map[string]string{
		"A": freeAddr(t), "B": freeAddr(t), "C": freeAddr(t), "D": freeAddr(t),
	}
	specs := []string{
		addrs["A"] + "=0-5460",
		addrs["B"] + "=5461-10922",
		addrs["C"] + "=10923-16383",
		addrs["D"] + "=replica-of:" + addrs["A"],
	}
	return startCluster(t, []string{"A", "B", "C", "D"}, specs, addrs), addrs
}

func TestReplicationStreamsToReplica(t *testing.T) {
	nodes, _ := threeShardsOneReplica(t)
	a, d := nodes["A"], nodes["D"]

	waitFor(t, 5*time.Second, "replica link up", func() bool {
		return d.cl.Replica().Status() == "up"
	})

	// Writes on the primary must appear on the replica, with their expiry.
	if err := a.store.Set("hello", []byte("world"), 0); err != nil {
		t.Fatal(err)
	}
	if err := a.store.Set("ttlkey", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "value replicated", func() bool {
		v, _, ok := d.store.Peek("hello")
		return ok && string(v) == "world"
	})
	_, exp, ok := d.store.Peek("ttlkey")
	if !ok || exp.IsZero() {
		t.Fatalf("expiry did not replicate: ok=%v exp=%v", ok, exp)
	}

	// Deletes replicate too, or a replica would serve entries the primary
	// has dropped.
	a.store.Delete("hello")
	waitFor(t, 3*time.Second, "delete replicated", func() bool {
		_, _, ok := d.store.Peek("hello")
		return !ok
	})

	// A replica must not feed its own writes back into the stream.
	if off := d.cl.Primary().Offset(); off != 0 {
		t.Fatalf("replica generated %d ops of its own", off)
	}
}

// A replica that connects after the primary already holds data gets a snapshot.
func TestFullResyncSnapshot(t *testing.T) {
	nodes, _ := threeShardsOneReplica(t)
	a, d := nodes["A"], nodes["D"]

	waitFor(t, 5*time.Second, "initial sync", func() bool {
		return d.cl.Replica().Status() == "up"
	})
	for i := 0; i < 200; i++ {
		if err := a.store.Set(fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, "200 keys replicated", func() bool {
		return d.store.Len() == 200
	})

	// Force a resync by dropping the link; the replica must end up with the
	// same keyspace, not a merge of old and new.
	_ = a.store.Set("only-before-resync", []byte("v"), 0)
	a.cl.Primary().DropAll()
	waitFor(t, 5*time.Second, "resync completes", func() bool {
		return d.cl.Replica().Status() == "up" && d.store.Len() == 201
	})
	if v, _, ok := d.store.Peek("only-before-resync"); !ok || string(v) != "v" {
		t.Fatal("key written before the resync is missing afterwards")
	}
}

// The headline case: the primary dies, its replica is promoted by a majority
// vote of the surviving primaries, and it still holds the replicated data.
func TestFailoverPromotesReplica(t *testing.T) {
	nodes, _ := threeShardsOneReplica(t)
	a, b, c, d := nodes["A"], nodes["B"], nodes["C"], nodes["D"]

	waitFor(t, 5*time.Second, "replica in sync", func() bool {
		return d.cl.Replica().Status() == "up"
	})
	if err := a.store.Set("hello", []byte("survives"), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "data on replica", func() bool {
		_, _, ok := d.store.Peek("hello")
		return ok
	})
	if d.role() != cluster.RoleReplica || d.slots() != 0 {
		t.Fatal("D should start as a replica with no slots")
	}

	a.cl.Close() // the primary dies

	waitFor(t, 15*time.Second, "D promoted to primary", func() bool {
		return d.role() == cluster.RolePrimary && d.slots() == 5461
	})
	// The survivors must agree, or clients would be redirected to a dead node.
	for _, n := range []*testNode{b, c} {
		waitFor(t, 10*time.Second, n.name+" sees D as the owner of slot 0", func() bool {
			owner := n.mgr.View().OwnerOf(0)
			return owner != nil && owner.Addr() == d.addr
		})
	}
	// Promotion is worthless if the data did not come with it.
	if v, _, ok := d.store.Peek("hello"); !ok || string(v) != "survives" {
		t.Fatalf("promoted replica lost the replicated data: %q ok=%v", v, ok)
	}
	if d.mgr.View().Myself().ConfigEpoch == 0 {
		t.Fatal("promotion did not take a new config epoch")
	}
}

// A primary that comes back after being replaced must not keep serving its old
// slots: two owners for one slot is the failure this whole mechanism prevents.
func TestReturningPrimaryDemotesItself(t *testing.T) {
	addrs := map[string]string{
		"A": freeAddr(t), "B": freeAddr(t), "C": freeAddr(t), "D": freeAddr(t),
	}
	specs := []string{
		addrs["A"] + "=0-5460",
		addrs["B"] + "=5461-10922",
		addrs["C"] + "=10923-16383",
		addrs["D"] + "=replica-of:" + addrs["A"],
	}
	nodes := startCluster(t, []string{"A", "B", "C", "D"}, specs, addrs)
	a, d := nodes["A"], nodes["D"]

	waitFor(t, 5*time.Second, "replica in sync", func() bool {
		return d.cl.Replica().Status() == "up"
	})
	_ = a.store.Set("hello", []byte("v1"), 0)
	a.cl.Close()
	waitFor(t, 15*time.Second, "D promoted", func() bool { return d.role() == cluster.RolePrimary })

	// Bring A back with a fresh runtime, exactly as a restarted process would:
	// its configuration still says it owns 0-5460.
	store := cache.NewWithLimits(cache.Limits{MaxItems: 10000, MaxBytes: 8 << 20})
	mgr, err := cluster.NewManager(specs, addrs["A"])
	if err != nil {
		t.Fatal(err)
	}
	opt := testOptions()
	opt.BusAddr = busAddrOf(hostOf(addrs["A"]), portOf(addrs["A"]))
	revived := New(mgr, store, opt)
	if err := revived.Start(); err != nil {
		t.Fatalf("restart A: %v", err)
	}
	t.Cleanup(func() { revived.Close(); store.Close() })

	waitFor(t, 15*time.Second, "A demotes itself", func() bool {
		me := mgr.View().Myself()
		return me.Role == cluster.RoleReplica && me.SlotsCount() == 0
	})
	// And it must resynchronise from the node that replaced it.
	waitFor(t, 10*time.Second, "A follows D", func() bool {
		return revived.Replica().Status() == "up"
	})
	_ = d.store.Set("hello", []byte("v2"), 0)
	waitFor(t, 5*time.Second, "A has the new primary's data", func() bool {
		v, _, ok := store.Peek("hello")
		return ok && string(v) == "v2"
	})
}

// Without a majority there must be no promotion: a minority partition that
// promoted itself would give the cluster two owners for one slot.
func TestNoPromotionWithoutMajority(t *testing.T) {
	addrs := map[string]string{"A": freeAddr(t), "B": freeAddr(t), "C": freeAddr(t), "D": freeAddr(t)}
	specs := []string{
		addrs["A"] + "=0-5460",
		addrs["B"] + "=5461-10922",
		addrs["C"] + "=10923-16383",
		addrs["D"] + "=replica-of:" + addrs["A"],
	}
	nodes := startCluster(t, []string{"A", "B", "C", "D"}, specs, addrs)
	d := nodes["D"]

	waitFor(t, 5*time.Second, "replica in sync", func() bool {
		return d.cl.Replica().Status() == "up"
	})

	// Kill the primary *and* both voters: D is alone and must stay a replica.
	nodes["A"].cl.Close()
	nodes["B"].cl.Close()
	nodes["C"].cl.Close()

	time.Sleep(4 * time.Second)
	if d.role() != cluster.RoleReplica {
		t.Fatal("D promoted itself without a majority of primaries")
	}
	if d.slots() != 0 {
		t.Fatal("D claimed slots without winning an election")
	}
}

// CLUSTER FAILOVER TAKEOVER is the operator's escape hatch and must work while
// the primary is still alive.
func TestManualTakeover(t *testing.T) {
	nodes, _ := threeShardsOneReplica(t)
	a, d := nodes["A"], nodes["D"]

	waitFor(t, 5*time.Second, "replica in sync", func() bool {
		return d.cl.Replica().Status() == "up"
	})
	if err := d.cl.Failover(false, false); err == nil {
		t.Fatal("failover should be refused while the primary is healthy")
	}
	if err := d.cl.Failover(false, true); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	waitFor(t, 5*time.Second, "D owns the slots", func() bool {
		return d.role() == cluster.RolePrimary && d.slots() == 5461
	})
	waitFor(t, 10*time.Second, "A stands down", func() bool {
		return a.mgr.View().Myself().Role == cluster.RoleReplica
	})
}
