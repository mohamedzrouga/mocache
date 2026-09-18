package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cluster"
)

// The client-visible half of live slot migration. Slot 866 ("hello") belongs to
// 7001 in clusterTopo; 7002 is the node it moves to.

const (
	slotHello = "866"
	addr7001  = "127.0.0.1:7001"
	addr7002  = "127.0.0.1:7002"
	keyHello  = "hello"
)

func id7002() string { return cluster.NodeID(addr7002) }
func id7001() string { return cluster.NodeID(addr7001) }

// fakeRuntime stands in for internal/node: it records what MIGRATE handed it
// without needing a second live node.
type fakeRuntime struct {
	got     []MigratedKey
	replace bool
	err     error
}

func (f *fakeRuntime) Failover(bool, bool) error                { return nil }
func (f *fakeRuntime) ReplicationStatus() ReplicationStatus     { return ReplicationStatus{Role: "master"} }
func (f *fakeRuntime) WaitAcked(uint64, int, time.Duration) int { return 0 }
func (f *fakeRuntime) MasterOffset() uint64                     { return 0 }
func (f *fakeRuntime) Replicate(string) error                   { return nil }

func (f *fakeRuntime) MigrateKeys(addr string, keys []MigratedKey, replace bool, _ time.Duration) error {
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, keys...)
	f.replace = replace
	return nil
}

// On the source of a migration: keys still here are served here, keys already
// gone are answered with ASK, and a multi-key command split across the two is
// refused rather than answered from half the data.
func TestRESPMigratingSlotServesPresentKeysAndAsksForTheRest(t *testing.T) {
	mgr := clusterTopo(t, addr7001)
	_, addr, _ := startRESP(t, RESPOptions{Cluster: mgr})
	c := dial(t, addr)

	if got := c.do("SET", keyHello, "v"); got != "+OK" {
		t.Fatalf("SET = %s", got)
	}
	if got := c.do("CLUSTER", "SETSLOT", slotHello, "MIGRATING", id7002()); got != "+OK" {
		t.Fatalf("SETSLOT MIGRATING = %s", got)
	}

	// Still here: still served here. The slot map has not changed yet.
	if got := c.do("GET", keyHello); got != "$v" {
		t.Fatalf("GET of a key that has not moved = %s", got)
	}
	// Gone: the client is sent to the destination for this one command.
	if got := c.do("GET", "nosuchkey{hello}"); got != "-ASK "+slotHello+" "+addr7002 {
		t.Fatalf("GET of a missing key = %s", got)
	}
	// Writes redirect too, or the two nodes would both accept the same key.
	if got := c.do("SET", "another{hello}", "x"); got != "-ASK "+slotHello+" "+addr7002 {
		t.Fatalf("SET of a missing key = %s", got)
	}

	// One key here, one not: neither node can answer correctly.
	if got := c.do("MGET", keyHello, "gone{hello}"); got != "-TRYAGAIN Multiple keys request during rehashing of slot" {
		t.Fatalf("straddling MGET = %s", got)
	}
	// Once every key is gone it is a plain ASK again.
	c.do("DEL", keyHello)
	if got := c.do("MGET", keyHello, "gone{hello}"); got != "-ASK "+slotHello+" "+addr7002 {
		t.Fatalf("MGET after the keys moved = %s", got)
	}
}

// On the destination: the slot is not ours until it is committed, so it is
// served only for a client that was sent here by an ASK and said ASKING.
func TestRESPImportingSlotNeedsAsking(t *testing.T) {
	mgr := clusterTopo(t, addr7002)
	_, addr, _ := startRESP(t, RESPOptions{Cluster: mgr})
	c := dial(t, addr)

	if got := c.do("CLUSTER", "SETSLOT", slotHello, "IMPORTING", id7001()); got != "+OK" {
		t.Fatalf("SETSLOT IMPORTING = %s", got)
	}
	// Without ASKING the client belongs at the current owner.
	if got := c.do("GET", keyHello); got != "-MOVED "+slotHello+" "+addr7001 {
		t.Fatalf("GET without ASKING = %s", got)
	}
	if got := c.do("ASKING"); got != "+OK" {
		t.Fatalf("ASKING = %s", got)
	}
	if got := c.do("SET", keyHello, "v"); got != "+OK" {
		t.Fatalf("SET after ASKING = %s", got)
	}
	// One command only: the flag must not leak into the next one, or the
	// destination would start answering for a slot it does not own.
	if got := c.do("GET", keyHello); got != "-MOVED "+slotHello+" "+addr7001 {
		t.Fatalf("second command after one ASKING = %s", got)
	}
	// It is consumed even by a command that is redirected away.
	c.do("ASKING")
	c.do("GET", "abc") // a different slot, MOVED elsewhere
	if got := c.do("GET", keyHello); got != "-MOVED "+slotHello+" "+addr7001 {
		t.Fatalf("ASKING survived a redirected command: %s", got)
	}
}

// Committing the move is refused while the source still holds keys in the slot:
// they would be invisible at the new owner and unreachable here.
func TestRESPSetSlotNodeRefusesWhileKeysRemain(t *testing.T) {
	mgr := clusterTopo(t, addr7001)
	_, addr, _ := startRESP(t, RESPOptions{Cluster: mgr})
	c := dial(t, addr)

	c.do("SET", keyHello, "v")
	c.do("CLUSTER", "SETSLOT", slotHello, "MIGRATING", id7002())

	got := c.do("CLUSTER", "SETSLOT", slotHello, "NODE", id7002())
	if !strings.Contains(got, "still hold keys") {
		t.Fatalf("SETSLOT NODE with keys left behind = %s", got)
	}

	c.do("DEL", keyHello)
	if got := c.do("CLUSTER", "SETSLOT", slotHello, "NODE", id7002()); got != "+OK" {
		t.Fatalf("SETSLOT NODE after the keys moved = %s", got)
	}
	// The slot now belongs elsewhere, and the marker is gone.
	if got := c.do("GET", keyHello); got != "-MOVED "+slotHello+" "+addr7002 {
		t.Fatalf("GET after committing the move = %s", got)
	}
	// Everything else this node owned is untouched.
	if got := c.do("SET", "foo{bar}", "v"); !strings.HasPrefix(got, "-MOVED") && got != "+OK" {
		t.Fatalf("unrelated key broke after the reshard: %s", got)
	}
	if n := mgr.View().Myself().SlotsCount(); n != 5460 {
		t.Fatalf("source kept %d slots, want 5460", n)
	}
}

func TestRESPMigrateShipsAndRemovesKeys(t *testing.T) {
	rt := &fakeRuntime{}
	_, addr, store := startRESP(t, RESPOptions{Cluster: clusterTopo(t, addr7001), Runtime: rt})
	c := dial(t, addr)

	c.do("SET", keyHello, "v")
	c.do("SET", "second{hello}", "w")
	c.do("EXPIRE", "second{hello}", "600")

	if got := c.do("MIGRATE", "127.0.0.1", "7002", "", "0", "5000", "KEYS", keyHello, "second{hello}"); got != "+OK" {
		t.Fatalf("MIGRATE = %s", got)
	}
	if len(rt.got) != 2 {
		t.Fatalf("shipped %d keys, want 2", len(rt.got))
	}
	// A key with a TTL has to arrive with the instant it expires at, not a
	// duration that restarts on the way.
	for _, k := range rt.got {
		if k.Key == "second{hello}" && k.ExpireAt.IsZero() {
			t.Fatal("expiry was dropped in transit")
		}
	}
	// Deleted only after the destination confirmed.
	if store.Exists(keyHello) || store.Exists("second{hello}") {
		t.Fatal("source kept the keys it migrated")
	}

	// Nothing to move is not an error: a cache entry may have expired between
	// the GETKEYSINSLOT that listed it and the MIGRATE that moves it.
	if got := c.do("MIGRATE", "127.0.0.1", "7002", "gone{hello}", "0", "5000"); got != "+NOKEY" {
		t.Fatalf("MIGRATE of a missing key = %s", got)
	}
}

func TestRESPMigrateKeepsKeysWhenTheTargetRefuses(t *testing.T) {
	rt := &fakeRuntime{err: errors.New("BUSYKEY Target key name already exists.")}
	_, addr, store := startRESP(t, RESPOptions{Cluster: clusterTopo(t, addr7001), Runtime: rt})
	c := dial(t, addr)

	c.do("SET", keyHello, "v")
	if got := c.do("MIGRATE", "127.0.0.1", "7002", keyHello, "0", "5000"); got != "-BUSYKEY Target key name already exists." {
		t.Fatalf("MIGRATE against an existing key = %s", got)
	}
	// The key must survive a failed migration, or it is simply lost.
	if !store.Exists(keyHello) {
		t.Fatal("a failed MIGRATE deleted the source key")
	}
	// COPY leaves it behind on success too.
	rt.err = nil
	if got := c.do("MIGRATE", "127.0.0.1", "7002", keyHello, "0", "5000", "COPY", "REPLACE"); got != "+OK" {
		t.Fatalf("MIGRATE COPY = %s", got)
	}
	if !store.Exists(keyHello) {
		t.Fatal("MIGRATE COPY removed the source key")
	}
	if !rt.replace {
		t.Fatal("REPLACE was not passed to the target")
	}
}

// redis-cli --cluster reads these markers to find a reshard that was
// interrupted, so they have to appear on the myself line and nowhere else.
func TestRESPClusterNodesShowsReshardMarkers(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{Cluster: clusterTopo(t, addr7001)})
	c := dial(t, addr)
	c.do("CLUSTER", "SETSLOT", slotHello, "MIGRATING", id7002())
	c.do("CLUSTER", "SETSLOT", "9000", "IMPORTING", id7002())

	out := c.do("CLUSTER", "NODES")
	for _, want := range []string{"[866->-" + id7002() + "]", "[9000-<-" + id7002() + "]"} {
		if !strings.Contains(out, want) {
			t.Fatalf("CLUSTER NODES is missing %s:\n%s", want, out)
		}
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "[") && !strings.Contains(line, "myself") {
			t.Fatalf("reshard markers leaked onto another node's line: %s", line)
		}
	}
}

func TestRESPSetSlotRejectsBadInput(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{Cluster: clusterTopo(t, addr7001)})
	c := dial(t, addr)

	for _, tc := range []struct{ args []string }{
		{[]string{"CLUSTER", "SETSLOT", "99999", "MIGRATING", id7002()}},
		{[]string{"CLUSTER", "SETSLOT", "notanumber", "STABLE"}},
		{[]string{"CLUSTER", "SETSLOT", slotHello, "NONSENSE", id7002()}},
		{[]string{"CLUSTER", "SETSLOT", slotHello, "MIGRATING", "unknown-node-id"}},
		{[]string{"CLUSTER", "SETSLOT", slotHello}},
	} {
		if got := c.do(tc.args...); !strings.HasPrefix(got, "-ERR") {
			t.Fatalf("%v was accepted: %s", tc.args, got)
		}
	}
}
