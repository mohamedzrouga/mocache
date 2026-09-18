package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/cluster"
)

// respClient is a minimal Redis client: enough to drive the server the way a
// real one does (array requests, full reply parsing) without a dependency.
type respClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func startRESP(t *testing.T, opt RESPOptions) (*RESPServer, string, *cache.Cache) {
	t.Helper()
	c := cache.NewWithLimits(cache.Limits{MaxItems: 1000, MaxBytes: 1 << 20, MaxValue: 4096, MaxKey: 256})
	s := NewRESP(c, nil, opt)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() {
		_ = ln.Close()
		s.Close()
		c.Close()
	})
	return s, ln.Addr().String(), c
}

func dial(t *testing.T, addr string) *respClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	return &respClient{t: t, conn: conn, r: bufio.NewReader(conn)}
}

// do sends a command and returns the reply rendered as a comparable string:
// "+OK", "-ERR ...", ":1", "$3:foo", "$-1" for nil, "*2:[...]" for arrays.
func (c *respClient) do(args ...string) string {
	c.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.conn.Write([]byte(b.String())); err != nil {
		c.t.Fatalf("write: %v", err)
	}
	return c.read()
}

func (c *respClient) read() string {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		c.t.Fatal("empty reply line")
	}
	switch line[0] {
	case '+', '-', ':', '#', ',':
		return line
	case '_':
		return "$-1" // RESP3 null, normalised to the RESP2 spelling for tests
	case '$', '=':
		n, _ := strconv.Atoi(strings.TrimPrefix(line[1:], ""))
		if n < 0 {
			return "$-1"
		}
		buf := make([]byte, n+2)
		if _, err := ioReadFull(c.r, buf); err != nil {
			c.t.Fatalf("read bulk: %v", err)
		}
		return "$" + string(buf[:n])
	case '*', '~', '%':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "*-1"
		}
		if line[0] == '%' {
			n *= 2
		}
		parts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			parts = append(parts, c.read())
		}
		return "*[" + strings.Join(parts, " ") + "]"
	default:
		c.t.Fatalf("unexpected reply type %q", line)
		return ""
	}
}

func ioReadFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func TestRESPBasicCommands(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"ping", []string{"PING"}, "+PONG"},
		{"ping payload", []string{"PING", "hi"}, "$hi"},
		{"echo", []string{"ECHO", "x"}, "$x"},
		{"get missing", []string{"GET", "nope"}, "$-1"},
		{"set", []string{"SET", "foo", "bar"}, "+OK"},
		{"get", []string{"GET", "foo"}, "$bar"},
		{"exists", []string{"EXISTS", "foo"}, ":1"},
		{"type", []string{"TYPE", "foo"}, "+string"},
		{"strlen", []string{"STRLEN", "foo"}, ":3"},
		{"append", []string{"APPEND", "foo", "!"}, ":4"},
		{"getrange", []string{"GETRANGE", "foo", "0", "2"}, "$bar"},
		{"getrange negative", []string{"GETRANGE", "foo", "-1", "-1"}, "$!"},
		{"setnx taken", []string{"SETNX", "foo", "x"}, ":0"},
		{"setnx free", []string{"SETNX", "fresh", "x"}, ":1"},
		{"incr", []string{"INCR", "n"}, ":1"},
		{"incrby", []string{"INCRBY", "n", "41"}, ":42"},
		{"decrby", []string{"DECRBY", "n", "2"}, ":40"},
		{"incr on string", []string{"INCR", "foo"}, "-ERR value is not an integer or out of range"},
		{"mset", []string{"MSET", "a", "1", "b", "2"}, "+OK"},
		{"mget", []string{"MGET", "a", "b", "zz"}, "*[$1 $2 $-1]"},
		{"del", []string{"DEL", "a", "b"}, ":2"},
		{"ttl none", []string{"TTL", "foo"}, ":-1"},
		{"ttl missing", []string{"TTL", "gone"}, ":-2"},
		{"getdel", []string{"GETDEL", "fresh"}, "$x"},
		{"getdel again", []string{"GETDEL", "fresh"}, "$-1"},
		{"unknown", []string{"NOPE"}, `-ERR unknown command 'NOPE', with args beginning with: `},
		{"wrong arity", []string{"GET"}, "-ERR wrong number of arguments for 'get' command"},
		{"lists unsupported", []string{"LPUSH", "l", "v"}, `-ERR unknown command 'LPUSH', with args beginning with: "l" "v"`},
	} {
		if got := c.do(tc.args...); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestRESPExpiry(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)

	if got := c.do("SET", "k", "v", "EX", "100"); got != "+OK" {
		t.Fatalf("SET EX: %s", got)
	}
	if got := c.do("TTL", "k"); got != ":100" {
		t.Fatalf("TTL = %s", got)
	}
	// PX 1500 must report 2 seconds: Redis rounds TTL up, never down.
	c.do("SET", "k2", "v", "PX", "1500")
	if got := c.do("TTL", "k2"); got != ":2" {
		t.Fatalf("TTL rounding: %s", got)
	}
	if got := c.do("PERSIST", "k"); got != ":1" {
		t.Fatalf("PERSIST = %s", got)
	}
	if got := c.do("TTL", "k"); got != ":-1" {
		t.Fatalf("TTL after PERSIST = %s", got)
	}
	if got := c.do("SET", "k", "v", "EX", "0"); got != "-ERR invalid expire time in 'set' command" {
		t.Fatalf("zero expiry: %s", got)
	}
	if got := c.do("SET", "k", "v", "NX", "XX"); got != "-ERR syntax error" {
		t.Fatalf("NX+XX: %s", got)
	}
	// An actually expired key must read as missing.
	c.do("SET", "quick", "v", "PX", "20")
	time.Sleep(60 * time.Millisecond)
	if got := c.do("GET", "quick"); got != "$-1" {
		t.Fatalf("expired key returned %s", got)
	}
}

func TestRESPSetGetOption(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)
	if got := c.do("SET", "k", "v1", "GET"); got != "$-1" {
		t.Fatalf("SET GET on absent key = %s", got)
	}
	if got := c.do("SET", "k", "v2", "GET"); got != "$v1" {
		t.Fatalf("SET GET = %s", got)
	}
}

func TestRESPScanAndKeys(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)
	for i := 0; i < 20; i++ {
		c.do("SET", "user:"+strconv.Itoa(i), "v")
	}
	c.do("SET", "post:1", "v")

	seen := map[string]bool{}
	cursor := "0"
	for i := 0; ; i++ {
		if i > 30 {
			t.Fatal("SCAN did not terminate")
		}
		reply := c.do("SCAN", cursor, "MATCH", "user:*", "COUNT", "5")
		// reply looks like *[$<cursor> *[$k ...]]
		inner := strings.TrimSuffix(strings.TrimPrefix(reply, "*["), "]")
		cur, rest, _ := strings.Cut(inner, " ")
		cursor = strings.TrimPrefix(cur, "$")
		for _, k := range strings.Fields(strings.Trim(rest, "*[]")) {
			seen[strings.TrimPrefix(k, "$")] = true
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 20 {
		t.Fatalf("SCAN MATCH returned %d keys, want 20", len(seen))
	}
	if got := c.do("DBSIZE"); got != ":21" {
		t.Fatalf("DBSIZE = %s", got)
	}
	if got := c.do("FLUSHDB"); got != "+OK" {
		t.Fatalf("FLUSHDB = %s", got)
	}
	if got := c.do("DBSIZE"); got != ":0" {
		t.Fatalf("DBSIZE after flush = %s", got)
	}
}

// HELLO 3 must switch the framing of subsequent replies, not just the handshake.
func TestRESPHelloSwitchesProtocol(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)
	reply := c.do("HELLO", "3")
	if !strings.Contains(reply, "mocache") || !strings.Contains(reply, ":3") {
		t.Fatalf("HELLO 3 reply = %s", reply)
	}
	// In RESP3 a missing key is "_\r\n"; the test client normalises it to $-1,
	// so read the raw bytes here instead.
	if _, err := c.conn.Write([]byte("*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n")); err != nil {
		t.Fatal(err)
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "_\r\n" {
		t.Fatalf("RESP3 null = %q, want %q", line, "_\r\n")
	}
	if got := c.do("HELLO", "4"); got != "-NOPROTO unsupported protocol version" {
		t.Fatalf("HELLO 4 = %s", got)
	}
}

func TestRESPAuth(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{Password: "s3cret"})
	c := dial(t, addr)
	if got := c.do("GET", "k"); got != "-NOAUTH Authentication required." {
		t.Fatalf("unauthenticated GET = %s", got)
	}
	if got := c.do("PING"); got != "+PONG" {
		t.Fatalf("PING should not need auth: %s", got)
	}
	if got := c.do("AUTH", "wrong"); !strings.HasPrefix(got, "-WRONGPASS") {
		t.Fatalf("bad password = %s", got)
	}
	if got := c.do("AUTH", "s3cret"); got != "+OK" {
		t.Fatalf("AUTH = %s", got)
	}
	if got := c.do("SET", "k", "v"); got != "+OK" {
		t.Fatalf("authenticated SET = %s", got)
	}
}

func clusterTopo(t *testing.T, me string) *cluster.Manager {
	t.Helper()
	mgr, err := cluster.NewManager([]string{
		"127.0.0.1:7001=0-5460",
		"127.0.0.1:7002=5461-10922",
		"127.0.0.1:7003=10923-16383",
		"127.0.0.1:7004=replica-of:127.0.0.1:7001",
	}, me)
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	return mgr
}

// The routing contract: keys we own are served, keys we do not own get MOVED
// with the owner's address, and multi-key commands spanning slots are refused.
func TestRESPClusterRedirects(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{Cluster: clusterTopo(t, "127.0.0.1:7001")})
	c := dial(t, addr)

	// Slots: "abc" is 7638 (owned by 7002), "foo" is 12182 (7003), and
	// "hello" is 866, which is ours.
	if got := c.do("GET", "abc"); got != "-MOVED 7638 127.0.0.1:7002" {
		t.Fatalf("GET abc = %s", got)
	}
	if got := c.do("GET", "foo"); got != "-MOVED 12182 127.0.0.1:7003" {
		t.Fatalf("GET foo = %s", got)
	}
	if got := c.do("SET", "hello", "v"); got != "+OK" {
		t.Fatalf("SET hello = %s", got)
	}
	if got := c.do("GET", "hello"); got != "$v" {
		t.Fatalf("GET hello = %s", got)
	}
	if got := c.do("MGET", "hello", "foo"); got != "-CROSSSLOT Keys in request don't hash to the same slot" {
		t.Fatalf("cross-slot MGET = %s", got)
	}
	// Hash tags put both keys on one slot, so this must be allowed.
	c.do("MSET", "{hello}:a", "1", "{hello}:b", "2")
	if got := c.do("MGET", "{hello}:a", "{hello}:b"); got != "*[$1 $2]" {
		t.Fatalf("tagged MGET = %s", got)
	}
	// Keyless commands are never redirected.
	if got := c.do("PING"); got != "+PONG" {
		t.Fatalf("PING in cluster mode = %s", got)
	}
	if got := c.do("CLUSTER", "KEYSLOT", "foo"); got != ":12182" {
		t.Fatalf("CLUSTER KEYSLOT = %s", got)
	}
	if got := c.do("CLUSTER", "MYID"); len(got) != 41 {
		t.Fatalf("CLUSTER MYID = %s", got)
	}
}

// A replica serves its primary's slots only after READONLY, and never for writes.
func TestRESPReplicaReadonly(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{Cluster: clusterTopo(t, "127.0.0.1:7004")})
	c := dial(t, addr)

	if got := c.do("GET", "hello"); got != "-MOVED 866 127.0.0.1:7001" {
		t.Fatalf("replica read without READONLY = %s", got)
	}
	if got := c.do("READONLY"); got != "+OK" {
		t.Fatalf("READONLY = %s", got)
	}
	if got := c.do("GET", "hello"); got != "$-1" {
		t.Fatalf("replica read after READONLY = %s", got)
	}
	if got := c.do("SET", "hello", "v"); got != "-MOVED 866 127.0.0.1:7001" {
		t.Fatalf("write on replica must still be redirected: %s", got)
	}
	if got := c.do("READWRITE"); got != "+OK" {
		t.Fatalf("READWRITE = %s", got)
	}
	if got := c.do("GET", "hello"); got != "-MOVED 866 127.0.0.1:7001" {
		t.Fatalf("READWRITE did not clear the readonly flag: %s", got)
	}
}

func TestRESPClusterSlotsReply(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{Cluster: clusterTopo(t, "127.0.0.1:7001")})
	c := dial(t, addr)
	reply := c.do("CLUSTER", "SLOTS")
	// Three ranges, the first carrying a replica entry.
	if strings.Count(reply, "$127.0.0.1") != 4 {
		t.Fatalf("CLUSTER SLOTS = %s", reply)
	}
	for _, want := range []string{":0 :5460", ":5461 :10922", ":10923 :16383"} {
		if !strings.Contains(reply, want) {
			t.Fatalf("CLUSTER SLOTS missing range %q: %s", want, reply)
		}
	}
	info := c.do("CLUSTER", "INFO")
	if !strings.Contains(info, "cluster_enabled:1") || !strings.Contains(info, "cluster_state:ok") {
		t.Fatalf("CLUSTER INFO = %s", info)
	}
	nodes := c.do("CLUSTER", "NODES")
	if !strings.Contains(nodes, "myself,master") || !strings.Contains(nodes, "slave") {
		t.Fatalf("CLUSTER NODES = %s", nodes)
	}
	if !strings.Contains(nodes, "@17001") {
		t.Fatalf("CLUSTER NODES should advertise the bus port: %s", nodes)
	}
}

// Standalone mode must look like a plain Redis: no redirects, cluster disabled.
func TestRESPStandaloneHasNoRedirects(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)
	if got := c.do("SET", "foo", "v"); got != "+OK" {
		t.Fatalf("SET = %s", got)
	}
	if got := c.do("MGET", "foo", "bar", "baz"); got != "*[$v $-1 $-1]" {
		t.Fatalf("cross-slot MGET must be allowed standalone: %s", got)
	}
	if info := c.do("CLUSTER", "INFO"); !strings.Contains(info, "cluster_enabled:0") {
		t.Fatalf("CLUSTER INFO = %s", info)
	}
	if got := c.do("CLUSTER", "SLOTS"); got != "*[]" {
		t.Fatalf("CLUSTER SLOTS = %s", got)
	}
}

func TestRESPInfoReportsLiveState(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)
	c.do("SET", "k", "v")
	c.do("GET", "k")
	c.do("GET", "missing")
	info := c.do("INFO")
	for _, want := range []string{"server_name:mocache", "keyspace_hits:1", "keyspace_misses:1", "db0:keys=1"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO missing %q:\n%s", want, info)
		}
	}
}

// COMMAND is generated from the routing table, so the two cannot drift.
func TestRESPCommandIntrospection(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)
	if got := c.do("COMMAND", "COUNT"); !strings.HasPrefix(got, ":") {
		t.Fatalf("COMMAND COUNT = %s", got)
	}
	if got := c.do("COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"); got != "*[$a $b]" {
		t.Fatalf("COMMAND GETKEYS MSET = %s", got)
	}
	if got := c.do("COMMAND", "GETKEYS", "PING"); got != "-ERR The command has no key arguments" {
		t.Fatalf("COMMAND GETKEYS PING = %s", got)
	}
}

// Oversized values are refused as OOM rather than accepted and evicted blind.
func TestRESPOversizedValue(t *testing.T) {
	_, addr, _ := startRESP(t, RESPOptions{})
	c := dial(t, addr)
	if got := c.do("SET", "big", strings.Repeat("x", 5000)); !strings.HasPrefix(got, "-OOM") {
		t.Fatalf("oversized SET = %s", got)
	}
}
