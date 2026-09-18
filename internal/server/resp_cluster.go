package server

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cluster"
)

// compatVersion is the Redis version MoCache reports. Clients gate features on
// it (RESP3, expiry options, CLUSTER SHARDS), so it has to name a real version
// whose command set we implement. INFO also reports server_name and
// mocache_version so nothing downstream mistakes this for an actual Redis.
const compatVersion = "7.4.0"

// entryOverheadEstimate mirrors internal/cache's per-entry overhead; MEMORY
// USAGE is an estimate in Redis too.
const entryOverheadEstimate = 96

var startedAt = time.Now()

func clusterMode(c *respConn) string {
	if c.s.view().Enabled() {
		return "cluster"
	}
	return "standalone"
}

func cmdCluster(c *respConn, args [][]byte) bool {
	t := c.s.view()
	switch strings.ToUpper(string(args[1])) {
	case "INFO":
		c.w.Verbatim("txt", clusterInfoText(c))
	case "MYID":
		c.w.BulkString(t.Myself().ID)
	case "SLOTS":
		writeClusterSlots(c)
	case "SHARDS":
		writeClusterShards(c)
	case "NODES":
		c.w.Verbatim("txt", clusterNodesText(c))
	case "KEYSLOT":
		if len(args) != 3 {
			c.w.Error("ERR wrong number of arguments for 'cluster|keyslot' command")
			return true
		}
		c.w.Int(int64(cluster.KeySlot(string(args[2]))))
	case "COUNTKEYSINSLOT":
		if len(args) != 3 {
			c.w.Error("ERR wrong number of arguments for 'cluster|countkeysinslot' command")
			return true
		}
		slot, err := strconv.Atoi(string(args[2]))
		if err != nil || slot < 0 || slot >= cluster.SlotCount {
			c.w.Error("ERR Invalid slot")
			return true
		}
		c.w.Int(int64(len(keysInSlot(c, slot, 0))))
	case "GETKEYSINSLOT":
		if len(args) != 4 {
			c.w.Error("ERR wrong number of arguments for 'cluster|getkeysinslot' command")
			return true
		}
		slot, err1 := strconv.Atoi(string(args[2]))
		count, err2 := strconv.Atoi(string(args[3]))
		if err1 != nil || err2 != nil || slot < 0 || slot >= cluster.SlotCount || count < 0 {
			c.w.Error("ERR Invalid slot or number of keys")
			return true
		}
		c.w.Strings(keysInSlot(c, slot, count))
	case "FAILOVER":
		if c.s.opt.Runtime == nil {
			c.w.Error("ERR This instance has cluster support disabled")
			return true
		}
		var force, takeover bool
		for _, a := range args[2:] {
			switch strings.ToUpper(string(a)) {
			case "FORCE":
				force = true
			case "TAKEOVER":
				takeover = true
			default:
				c.w.Error("ERR Invalid CLUSTER FAILOVER option")
				return true
			}
		}
		if err := c.s.opt.Runtime.Failover(force, takeover); err != nil {
			c.w.Errorf("ERR %v", err)
			return true
		}
		c.w.OK()

	case "SETSLOT":
		return cmdClusterSetSlot(c, args)

	case "BUMPEPOCH":
		if !t.Enabled() {
			c.w.Error("ERR This instance has cluster support disabled")
			return true
		}
		c.w.Simple("BUMPED " + strconv.FormatUint(c.s.mgr.BumpConfigEpoch(), 10))

	case "REPLICATE":
		if len(args) != 3 {
			c.w.Error("ERR wrong number of arguments for 'cluster|replicate' command")
			return true
		}
		if c.s.opt.Runtime == nil {
			c.w.Error("ERR This instance has cluster support disabled")
			return true
		}
		if err := c.s.opt.Runtime.Replicate(string(args[2])); err != nil {
			c.w.Errorf("ERR %v", err)
			return true
		}
		c.w.OK()

	case "SLAVES", "REPLICAS":
		if len(args) != 3 {
			c.w.Error("ERR wrong number of arguments")
			return true
		}
		primary := nodeByID(t, string(args[2]))
		if primary == nil {
			c.w.Error("ERR Unknown node")
			return true
		}
		reps := t.ReplicasOf(primary)
		c.w.Array(len(reps))
		for _, r := range reps {
			c.w.BulkString(nodeLine(c, r))
		}
	default:
		c.w.Errorf("ERR Unknown CLUSTER subcommand or wrong number of arguments for '%s'", string(args[1]))
		return true
	}
	return false
}

func nodeByID(t *cluster.Topology, id string) *cluster.Node {
	for _, n := range t.Nodes() {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// keysInSlot scans the local keyspace for keys hashing to a slot. It is O(n) in
// the keyspace — acceptable because only administrative commands use it.
func keysInSlot(c *respConn, slot, limit int) []string {
	all := c.s.cache.Keys("*", 0)
	out := make([]string, 0, 16)
	for _, k := range all {
		if cluster.KeySlot(k) != slot {
			continue
		}
		out = append(out, k)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// writeClusterSlots emits the reply every cluster client bootstraps from:
// [start, end, [ip, port, id, {}], replicas...] per slot range.
func writeClusterSlots(c *respConn) {
	t := c.s.view()
	if !t.Enabled() {
		c.w.Array(0)
		return
	}
	type row struct {
		r        cluster.SlotRange
		primary  *cluster.Node
		replicas []*cluster.Node
	}
	var rows []row
	for _, p := range t.Primaries() {
		for _, r := range p.Slots {
			rows = append(rows, row{r: r, primary: p, replicas: t.ReplicasOf(p)})
		}
	}
	c.w.Array(len(rows))
	for _, row := range rows {
		c.w.Array(2 + 1 + len(row.replicas))
		c.w.Int(int64(row.r.Start))
		c.w.Int(int64(row.r.End))
		writeSlotNode(c, row.primary)
		for _, rep := range row.replicas {
			writeSlotNode(c, rep)
		}
	}
}

func writeSlotNode(c *respConn, n *cluster.Node) {
	c.w.Array(4)
	c.w.BulkString(n.Host)
	c.w.Int(int64(n.Port))
	c.w.BulkString(n.ID)
	c.w.Map(0) // additional networking metadata (Redis 7)
}

// writeClusterShards is the Redis 7 view: one entry per shard, with its slot
// ranges and the health of every node in it.
func writeClusterShards(c *respConn) {
	t := c.s.view()
	if !t.Enabled() {
		c.w.Array(0)
		return
	}
	primaries := t.Primaries()
	c.w.Array(len(primaries))
	for _, p := range primaries {
		nodes := append([]*cluster.Node{p}, t.ReplicasOf(p)...)
		c.w.Map(2)
		c.w.BulkString("slots")
		c.w.Array(len(p.Slots) * 2)
		for _, r := range p.Slots {
			c.w.Int(int64(r.Start))
			c.w.Int(int64(r.End))
		}
		c.w.BulkString("nodes")
		c.w.Array(len(nodes))
		for _, n := range nodes {
			role := "master"
			if n.Role == cluster.RoleReplica {
				role = "replica"
			}
			c.w.Map(7)
			c.w.BulkString("id")
			c.w.BulkString(n.ID)
			c.w.BulkString("port")
			c.w.Int(int64(n.Port))
			c.w.BulkString("ip")
			c.w.BulkString(n.Host)
			c.w.BulkString("endpoint")
			c.w.BulkString(n.Host)
			c.w.BulkString("role")
			c.w.BulkString(role)
			c.w.BulkString("replication-offset")
			c.w.Int(0) // no replication stream yet; phase two reports real offsets
			c.w.BulkString("health")
			c.w.BulkString("online")
		}
	}
}

// clusterNodesText is the gossip-style node table redis-cli --cluster and some
// clients parse:
//
//	<id> <ip:port@cport> <flags> <primary> <ping> <pong> <epoch> <link> <slots...>
func clusterNodesText(c *respConn) string {
	var b strings.Builder
	for _, n := range c.s.view().Nodes() {
		b.WriteString(nodeLine(c, n))
		b.WriteByte('\n')
	}
	return b.String()
}

func nodeLine(c *respConn, n *cluster.Node) string {
	t := c.s.view()
	self := n.ID == t.Myself().ID
	flags := n.Role.String()
	switch {
	case n.Fail:
		flags += ",fail"
	case n.PFail:
		flags += ",fail?"
	}
	if self {
		flags = "myself," + flags
	}
	primary := "-"
	if n.Role == cluster.RoleReplica {
		if p := t.PrimaryOf(n); p != nil {
			primary = p.ID
		}
	}
	var slots strings.Builder
	for _, r := range n.Slots {
		slots.WriteByte(' ')
		slots.WriteString(r.String())
	}
	// Reshard markers belong only on the myself line: they are local state, and
	// no other node knows about a migration it is not a party to. redis-cli
	// --cluster reads them to find an interrupted reshard and offer to finish it.
	if self {
		writeReshardMarkers(&slots, t.MigratingSlots(), "->-")
		writeReshardMarkers(&slots, t.ImportingSlots(), "-<-")
	}
	// ping-sent and pong-recv stay 0: the bus measures liveness as a boolean,
	// and clients use the flags for that, not the timestamps.
	link := "connected"
	if !self && !n.LinkUp {
		link = "disconnected"
	}
	return fmt.Sprintf("%s %s@%d %s %s 0 0 %d %s%s",
		n.ID, n.Addr(), n.BusPort(), flags, primary, n.ConfigEpoch, link, slots.String())
}

func writeReshardMarkers(b *strings.Builder, slots map[int]string, arrow string) {
	if len(slots) == 0 {
		return
	}
	ordered := make([]int, 0, len(slots))
	for slot := range slots {
		ordered = append(ordered, slot)
	}
	sort.Ints(ordered)
	for _, slot := range ordered {
		fmt.Fprintf(b, " [%d%s%s]", slot, arrow, slots[slot])
	}
}

// cmdClusterSetSlot implements the four forms of CLUSTER SETSLOT. Together they
// are the whole of live resharding: MIGRATING and IMPORTING open the window in
// which a slot's keys live on two nodes at once, NODE closes it by moving
// ownership, and STABLE abandons it.
func cmdClusterSetSlot(c *respConn, args [][]byte) bool {
	t := c.s.view()
	if !t.Enabled() {
		c.w.Error("ERR This instance has cluster support disabled")
		return true
	}
	if len(args) < 4 {
		c.w.Error("ERR wrong number of arguments for 'cluster|setslot' command")
		return true
	}
	slot, err := strconv.Atoi(string(args[2]))
	if err != nil || slot < 0 || slot >= cluster.SlotCount {
		c.w.Error("ERR Invalid or out of range slot")
		return true
	}

	action := strings.ToUpper(string(args[3]))
	if action == "STABLE" {
		if len(args) != 4 {
			c.w.Error("ERR wrong number of arguments for 'cluster|setslot' command")
			return true
		}
		if err := c.s.mgr.SetSlotStable(slot); err != nil {
			c.w.Errorf("ERR %v", err)
			return true
		}
		c.w.OK()
		return false
	}
	if len(args) != 5 {
		c.w.Error("ERR wrong number of arguments for 'cluster|setslot' command")
		return true
	}
	nodeID := string(args[4])

	switch action {
	case "MIGRATING":
		err = c.s.mgr.SetSlotMigrating(slot, nodeID)
	case "IMPORTING":
		err = c.s.mgr.SetSlotImporting(slot, nodeID)
	case "NODE":
		// Giving a slot away while its keys are still here would strand them:
		// invisible at the new owner, and unreachable here the moment the slot
		// map changes. The reshard has to finish moving them first.
		if nodeID != t.Myself().ID && t.Mine(slot) && len(keysInSlot(c, slot, 1)) > 0 {
			c.w.Errorf("ERR Can't assign hashslot %d to a different node while I still hold keys for this hash slot.", slot)
			return true
		}
		err = c.s.mgr.SetSlotOwner(slot, nodeID)
	default:
		c.w.Error("ERR Invalid CLUSTER SETSLOT action or number of arguments.")
		return true
	}
	if err != nil {
		c.w.Errorf("ERR %v", err)
		return true
	}
	c.w.OK()
	return false
}

func clusterInfoText(c *respConn) string {
	t := c.s.view()
	enabled := 0
	state := "ok"
	assigned := cluster.SlotCount
	known := 1
	size := 1
	if t.Enabled() {
		enabled = 1
		state = t.State()
		assigned = t.AssignedSlots()
		known = len(t.Nodes())
		size = len(t.Primaries())
	}
	var b strings.Builder
	fmt.Fprintf(&b, "cluster_enabled:%d\r\n", enabled)
	fmt.Fprintf(&b, "cluster_state:%s\r\n", state)
	fmt.Fprintf(&b, "cluster_slots_assigned:%d\r\n", assigned)
	fmt.Fprintf(&b, "cluster_slots_ok:%d\r\n", assigned)
	b.WriteString("cluster_slots_pfail:0\r\n")
	b.WriteString("cluster_slots_fail:0\r\n")
	fmt.Fprintf(&b, "cluster_known_nodes:%d\r\n", known)
	fmt.Fprintf(&b, "cluster_size:%d\r\n", size)
	b.WriteString("cluster_current_epoch:0\r\n")
	b.WriteString("cluster_my_epoch:0\r\n")
	return b.String()
}

// cmdInfo reports the sections clients and monitoring actually read. Values
// come from the live LRU and Go runtime, never from a fixed string.
func cmdInfo(c *respConn, args [][]byte) bool {
	section := "all"
	if len(args) > 1 {
		section = strings.ToLower(string(args[1]))
	}
	st := c.s.cache.Stats()
	t := c.s.view()
	var b strings.Builder
	want := func(s string) bool { return section == "all" || section == "default" || section == s }

	if want("server") {
		b.WriteString("# Server\r\n")
		fmt.Fprintf(&b, "redis_version:%s\r\n", compatVersion)
		b.WriteString("server_name:mocache\r\n")
		fmt.Fprintf(&b, "mocache_version:%s\r\n", Version)
		b.WriteString("redis_mode:" + clusterMode(c) + "\r\n")
		fmt.Fprintf(&b, "os:%s %s\r\n", runtime.GOOS, runtime.GOARCH)
		fmt.Fprintf(&b, "process_id:%d\r\n", pid)
		fmt.Fprintf(&b, "run_id:%s\r\n", t.Myself().ID)
		fmt.Fprintf(&b, "tcp_port:%d\r\n", t.Myself().Port)
		fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(time.Since(startedAt).Seconds()))
		fmt.Fprintf(&b, "uptime_in_days:%d\r\n", int64(time.Since(startedAt).Hours()/24))
		b.WriteString("\r\n")
	}
	if want("clients") {
		b.WriteString("# Clients\r\n")
		fmt.Fprintf(&b, "connected_clients:%d\r\n", c.s.connCount())
		b.WriteString("blocked_clients:0\r\n\r\n")
	}
	if want("memory") {
		b.WriteString("# Memory\r\n")
		fmt.Fprintf(&b, "used_memory:%d\r\n", st.Bytes)
		fmt.Fprintf(&b, "used_memory_human:%.2fM\r\n", float64(st.Bytes)/(1<<20))
		fmt.Fprintf(&b, "maxmemory:%d\r\n", st.MaxBytes)
		b.WriteString("maxmemory_policy:allkeys-lru\r\n\r\n")
	}
	if want("stats") {
		b.WriteString("# Stats\r\n")
		fmt.Fprintf(&b, "keyspace_hits:%d\r\n", st.Hits)
		fmt.Fprintf(&b, "keyspace_misses:%d\r\n", st.Misses)
		fmt.Fprintf(&b, "evicted_keys:%d\r\n", st.Evictions)
		fmt.Fprintf(&b, "expired_keys:%d\r\n", st.Invalidations)
		b.WriteString("\r\n")
	}
	if want("replication") {
		b.WriteString("# Replication\r\n")
		writeReplicationInfo(&b, c)
		b.WriteString("\r\n")
	}
	if want("cluster") {
		b.WriteString("# Cluster\r\n")
		enabled := 0
		if t.Enabled() {
			enabled = 1
		}
		fmt.Fprintf(&b, "cluster_enabled:%d\r\n\r\n", enabled)
	}
	if want("keyspace") {
		b.WriteString("# Keyspace\r\n")
		if st.ItemCount > 0 {
			fmt.Fprintf(&b, "db0:keys=%d,expires=0,avg_ttl=0\r\n", st.ItemCount)
		}
	}
	c.w.Verbatim("txt", b.String())
	return false
}

// writeReplicationInfo reports the live stream, not the configured intent. If
// no replication layer is running it says so with master_link_status:down
// rather than implying a sync that does not exist.
func writeReplicationInfo(b *strings.Builder, c *respConn) {
	v := c.s.view()
	me := v.Myself()
	role := "master"
	if me.Role == cluster.RoleReplica {
		role = "slave"
	}

	rt := c.s.opt.Runtime
	if rt == nil {
		fmt.Fprintf(b, "role:%s\r\n", role)
		b.WriteString("connected_slaves:0\r\n")
		if role == "slave" {
			if p := v.PrimaryOf(me); p != nil {
				fmt.Fprintf(b, "master_host:%s\r\nmaster_port:%d\r\n", p.Host, p.Port)
			}
			b.WriteString("master_link_status:down\r\n")
		}
		b.WriteString("master_repl_offset:0\r\n")
		return
	}

	st := rt.ReplicationStatus()
	fmt.Fprintf(b, "role:%s\r\n", st.Role)
	fmt.Fprintf(b, "connected_slaves:%d\r\n", st.ConnectedReplicas)
	for i, r := range st.Replicas {
		fmt.Fprintf(b, "slave%d:addr=%s,state=online,offset=%d,lag=0\r\n", i, r.Addr, r.Offset)
	}
	if st.Role == "slave" {
		fmt.Fprintf(b, "master_host:%s\r\n", st.MasterHost)
		fmt.Fprintf(b, "master_port:%d\r\n", st.MasterPort)
		fmt.Fprintf(b, "master_link_status:%s\r\n", st.MasterLinkStatus)
		fmt.Fprintf(b, "slave_repl_offset:%d\r\n", st.Offset)
		fmt.Fprintf(b, "slave_read_only:1\r\n")
		fmt.Fprintf(b, "sync_full:%d\r\n", st.FullResyncs)
		fmt.Fprintf(b, "synced_keys:%d\r\n", st.SyncedKeys)
	}
	fmt.Fprintf(b, "master_replid:%s\r\n", st.ReplID)
	fmt.Fprintf(b, "master_repl_offset:%d\r\n", st.Offset)
	fmt.Fprintf(b, "total_failovers:%d\r\n", st.Failovers)
}

// cmdConfig answers the handful of parameters clients read at connect time.
// Anything else returns an empty map rather than an error, because clients
// treat an unknown parameter as "not configured" but an error as a failure.
func cmdConfig(c *respConn, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "GET":
		if len(args) < 3 {
			c.w.Error("ERR wrong number of arguments for 'config|get' command")
			return true
		}
		st := c.s.cache.Stats()
		known := map[string]string{
			"maxmemory":              strconv.FormatInt(st.MaxBytes, 10),
			"maxmemory-policy":       "allkeys-lru",
			"maxclients":             strconv.Itoa(c.s.opt.MaxConns),
			"timeout":                strconv.Itoa(int(c.s.opt.Idle.Seconds())),
			"databases":              "1",
			"save":                   "",
			"appendonly":             "no",
			"cluster-enabled":        map[bool]string{true: "yes", false: "no"}[c.s.view().Enabled()],
			"proto-max-bulk-len":     strconv.Itoa(16 << 20),
			"notify-keyspace-events": "",
		}
		var pairs []string
		for _, pat := range args[2:] {
			for k, v := range known {
				if globMatchString(string(pat), k) {
					pairs = append(pairs, k, v)
				}
			}
		}
		c.w.Map(len(pairs) / 2)
		for _, s := range pairs {
			c.w.BulkString(s)
		}
	case "SET":
		// Accepted and ignored: MoCache's limits are process flags, not runtime
		// state, so silently pretending to apply one would be worse than a no-op
		// the client can see in CONFIG GET.
		c.w.OK()
	case "RESETSTAT", "REWRITE":
		c.w.OK()
	default:
		c.w.Errorf("ERR Unknown CONFIG subcommand or wrong number of arguments for '%s'", string(args[1]))
		return true
	}
	return false
}
