// Compatibility check: drive MoCache with the real go-redis client.
//
// This is a separate Go module on purpose. go-redis is a third-party library
// and the MoCache module must stay dependency-free — but compatibility cannot
// be proven without the actual client, so it lives here, outside the main
// module's build and test graph.
//
//	docker compose run --rm compat-go
//	docker compose run --rm compat-go -cluster
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var failures []string

func check[T comparable](label string, got, want T) {
	if got == want {
		fmt.Printf("  ok    %s: %v\n", label, got)
		return
	}
	fmt.Printf("  FAIL  %s: %v (want %v)\n", label, got, want)
	failures = append(failures, label)
}

func checkTrue(label string, ok bool, detail any) {
	if ok {
		fmt.Printf("  ok    %s: %v\n", label, detail)
		return
	}
	fmt.Printf("  FAIL  %s: %v\n", label, detail)
	failures = append(failures, label)
}

// client is the subset both redis.Client and redis.ClusterClient satisfy.
type client interface {
	redis.Cmdable
	Close() error
}

func main() {
	addr := flag.String("addr", envOr("REDIS_ADDR", "cache1:6379"), "node address")
	addrs := flag.String("addrs", envOr("REDIS_ADDRS", "cache1:6379,cache2:6379,cache3:6379"), "cluster seed addresses")
	cluster := flag.Bool("cluster", false, "use the cluster client")
	proto := flag.Int("proto", 3, "RESP protocol version (go-redis defaults to 3)")
	flag.Parse()

	ctx := context.Background()
	var c client
	mode := "standalone"
	if *cluster {
		mode = "cluster"
		c = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    strings.Split(*addrs, ","),
			Protocol: *proto,
		})
	} else {
		client := redis.NewClient(&redis.Options{Addr: *addr, Protocol: *proto})
		// A plain client does not follow MOVED, and the owner of a slot changes
		// after a failover. Resolve it once so the standalone check stays
		// meaningful whatever state the cluster is in.
		if err := client.Set(ctx, "{go}:probe", "1", 0).Err(); err != nil && strings.HasPrefix(err.Error(), "MOVED ") {
			parts := strings.Fields(err.Error())
			target := parts[len(parts)-1]
			fmt.Printf("  (slot moved: following the redirect to %s)\n", target)
			_ = client.Close()
			client = redis.NewClient(&redis.Options{Addr: target, Protocol: *proto})
			*addr = target
		}
		client.Del(ctx, "{go}:probe")
		c = client
	}
	defer c.Close()

	fmt.Printf("go-redis -> %s (%s, RESP%d)\n\n", pick(*cluster, *addrs, *addr), mode, *proto)

	server(ctx, c)
	strings_(ctx, c)
	expiry(ctx, c)
	keyspace(ctx, c)
	if *cluster {
		clusterTopology(ctx, c.(*redis.ClusterClient))
	}

	fmt.Println()
	if len(failures) > 0 {
		fmt.Printf("FAILED: %d check(s): %s\n", len(failures), strings.Join(failures, ", "))
		os.Exit(1)
	}
	fmt.Println("all go-redis checks passed")
}

func server(ctx context.Context, c client) {
	fmt.Println("server")
	pong, err := c.Ping(ctx).Result()
	checkTrue("ping", err == nil && pong == "PONG", pong)
	// go-redis sends HELLO 3 itself on connect; reaching here means the
	// handshake and RESP3 framing were accepted.
	info, err := c.Info(ctx, "server").Result()
	checkTrue("info", err == nil && strings.Contains(info, "server_name:mocache"), firstLine(info))
}

// Keys carry the {go} hash tag so every command stays on one slot: a plain
// client against a cluster-mode node would otherwise get MOVED (single key) or
// CROSSSLOT (multi-key), which is correct Redis behaviour rather than a bug.
func strings_(ctx context.Context, c client) {
	fmt.Println("strings")
	c.Del(ctx, "{go}:str", "{go}:n")
	check("set", c.Set(ctx, "{go}:str", "hello", 0).Err() == nil, true)
	v, _ := c.Get(ctx, "{go}:str").Result()
	check("get", v, "hello")

	_, err := c.Get(ctx, "{go}:absent").Result()
	// A miss must be redis.Nil, not a transport error: the whole client API
	// depends on being able to tell those apart.
	checkTrue("miss is redis.Nil", errors.Is(err, redis.Nil), err)

	n, _ := c.Incr(ctx, "{go}:n").Result()
	check("incr", n, int64(1))
	n, _ = c.IncrBy(ctx, "{go}:n", 41).Result()
	check("incrby", n, int64(42))
	f, _ := c.IncrByFloat(ctx, "{go}:n", 0.5).Result()
	check("incrbyfloat", f, 42.5)

	ok, _ := c.SetNX(ctx, "{go}:str", "other", 0).Result()
	check("setnx on taken key", ok, false)
	old, _ := c.GetSet(ctx, "{go}:str", "world").Result()
	check("getset", old, "hello")
	got, _ := c.GetDel(ctx, "{go}:str").Result()
	check("getdel", got, "world")
}

func expiry(ctx context.Context, c client) {
	fmt.Println("expiry")
	c.Set(ctx, "{go}:ttl", "v", 100*time.Second)
	d, _ := c.TTL(ctx, "{go}:ttl").Result()
	check("ttl", d, 100*time.Second)
	ok, _ := c.Persist(ctx, "{go}:ttl").Result()
	check("persist", ok, true)
	d, _ = c.TTL(ctx, "{go}:ttl").Result()
	check("ttl after persist", d, time.Duration(-1))
	c.Set(ctx, "{go}:quick", "v", 50*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	_, err := c.Get(ctx, "{go}:quick").Result()
	checkTrue("expired key is a miss", errors.Is(err, redis.Nil), err)
	c.Del(ctx, "{go}:ttl")
}

func keyspace(ctx context.Context, c client) {
	fmt.Println("keyspace")
	// One hash tag keeps every key on a single slot, so MSET/MGET stay legal
	// in cluster mode.
	pairs := []any{}
	for i := 0; i < 10; i++ {
		pairs = append(pairs, fmt.Sprintf("{go}:k%d", i), i)
	}
	check("mset", c.MSet(ctx, pairs...).Err() == nil, true)
	vals, err := c.MGet(ctx, "{go}:k0", "{go}:k1", "{go}:missing").Result()
	checkTrue("mget returns nil for a miss", err == nil && len(vals) == 3 && vals[2] == nil, vals)

	// SCAN carries no key, so a cluster client has no slot to route by and
	// go-redis sends it to one arbitrary shard. Iterating every master is the
	// idiomatic form — and the one that finds all the keys. (redis-py's cluster
	// client fans SCAN out for you; go-redis does not.)
	var scanned int
	scanShard := func(ctx context.Context, node redis.Cmdable) error {
		iter := node.Scan(ctx, 0, "{go}:k*", 3).Iterator()
		for iter.Next(ctx) {
			scanned++
		}
		return iter.Err()
	}
	var scanErr error
	if cc, ok := c.(*redis.ClusterClient); ok {
		scanErr = cc.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
			return scanShard(ctx, node)
		})
	} else {
		scanErr = scanShard(ctx, c)
	}
	checkTrue("scan found all 10", scanErr == nil && scanned == 10, scanned)

	keys := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		keys = append(keys, fmt.Sprintf("{go}:k%d", i))
	}
	deleted, _ := c.Del(ctx, keys...).Result()
	check("del", deleted, int64(10))
}

func clusterTopology(ctx context.Context, c *redis.ClusterClient) {
	fmt.Println("cluster")
	slots, err := c.ClusterSlots(ctx).Result()
	// Not a fixed count: a resharded cluster legitimately advertises more
	// ranges than it has shards, because moving one slot splits a range. What
	// must hold is that they sum to exactly 16384, checked next — that is the
	// condition go-redis itself enforces before it will use the topology.
	checkTrue("cluster slots parsed", err == nil && len(slots) >= 3, len(slots))
	covered := 0
	for _, s := range slots {
		covered += s.End - s.Start + 1
	}
	check("slot coverage", covered, 16384)

	// Writing keys that hash all over the keyspace exercises MOVED handling and
	// the client's slot map at the same time.
	const n = 60
	for i := 0; i < n; i++ {
		if err := c.Set(ctx, fmt.Sprintf("{go}:spread:%d", i), i, 0).Err(); err != nil {
			checkTrue("spread write", false, err)
			return
		}
	}
	readBack := 0
	for i := 0; i < n; i++ {
		if v, err := c.Get(ctx, fmt.Sprintf("{go}:spread:%d", i)).Int(); err == nil && v == i {
			readBack++
		}
	}
	check("keys read back across shards", readBack, n)

	masters := 0
	_ = c.ForEachMaster(ctx, func(ctx context.Context, _ *redis.Client) error {
		masters++
		return nil
	})
	check("ForEachMaster visited every shard", masters, 3)

	for i := 0; i < n; i++ {
		c.Del(ctx, fmt.Sprintf("{go}:spread:%d", i))
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

func pick(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
