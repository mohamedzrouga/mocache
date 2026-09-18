// Command mocache is a single cache-node process: HTTP, unary RPC, and the
// Redis protocol in front of one in-memory LRU. On SIGTERM the process fails
// readiness, waits -drain so kube-proxy can drop endpoints, then closes
// listeners and in-flight connections. Memory is not persisted — a crash or
// rolling restart simply starts empty, which is the intended cache behaviour.
//
// Cluster mode: every node is given the same -cluster-peer list and derives the
// same slot map from it. With -cluster-bus enabled the nodes also gossip over a
// private bus, replicate primary to replica, and promote a replica by majority
// vote of the primaries when one fails. See docs/cluster.md.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/cluster"
	"github.com/mohamedzrouga/mocache/internal/node"
	"github.com/mohamedzrouga/mocache/internal/obs"
	"github.com/mohamedzrouga/mocache/internal/server"
)

// peerList collects repeated -cluster-peer flags.
type peerList []string

func (p *peerList) String() string { return strings.Join(*p, ",") }

func (p *peerList) Set(v string) error {
	// One flag may also carry a comma-separated list, which is friendlier in
	// container environments where flags come from a single env var.
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*p = append(*p, part)
		}
	}
	return nil
}

func main() {
	httpAddr := flag.String("http", ":8090", "HTTP listen address")
	rpcAddr := flag.String("rpc", ":8091", "binary RPC (gRPC-style) listen address; empty disables")
	respAddr := flag.String("resp", "", "Redis-protocol (RESP) listen address, e.g. :6379; empty disables")
	respPass := flag.String("resp-password", os.Getenv("MOCACHE_PASSWORD"), "require AUTH on the RESP port; empty disables auth")
	respIdle := flag.Duration("resp-idle", 0, "close idle RESP connections after this long; 0 = never (Redis default)")
	var peers peerList
	flag.Var(&peers, "cluster-peer", "cluster member as host:port=<slots|replica-of:host:port>; repeatable. Every node takes the same list")
	announce := flag.String("cluster-announce", "", "this node's host:port as it appears in -cluster-peer (required with -cluster-peer)")
	clusterBus := flag.Bool("cluster-bus", true, "run the cluster bus: gossip, replication and automatic failover (requires -cluster-peer)")
	busAddr := flag.String("cluster-bus-addr", "", "cluster bus listen address; default is the RESP port + 10000")
	nodeTimeout := flag.Duration("cluster-node-timeout", 5*time.Second, "peer silence before it is suspected; failover timings derive from this")
	failoverDelay := flag.Duration("cluster-failover-delay", 500*time.Millisecond, "base wait before a replica stands for election")
	replBacklog := flag.Int64("repl-backlog-bytes", 32<<20, "replication backlog per primary; a replica that falls further behind resynchronises")
	capacity := flag.Int("capacity", 100_000, "maximum number of cached items")
	maxBytes := flag.Int64("max-bytes", 64<<20, "approximate max bytes of keys+values+overhead")
	maxValue := flag.Int("max-value", 1<<20, "max bytes per value")
	maxKey := flag.Int("max-key", 4096, "max bytes per key")
	memLimit := flag.Int64("mem-limit", 0, "Go soft memory limit in bytes (debug.SetMemoryLimit); 0 = unset")
	drain := flag.Duration("drain", 5*time.Second, "after SIGTERM, wait this long with readiness false before closing sockets")
	janitor := flag.Duration("janitor", 30*time.Second, "expired-entry sweep interval; 0 disables")
	accessLog := flag.Bool("access-log", false, "JSON access log every non-probe HTTP request")
	otelEP := flag.String("otel-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "OTLP/HTTP traces endpoint; empty disables")
	otelSvc := flag.String("otel-service", getenv("OTEL_SERVICE_NAME", "mocache"), "OTEL service.name")
	flag.Parse()

	if *capacity < 1 {
		slog.Error("capacity must be >= 1")
		os.Exit(1)
	}

	obs.InitJSONLogs()

	// Soft cap the Go heap below the cgroup limit so GC runs before the OOM killer.
	if *memLimit > 0 {
		debug.SetMemoryLimit(*memLimit)
		slog.Info("memory limit", "bytes", *memLimit)
	}

	store := cache.NewWithLimits(cache.Limits{
		MaxItems: *capacity,
		MaxBytes: *maxBytes,
		MaxValue: *maxValue,
		MaxKey:   *maxKey,
	})
	store.StartJanitor(*janitor)
	defer store.Close()

	traces := obs.NewOTLP(*otelEP, *otelSvc)
	defer traces.Close()

	mgr, err := buildCluster(peers, *announce, *respAddr)
	if err != nil {
		slog.Error("cluster configuration", "err", err)
		os.Exit(1)
	}
	topo := mgr.View()
	obs.SetClusterInfo(topo.Enabled(), topo.AssignedSlots(), len(topo.Nodes()), topo.Myself().SlotsCount())

	// The clustering runtime is what makes nodes replicate and fail over. It
	// only runs in cluster mode: with a single node there is nothing to gossip
	// with and nothing to promote.
	var runtime *node.Cluster
	if topo.Enabled() {
		slog.Info("cluster mode",
			"announce", topo.Myself().Addr(),
			"node_id", topo.Myself().ID,
			"role", topo.Myself().Role.String(),
			"my_slots", topo.Myself().SlotsCount(),
			"nodes", len(topo.Nodes()),
			"state", topo.State())
		if topo.State() != "ok" {
			slog.Warn("cluster slot coverage incomplete; clients will see CLUSTERDOWN for unassigned slots",
				"assigned", topo.AssignedSlots(), "total", cluster.SlotCount)
		}
		if *clusterBus {
			runtime = node.New(mgr, store, node.Options{
				BusAddr:          *busAddr,
				NodeTimeout:      *nodeTimeout,
				FailoverDelay:    *failoverDelay,
				ReplBacklogBytes: *replBacklog,
			})
			if err := runtime.Start(); err != nil {
				slog.Error("cluster bus", "err", err)
				os.Exit(1)
			}
			defer runtime.Close()
			go publishClusterMetrics(mgr, runtime)
		} else {
			slog.Warn("cluster bus disabled: no replication, no failover, a node that dies takes its slots with it")
		}
	}

	gate := server.NewGate()
	httpSrv := &http.Server{
		Addr: *httpAddr,
		Handler: server.NewMuxOpts(store, gate, server.Options{
			AccessLog: *accessLog,
			Traces:    traces,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	errc := make(chan error, 3)
	go func() {
		slog.Info("http listening", "addr", *httpAddr, "capacity", *capacity, "max_bytes", *maxBytes)
		errc <- httpSrv.ListenAndServe()
	}()

	var rpcLn net.Listener
	rpcSrv := server.NewRPC(store)
	if *rpcAddr != "" {
		ln, err := net.Listen("tcp", *rpcAddr)
		if err != nil {
			slog.Error("rpc listen", "err", err)
			os.Exit(1)
		}
		rpcLn = ln
		go func() {
			slog.Info("rpc listening", "addr", *rpcAddr)
			errc <- rpcSrv.Serve(ln)
		}()
	}

	var respLn net.Listener
	respSrv := server.NewRESP(store, gate, server.RESPOptions{
		Cluster:  mgr,
		Runtime:  respRuntime(runtime),
		Password: *respPass,
		Idle:     *respIdle,
	})
	if *respAddr != "" {
		ln, err := net.Listen("tcp", *respAddr)
		if err != nil {
			slog.Error("resp listen", "err", err)
			os.Exit(1)
		}
		respLn = ln
		go func() {
			slog.Info("resp listening", "addr", *respAddr, "cluster", topo.Enabled(), "auth", *respPass != "")
			errc <- respSrv.Serve(ln)
		}()
	}

	gate.SetReady(true)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errc:
		slog.Error("listener error", "err", err)
	case <-ctx.Done():
		slog.Info("signal received, draining", "drain", drain.String())
	}

	gate.SetReady(false)
	time.Sleep(*drain)

	shutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.Error("http shutdown", "err", err)
		_ = httpSrv.Close()
	}
	if rpcLn != nil {
		_ = rpcLn.Close()
		rpcSrv.Close()
	}
	if respLn != nil {
		_ = respLn.Close()
		respSrv.Close()
	}
	slog.Info("shutdown complete")
}

// buildCluster turns the -cluster-peer flags into live cluster state. With no
// peers the node is standalone: it answers for every key and reports
// cluster_enabled:0, which is what a non-cluster redis client expects.
func buildCluster(peers []string, announce, respAddr string) (*cluster.Manager, error) {
	host, port := splitAddr(respAddr, 6379)
	if len(peers) == 0 {
		if announce != "" {
			h, p := splitAddr(announce, port)
			host, port = h, p
		}
		return cluster.StandaloneManager(host, port), nil
	}
	if announce == "" {
		return nil, errors.New("-cluster-announce is required with -cluster-peer: a node must know which peer entry is itself")
	}
	return cluster.NewManager(peers, announce)
}

// respRuntime avoids handing the RESP server a non-nil interface wrapping a nil
// pointer, which would make its nil checks pass and then panic.
func respRuntime(c *node.Cluster) server.Runtime {
	if c == nil {
		return nil
	}
	return c
}

// publishClusterMetrics keeps the scrape endpoint current: topology changes
// come from gossip, not from configuration, so these cannot be set once at
// startup.
func publishClusterMetrics(mgr *cluster.Manager, rt *node.Cluster) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		v := mgr.View()
		me := v.Myself()
		obs.SetClusterInfo(v.Enabled(), v.AssignedSlots(), len(v.Nodes()), me.SlotsCount())
		st := rt.ReplicationStatus()
		obs.SetReplicationInfo(st.Role == "slave", st.Offset, st.ConnectedReplicas,
			st.MasterLinkStatus == "up", st.FullResyncs, mgr.Failovers())
	}
}

// splitAddr accepts ":6379", "host:port" or "host", filling in a default port.
// A bare ":port" listen address is announced as 127.0.0.1 because a redirect
// must send clients somewhere they can actually connect.
func splitAddr(addr string, defPort int) (string, int) {
	if addr == "" {
		return "127.0.0.1", defPort
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, defPort
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = defPort
	}
	return host, port
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
