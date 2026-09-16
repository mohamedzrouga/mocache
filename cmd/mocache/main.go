// Command mocache is a single cache-node process: HTTP + unary RPC in front of
// an in-memory LRU. There is no clustering. On SIGTERM the process fails
// readiness, waits -drain so kube-proxy can drop endpoints, then closes
// listeners and in-flight connections. Memory is not persisted — a crash or
// rolling restart simply starts empty, which is the intended cache behaviour.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/obs"
	"github.com/mohamedzrouga/mocache/internal/server"
)

func main() {
	httpAddr := flag.String("http", ":8090", "HTTP listen address")
	rpcAddr := flag.String("rpc", ":8091", "binary RPC (gRPC-style) listen address; empty disables")
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

	errc := make(chan error, 2)
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
	slog.Info("shutdown complete")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
