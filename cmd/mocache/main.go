// Command mocache is a single cache-node process: HTTP + unary RPC in front of
// an in-memory LRU. There is no clustering. On SIGTERM the process fails
// readiness, waits -drain so kube-proxy can drop endpoints, then closes
// listeners and in-flight connections. Memory is not persisted — a crash or
// rolling restart simply starts empty, which is the intended cache behaviour.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/med/mocache/internal/cache"
	"github.com/med/mocache/internal/server"
)

func main() {
	httpAddr := flag.String("http", ":8090", "HTTP listen address")
	rpcAddr := flag.String("rpc", ":8091", "binary RPC (gRPC-style) listen address; empty disables")
	capacity := flag.Int("capacity", 100_000, "maximum number of cached items")
	drain := flag.Duration("drain", 5*time.Second, "after SIGTERM, wait this long with readiness false before closing sockets")
	janitor := flag.Duration("janitor", 30*time.Second, "expired-entry sweep interval; 0 disables")
	flag.Parse()

	if *capacity < 1 {
		log.Fatal("capacity must be >= 1")
	}

	store := cache.New(*capacity)
	store.StartJanitor(*janitor)
	defer store.Close()

	gate := server.NewGate()
	httpSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           server.NewMux(store, gate),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	errc := make(chan error, 2)
	go func() {
		log.Printf("http listening on %s (capacity=%d)", *httpAddr, *capacity)
		errc <- httpSrv.ListenAndServe()
	}()

	var rpcLn net.Listener
	rpcSrv := server.NewRPC(store)
	if *rpcAddr != "" {
		ln, err := net.Listen("tcp", *rpcAddr)
		if err != nil {
			log.Fatal(err)
		}
		rpcLn = ln
		go func() {
			log.Printf("rpc listening on %s", *rpcAddr)
			errc <- rpcSrv.Serve(ln)
		}()
	}

	gate.SetReady(true)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errc:
		log.Printf("listener error: %v", err)
	case <-ctx.Done():
		log.Printf("signal received, draining for %s", *drain)
	}

	// Fail probes first so the Service/Endpoints controller stops new traffic.
	gate.SetReady(false)
	time.Sleep(*drain)

	shutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Printf("http shutdown: %v", err)
		_ = httpSrv.Close()
	}
	if rpcLn != nil {
		_ = rpcLn.Close()
		rpcSrv.Close()
	}
	log.Println("shutdown complete")
}
