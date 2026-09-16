package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/med/mocache/internal/cache"
)

const maxBody = 4 << 20

// Gate is the process-wide ready flag used to fail Kubernetes readiness
// immediately on SIGTERM so endpoints drop us before we close listeners.
type Gate struct {
	ready atomic.Bool
}

func NewGate() *Gate { return &Gate{} }

func (g *Gate) SetReady(v bool) { g.ready.Store(v) }

func (g *Gate) Ready() bool { return g.ready.Load() }

// NewMux exposes the HTTP API. Pass a Gate so /readyz can go false during drain.
// If g is nil the process is treated as ready (tests).
func NewMux(c *cache.Cache, g *Gate) http.Handler {
	if g == nil {
		g = NewGate()
		g.SetReady(true)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		val, found := c.Get(r.URL.Query().Get("key"))
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(val)
	})
	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			TTL   int    `json:"ttl_seconds"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var ttl time.Duration
		if req.TTL > 0 {
			ttl = time.Duration(req.TTL) * time.Second
		}
		c.Set(req.Key, []byte(req.Value), ttl)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		c.Delete(r.URL.Query().Get("key"))
		w.WriteHeader(http.StatusOK)
	})
	// /livez must stay 200 during drain so kube does not SIGKILL a shutting-down pod.
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !g.Ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	// /healthz is readiness: a load balancer should stop sending traffic here.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !g.Ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := c.Stats()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintf(w, "hits %d\nmisses %d\nevictions %d\nitem_count %d\n",
			s.Hits, s.Misses, s.Evictions, s.ItemCount)
	})
	return recoverHandler(mux)
}

// recoverHandler keeps one panicking request from taking down the process
// (important on a crash-loop-sensitive StatefulSet).
func recoverHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("http panic: %v", rec)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
