package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/obs"
)

const maxBody = 1 << 20 // keep JSON /set well under the per-value cap

// Gate is the process-wide ready flag used to fail Kubernetes readiness
// immediately on SIGTERM so endpoints drop us before we close listeners.
type Gate struct {
	ready atomic.Bool
}

func NewGate() *Gate { return &Gate{} }

func (g *Gate) SetReady(v bool) { g.ready.Store(v) }

func (g *Gate) Ready() bool { return g.ready.Load() }

// Options tune HTTP observability. Zero value is safe for tests.
type Options struct {
	AccessLog bool
	Traces    *obs.Exporter
}

// NewMux exposes the HTTP API. Pass a Gate so /readyz can go false during drain.
// If g is nil the process is treated as ready (tests).
func NewMux(c *cache.Cache, g *Gate) http.Handler {
	return NewMuxOpts(c, g, Options{})
}

func NewMuxOpts(c *cache.Cache, g *Gate, opt Options) http.Handler {
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
		// Cap the decoder so a huge JSON blob cannot allocate past maxBody.
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var ttl time.Duration
		if req.TTL > 0 {
			ttl = time.Duration(req.TTL) * time.Second
		}
		if err := c.Set(req.Key, []byte(req.Value), ttl); err != nil {
			if errors.Is(err, cache.ErrTooLarge) {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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
	// Invalidate is a broadcast-on-the-client operation: this handler only
	// clears matching keys on *this* node. Prefix is cheaper; regex uses RE2.
	mux.HandleFunc("/invalidate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Prefix string `json:"prefix"`
			Regex  string `json:"regex"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var n int
		var err error
		switch {
		case req.Prefix != "":
			n = c.InvalidatePrefix(req.Prefix)
		case req.Regex != "":
			n, err = c.InvalidateRegex(req.Regex)
		default:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"deleted":` + strconv.Itoa(n) + `}`))
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
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !g.Ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		obs.WritePrometheus(w, c.Stats())
	})
	h := recoverHandler(mux)
	return obs.Middleware(h, opt.AccessLog, opt.Traces)
}

// recoverHandler keeps one panicking request from taking down the process.
func recoverHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("http panic", "panic", rec)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
