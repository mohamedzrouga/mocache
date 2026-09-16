package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/med/mocache/internal/cache"
)

const maxBody = 4 << 20

func NewMux(c *cache.Cache) http.Handler {
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
		w.Write(val)
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
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := c.Stats()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "hits %d\nmisses %d\nevictions %d\nitem_count %d\n",
			s.Hits, s.Misses, s.Evictions, s.ItemCount)
	})
	return mux
}
