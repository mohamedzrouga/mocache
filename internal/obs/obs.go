// Package obs is process-level observability: JSON logs, Prometheus text, OTLP traces.
// No third-party SDKs — Prometheus exposition and OTLP/HTTP JSON are written by hand
// so the cache-node binary stays dependency-free.
package obs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
)

var (
	reqTotal    atomic.Uint64
	reqErrors   atomic.Uint64
	inflight    atomic.Int64
	histCount   atomic.Uint64
	histSumNs   atomic.Uint64
	histBuckets [11]atomic.Uint64

	// RESP (Redis-protocol) front end.
	respCmds  atomic.Uint64
	respErrs  atomic.Uint64
	respConns atomic.Int64
	respMoved atomic.Uint64
	respAsk   atomic.Uint64
	respSumNs atomic.Uint64

	// Replication, published on a timer by cmd/mocache.
	replIsReplica atomic.Bool
	replOffset    atomic.Uint64
	replConnected atomic.Int64
	replLinkUp    atomic.Bool
	replResyncs   atomic.Uint64
	replFailovers atomic.Uint64

	// Cluster view, published on a timer by cmd/mocache.
	clusterOn       atomic.Bool
	clusterAssigned atomic.Int64
	clusterKnown    atomic.Int64
	clusterMySlots  atomic.Int64
)

// Prometheus histogram bounds in seconds.
var buckets = [...]float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}

func boolGauge(b bool) int {
	if b {
		return 1
	}
	return 0
}

// InitJSONLogs replaces the default logger with JSON on stdout (cluster-friendly).
func InitJSONLogs() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
}

func RecordRequest(dur time.Duration, err bool) {
	reqTotal.Add(1)
	if err {
		reqErrors.Add(1)
	}
	ns := uint64(dur.Nanoseconds())
	histCount.Add(1)
	histSumNs.Add(ns)
	sec := dur.Seconds()
	for i, b := range buckets {
		if sec <= b {
			histBuckets[i].Add(1)
		}
	}
}

func InFlight(delta int64) { inflight.Add(delta) }

// RecordRESP counts one Redis-protocol command. err marks an error reply —
// including MOVED, which is an error frame on the wire but a normal event in a
// cluster, so it is also counted separately by RecordMoved.
func RecordRESP(dur time.Duration, err bool) {
	respCmds.Add(1)
	respSumNs.Add(uint64(dur.Nanoseconds()))
	if err {
		respErrs.Add(1)
	}
}

// RecordMoved counts a slot redirect. A high rate means clients are routing on
// a stale slot map — the first thing to look at after a topology change.
func RecordMoved() { respMoved.Add(1) }

// RecordAsk counts an ASK redirect, which means a key has already moved to the
// slot's new home while the reshard is still running. Unlike MOVED it is
// expected to be non-zero only while a reshard is in progress, and to stop when
// it finishes — a steady rate afterwards means a migration was never committed
// with CLUSTER SETSLOT NODE.
func RecordAsk() { respAsk.Add(1) }

// RESPConns tracks open Redis-protocol connections.
func RESPConns(delta int64) { respConns.Add(delta) }

// SetReplicationInfo publishes the live replication state for scraping. Called
// on a timer because gossip and failover change it at runtime.
func SetReplicationInfo(isReplica bool, offset uint64, replicas int, linkUp bool, resyncs, failovers uint64) {
	replIsReplica.Store(isReplica)
	replOffset.Store(offset)
	replConnected.Store(int64(replicas))
	replLinkUp.Store(linkUp)
	replResyncs.Store(resyncs)
	replFailovers.Store(failovers)
}

// SetClusterInfo publishes the static cluster view for scraping.
func SetClusterInfo(enabled bool, assigned, known, mySlots int) {
	clusterOn.Store(enabled)
	clusterAssigned.Store(int64(assigned))
	clusterKnown.Store(int64(known))
	clusterMySlots.Store(int64(mySlots))
}

// WritePrometheus emits the Prometheus text exposition format (0.0.4).
func WritePrometheus(w io.Writer, st cache.Stats) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	p := func(s string, a ...any) { _, _ = fmt.Fprintf(w, s, a...) }
	p("# HELP mocache_hits_total Cache hits.\n# TYPE mocache_hits_total counter\nmocache_hits_total %d\n", st.Hits)
	p("# HELP mocache_misses_total Cache misses (absent or expired).\n# TYPE mocache_misses_total counter\nmocache_misses_total %d\n", st.Misses)
	p("# HELP mocache_evictions_total LRU evictions.\n# TYPE mocache_evictions_total counter\nmocache_evictions_total %d\n", st.Evictions)
	p("# HELP mocache_invalidations_total Keys removed by prefix/regex invalidate.\n# TYPE mocache_invalidations_total counter\nmocache_invalidations_total %d\n", st.Invalidations)
	p("# HELP mocache_items Current item count.\n# TYPE mocache_items gauge\nmocache_items %d\n", st.ItemCount)
	p("# HELP mocache_items_max Configured item cap.\n# TYPE mocache_items_max gauge\nmocache_items_max %d\n", st.MaxItems)
	p("# HELP mocache_bytes Approximate bytes of keys+values+overhead.\n# TYPE mocache_bytes gauge\nmocache_bytes %d\n", st.Bytes)
	p("# HELP mocache_bytes_max Configured byte cap.\n# TYPE mocache_bytes_max gauge\nmocache_bytes_max %d\n", st.MaxBytes)
	p("# HELP mocache_http_requests_total HTTP requests handled.\n# TYPE mocache_http_requests_total counter\nmocache_http_requests_total %d\n", reqTotal.Load())
	p("# HELP mocache_http_errors_total HTTP 5xx / handler errors.\n# TYPE mocache_http_errors_total counter\nmocache_http_errors_total %d\n", reqErrors.Load())
	p("# HELP mocache_in_flight In-flight HTTP requests.\n# TYPE mocache_in_flight gauge\nmocache_in_flight %d\n", inflight.Load())
	p("# HELP mocache_request_duration_seconds Request duration.\n# TYPE mocache_request_duration_seconds histogram\n")
	var acc uint64
	for i, b := range buckets {
		acc = histBuckets[i].Load()
		p("mocache_request_duration_seconds_bucket{le=\"%g\"} %d\n", b, acc)
	}
	p("mocache_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", histCount.Load())
	p("mocache_request_duration_seconds_sum %s\n", strconv.FormatFloat(float64(histSumNs.Load())/1e9, 'f', 6, 64))
	p("mocache_request_duration_seconds_count %d\n", histCount.Load())
	p("# HELP mocache_resp_commands_total Redis-protocol commands handled.\n# TYPE mocache_resp_commands_total counter\nmocache_resp_commands_total %d\n", respCmds.Load())
	p("# HELP mocache_resp_errors_total Redis-protocol error replies (including redirects).\n# TYPE mocache_resp_errors_total counter\nmocache_resp_errors_total %d\n", respErrs.Load())
	p("# HELP mocache_resp_moved_total MOVED redirects sent; a rising rate means stale client slot maps.\n# TYPE mocache_resp_moved_total counter\nmocache_resp_moved_total %d\n", respMoved.Load())
	p("# HELP mocache_resp_ask_total ASK redirects sent during a live slot migration.\n# TYPE mocache_resp_ask_total counter\nmocache_resp_ask_total %d\n", respAsk.Load())
	p("# HELP mocache_resp_connections Open Redis-protocol connections.\n# TYPE mocache_resp_connections gauge\nmocache_resp_connections %d\n", respConns.Load())
	p("# HELP mocache_resp_duration_seconds_sum Total time spent executing Redis-protocol commands.\n# TYPE mocache_resp_duration_seconds_sum counter\nmocache_resp_duration_seconds_sum %s\n", strconv.FormatFloat(float64(respSumNs.Load())/1e9, 'f', 6, 64))
	p("# HELP mocache_cluster_enabled 1 when the node serves a slot range.\n# TYPE mocache_cluster_enabled gauge\nmocache_cluster_enabled %d\n", boolGauge(clusterOn.Load()))
	p("# HELP mocache_cluster_slots_assigned Slots with an owner across the cluster (16384 = complete).\n# TYPE mocache_cluster_slots_assigned gauge\nmocache_cluster_slots_assigned %d\n", clusterAssigned.Load())
	p("# HELP mocache_cluster_known_nodes Nodes in this node's cluster view.\n# TYPE mocache_cluster_known_nodes gauge\nmocache_cluster_known_nodes %d\n", clusterKnown.Load())
	p("# HELP mocache_cluster_my_slots Slots served by this node.\n# TYPE mocache_cluster_my_slots gauge\nmocache_cluster_my_slots %d\n", clusterMySlots.Load())
	p("# HELP mocache_repl_role 1 when this node is a replica, 0 when it is a primary.\n# TYPE mocache_repl_role gauge\nmocache_repl_role %d\n", boolGauge(replIsReplica.Load()))
	p("# HELP mocache_repl_offset Replication offset: a primary's write position, or a replica's applied position.\n# TYPE mocache_repl_offset gauge\nmocache_repl_offset %d\n", replOffset.Load())
	p("# HELP mocache_repl_connected_replicas Replicas currently streaming from this primary.\n# TYPE mocache_repl_connected_replicas gauge\nmocache_repl_connected_replicas %d\n", replConnected.Load())
	p("# HELP mocache_repl_link_up 1 when a replica's link to its primary is in sync.\n# TYPE mocache_repl_link_up gauge\nmocache_repl_link_up %d\n", boolGauge(replLinkUp.Load()))
	p("# HELP mocache_repl_full_resyncs_total Full resynchronisations this replica has performed.\n# TYPE mocache_repl_full_resyncs_total counter\nmocache_repl_full_resyncs_total %d\n", replResyncs.Load())
	p("# HELP mocache_cluster_failovers_total Slot-ownership changes this node has applied.\n# TYPE mocache_cluster_failovers_total counter\nmocache_cluster_failovers_total %d\n", replFailovers.Load())
	p("# HELP go_goroutines Number of goroutines.\n# TYPE go_goroutines gauge\ngo_goroutines %d\n", runtime.NumGoroutine())
	p("# HELP go_memstats_alloc_bytes Bytes allocated and still in use.\n# TYPE go_memstats_alloc_bytes gauge\ngo_memstats_alloc_bytes %d\n", ms.Alloc)
	p("# HELP go_memstats_sys_bytes Bytes obtained from the OS.\n# TYPE go_memstats_sys_bytes gauge\ngo_memstats_sys_bytes %d\n", ms.Sys)
	p("# HELP go_memstats_heap_inuse_bytes Heap bytes in use.\n# TYPE go_memstats_heap_inuse_bytes gauge\ngo_memstats_heap_inuse_bytes %d\n", ms.HeapInuse)
}

// Middleware records duration, in-flight, optional access logs, and OTLP spans.
func Middleware(next http.Handler, accessLog bool, traces *Exporter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		InFlight(1)
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		defer func() {
			InFlight(-1)
			d := time.Since(start)
			err := sw.code >= 500
			RecordRequest(d, err)
			if accessLog && r.URL.Path != "/metrics" && r.URL.Path != "/livez" && r.URL.Path != "/readyz" {
				slog.Info("http", "method", r.Method, "path", r.URL.Path, "code", sw.code, "dur_ms", d.Milliseconds())
			}
			if traces != nil {
				traces.Emit(r.Context(), r.Method+" "+r.URL.Path, start, d, sw.code)
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) {
	w.code = c
	w.ResponseWriter.WriteHeader(c)
}

// Exporter is a non-blocking OTLP/HTTP JSON tracer. The queue is bounded so a
// stuck collector cannot grow memory; spans are dropped rather than blocking.
type Exporter struct {
	url     string
	service string
	ch      chan span
	client  *http.Client
	stop    chan struct{}
	wg      sync.WaitGroup
}

type span struct {
	name  string
	start time.Time
	dur   time.Duration
	code  int
}

func NewOTLP(endpoint, service string) *Exporter {
	endpoint = strings.TrimRight(endpoint, "/")
	if endpoint == "" {
		return nil
	}
	if !strings.Contains(endpoint, "/v1/traces") {
		endpoint += "/v1/traces"
	}
	if service == "" {
		service = "mocache"
	}
	e := &Exporter{
		url:     endpoint,
		service: service,
		ch:      make(chan span, 256), // bounded: never OOM waiting on OTEL
		client:  &http.Client{Timeout: 3 * time.Second},
		stop:    make(chan struct{}),
	}
	e.wg.Add(1)
	go e.loop()
	return e
}

func (e *Exporter) Emit(_ context.Context, name string, start time.Time, dur time.Duration, code int) {
	if e == nil {
		return
	}
	select {
	case e.ch <- span{name: name, start: start, dur: dur, code: code}:
	default:
		// drop — observability must never stall or inflate the cache process
	}
}

func (e *Exporter) Close() {
	if e == nil {
		return
	}
	close(e.stop)
	e.wg.Wait()
}

func (e *Exporter) loop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.stop:
			return
		case s := <-e.ch:
			e.post(s)
		}
	}
}

func (e *Exporter) post(s span) {
	tid, sid := newIDs()
	startNs := s.start.UnixNano()
	endNs := startNs + s.dur.Nanoseconds()
	body := fmt.Sprintf(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":%q}}]},"scopeSpans":[{"spans":[{"traceId":%q,"spanId":%q,"name":%q,"kind":2,"startTimeUnixNano":"%d","endTimeUnixNano":"%d","status":{"code":%d}}]}]}]}`,
		e.service, tid, sid, s.name, startNs, endNs, map[bool]int{true: 2, false: 1}[s.code >= 500])
	req, err := http.NewRequest(http.MethodPost, e.url, bytes.NewReader([]byte(body)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		slog.Warn("otlp export failed", "err", err)
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
}

func newIDs() (traceID, spanID string) {
	var b [24]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:16]), hex.EncodeToString(b[16:])
}
