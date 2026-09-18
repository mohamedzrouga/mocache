# Observability

MoCache emits **JSON logs**, a **Prometheus** scrape endpoint, and optional **OTLP/HTTP JSON traces**. No Prometheus or OpenTelemetry client libraries are linked — the node stays stdlib-only.

## Logs

Stdout is JSON via `log/slog` (`msg`, `level`, plus fields).

| Flag | Default | Meaning |
|---|---|---|
| `-access-log` | off | One line per non-probe HTTP request (`method`, `path`, `code`, `dur_ms`) |

Errors (listen, shutdown, OTLP export, panics) are always logged.

## Prometheus

Scrape `GET /metrics` (`text/plain; version=0.0.4`).

| Metric | Type |
|---|---|
| `mocache_hits_total` | counter |
| `mocache_misses_total` | counter |
| `mocache_evictions_total` | counter |
| `mocache_invalidations_total` | counter |
| `mocache_items` / `mocache_items_max` | gauge |
| `mocache_bytes` / `mocache_bytes_max` | gauge |
| `mocache_http_requests_total` | counter |
| `mocache_http_errors_total` | counter |
| `mocache_in_flight` | gauge |
| `mocache_request_duration_seconds` | histogram |
| `mocache_resp_commands_total` | counter |
| `mocache_resp_errors_total` | counter |
| `mocache_resp_moved_total` | counter |
| `mocache_resp_ask_total` | counter |
| `mocache_resp_connections` | gauge |
| `mocache_resp_duration_seconds_sum` | counter |
| `mocache_cluster_enabled` | gauge |
| `mocache_cluster_slots_assigned` | gauge |
| `mocache_cluster_known_nodes` | gauge |
| `mocache_cluster_my_slots` | gauge |
| `mocache_cluster_failovers_total` | counter |
| `mocache_repl_role` | gauge |
| `mocache_repl_offset` | gauge |
| `mocache_repl_connected_replicas` | gauge |
| `mocache_repl_link_up` | gauge |
| `mocache_repl_full_resyncs_total` | counter |
| `go_goroutines` | gauge |
| `go_memstats_alloc_bytes` / `_sys_bytes` / `_heap_inuse_bytes` | gauge |

On a replicated cluster, alert on `mocache_repl_link_up == 0` (a replica that is not receiving its primary's stream is not protecting anything) and on `mocache_cluster_failovers_total` rising when nobody asked for it (the failure detector is too twitchy for the network — raise `-cluster-node-timeout`). A primary's `mocache_repl_connected_replicas` dropping to 0 means the next failure of that node loses its shard.

`mocache_resp_moved_total` is the cluster health signal to alert on: a sustained redirect rate means clients are routing on a stale slot map. `mocache_cluster_slots_assigned` below 16384 means part of the keyspace has no owner.

`mocache_resp_ask_total` counts the other redirect: a key whose slot is mid-migration and has already been copied to its new node. It should be non-zero only while a reshard is running, and fall back to flat when it finishes — a steady rate afterwards means a migration was opened with `CLUSTER SETSLOT ... MIGRATING` and never committed with `CLUSTER SETSLOT ... NODE`. See [cluster.md](cluster.md#resharding).

Point Prometheus at the **pod IP** (or a metrics Service), not only the headless cache DNS — headless is for client hashing.

## OpenTelemetry

Set `-otel-endpoint` or `OTEL_EXPORTER_OTLP_ENDPOINT` to an OTLP/HTTP collector (e.g. `http://otel-collector:4318`). Spans are JSON POSTed to `/v1/traces`.

- Queue is **bounded (256)**; if the collector is down, spans are **dropped** (never block or OOM the cache).
- `OTEL_SERVICE_NAME` / `-otel-service` (default `mocache`).

Empty endpoint disables tracing.

## Memory (cannot OOM the node)

1. **Item cap** `-capacity`
2. **Byte cap** `-max-bytes` (keys+values+overhead); LRU evicts until both caps hold
3. **Per-entry caps** `-max-value`, `-max-key`; oversized Set returns HTTP 413
4. **Request body cap** (1 MiB JSON) and RPC frame cap (4 MiB)
5. **Go soft limit** `-mem-limit` → `debug.SetMemoryLimit` so GC runs before the cgroup OOM killer
6. Helm example: 256Mi cache budget, 384Mi Go limit, 512Mi container limit

Watch `mocache_bytes` vs `mocache_bytes_max` and `go_memstats_heap_inuse_bytes`.
