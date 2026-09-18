# HTTP API

Base URL: `http://<pod-dns>:8090`

This is one of three front ends over the same LRU: HTTP here, unary RPC in [protocol.md](protocol.md), and the Redis protocol in [redis.md](redis.md).

| Method | Path | Request | Response |
|---|---|---|---|
| GET | `/get?key=` | — | **200** raw value bytes; **404** absent or expired |
| POST | `/set` | JSON `{"key": string, "value": string, "ttl_seconds": int}` | **200** empty; **400** bad JSON or empty key |
| DELETE | `/delete?key=` | — | **200** empty (idempotent) |
| POST | `/invalidate` | JSON `{"prefix":"user:"}` **or** `{"regex":"^user:"}` | **200** `{"deleted":N}`; **400** bad pattern |
| GET | `/livez` | — | **200** `ok` while the process can run, **including during drain** |
| GET | `/readyz` | — | **200** `ok` when accepting traffic; **503** during drain |
| GET | `/healthz` | — | Same as `/readyz` (load-balancer friendly) |
| GET | `/metrics` | — | Prometheus text (hits, bytes, histograms, Go memstats) |

`/get` returns raw bytes (not JSON) on the hot path. `/set` values are JSON strings (UTF-8). For opaque bytes, use the unary RPC transport.

`ttl_seconds` ≤ 0 means no expiry.

Bodies are capped at 1 MiB for JSON `/set` and 4 MiB for RPC frames. Entries larger than `-max-value` / `-max-bytes` return **413**.

## Probe mapping

Use **different** paths for liveness and readiness:

- Liveness → `/livez`. Must not fail during SIGTERM drain, or kubelet will SIGKILL a pod that is shutting down cleanly.
- Readiness → `/readyz`. Goes 503 as soon as SIGTERM is received so Endpoints stop sending new work.

See [operations.md](operations.md).
