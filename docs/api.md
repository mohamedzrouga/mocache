# HTTP API

Base URL: `http://<pod-dns>:8090`

| Method | Path | Request | Response |
|---|---|---|---|
| GET | `/get?key=` | — | **200** raw value bytes; **404** absent or expired |
| POST | `/set` | JSON `{"key": string, "value": string, "ttl_seconds": int}` | **200** empty; **400** bad JSON or empty key |
| DELETE | `/delete?key=` | — | **200** empty (idempotent) |
| GET | `/livez` | — | **200** `ok` while the process can run, **including during drain** |
| GET | `/readyz` | — | **200** `ok` when accepting traffic; **503** during drain |
| GET | `/healthz` | — | Same as `/readyz` (load-balancer friendly) |
| GET | `/metrics` | — | `hits`, `misses`, `evictions`, `item_count` as plain-text counters |

`/get` returns raw bytes (not JSON) on the hot path. `/set` values are JSON strings (UTF-8). For opaque bytes, use the unary RPC transport.

`ttl_seconds` ≤ 0 means no expiry.

Bodies are capped at 4 MiB.

## Probe mapping

Use **different** paths for liveness and readiness:

- Liveness → `/livez`. Must not fail during SIGTERM drain, or kubelet will SIGKILL a pod that is shutting down cleanly.
- Readiness → `/readyz`. Goes 503 as soon as SIGTERM is received so Endpoints stop sending new work.

See [operations.md](operations.md).
