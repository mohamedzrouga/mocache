# Operations

MoCache nodes are **stateless**. Restarts, crashes, and rolling upgrades always come up with an empty LRU. That is not a defect.

## Process flags

| Flag | Default | Meaning |
|---|---|---|
| `-http` | `:8090` | HTTP listen address |
| `-rpc` | `:8091` | Unary RPC listen address; empty disables |
| `-capacity` | `100000` | Max items (LRU evicts beyond this) |
| `-max-bytes` | `64Mi` | Max approximate payload bytes |
| `-max-value` / `-max-key` | `1Mi` / `4Ki` | Reject oversized entries (HTTP 413) |
| `-mem-limit` | `0` | `debug.SetMemoryLimit`; keep below cgroup memory |
| `-drain` | `5s` | After SIGTERM: `/readyz` = 503, then wait, then close sockets |
| `-janitor` | `30s` | Sweep expired keys; `0` disables |
| `-access-log` | off | JSON access logs |
| `-otel-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/HTTP traces; empty disables |

## Signals and drain

1. kubelet sends **SIGTERM** (or you Ctrl-C).
2. Process sets readiness false → `/readyz` and `/healthz` return 503. `/livez` stays 200 so the kubelet does not SIGKILL a draining pod.
3. Sleep `-drain` so kube-proxy / CNI can drop Endpoints.
4. `http.Server.Shutdown` (finish in-flight HTTP, no new conns).
5. Close the RPC listener and every tracked TCP conn, wait for handler goroutines.
6. Stop the janitor. Exit 0.

`terminationGracePeriodSeconds` must exceed `-drain` plus shutdown (Helm default 30s vs 5s drain).

## Rollout (StatefulSet)

Use **OrderedReady** + **RollingUpdate** (the example chart does). One pod is replaced at a time:

- The new pod starts empty and becomes Ready via `/readyz`.
- The old pod drains as above.
- Keys that hashed to that ordinal miss until clients refill them (~1/N of the keyspace).
- The other pods keep their in-memory data.

Do **not** point liveness at `/healthz` or `/readyz`. Those go 503 on drain.

A **PodDisruptionBudget** (`minAvailable: N-1`) prevents voluntary eviction from taking more than one node at a time.

## Crashes

PID gone → kubelet restarts the container → empty cache, `/readyz` 200 once listening. Clients retry a reset TCP session once; further errors surface as `OpError` / `MoCacheError`. Consumers should treat that as a miss.

## Scaling node count

Manual, both sides:

1. Change StatefulSet `replicas` **and** the static node list in every SDK.
2. Expect a hit-rate dip: consistent hashing moves ~1/N keys (virtual nodes reduce hot spots, they do not eliminate moves).

There is no rebalancing protocol.

## Memory

The LRU cannot grow past `-capacity` items. Size `resources.limits.memory` with headroom: `capacity × average value × 1.3` (map/list + Go runtime). The janitor reclaims expired entries that would otherwise sit until they became the LRU tail.

## Local run

```bash
go run ./cmd/mocache -http :8090 -rpc :8091 -capacity 100000
curl -s localhost:8090/livez
```
