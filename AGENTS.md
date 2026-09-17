# AGENTS.md

Working notes for coding agents in this repo. Humans: [README.md](README.md) and [docs/](docs/README.md) are the real documentation; this file is about how to change the code without breaking its constraints.

## What this is

MoCache is a memcached-style cache built in-house because Redis/Valkey and other third-party cache products are not an option here. **Nodes are dumb, clients are smart**: each node is an independent LRU process with no knowledge of any other node, and the client picks which node owns a key. No replication, no persistence, no membership protocol — yet. A node restart loses its shard, which the cache design accepts because the data is recomputable.

Since the Redis-compatibility work, a node serves three front ends over one LRU: HTTP, unary RPC, and **RESP on :6379** for stock Redis clients, including Redis Cluster slot routing and `MOVED` redirects. Replication and automatic failover are designed but not built ([docs/cluster.md](docs/cluster.md)).

```
cmd/mocache          cache-node server (flags, drain, wiring)
internal/cache       byte- and item-capped LRU with TTL, plus the Redis string ops
internal/cluster     CRC16 slots, hash tags, cluster topology
internal/obs         Prometheus text, JSON logs, OTLP/HTTP traces
internal/protocol    unary RPC framing
internal/resp        RESP2/RESP3 codec
internal/server      HTTP mux, RPC server, RESP server, readiness gate
sdk/go               Go client (ring + HTTP/RPC transports)
sdk/python           Python 3.13 client (sync + async)
sdk/examples         runnable samples
helm/example         StatefulSet chart
deploy/              OpenShift manifests
docs/                architecture, API, protocol, SDK, operations, observability
lab/                 MinIO + FastAPI + benchmark scenario, and the Redis client
                     compatibility checks (disposable)
```

## Hard rules

1. **No third-party runtime dependencies** in `cmd/`, `internal/`, or either SDK. `go.mod` has no `require` block and must stay that way; the Python SDK's `dependencies` list is empty. Prometheus exposition, OTLP JSON, the RPC framing, the RESP codec and CRC16 are all hand-written for exactly this reason. `lab/` is the one exception — it may pip-install whatever it needs, and `lab/compat/go` is a **separate Go module** so that go-redis never enters the main `go.mod`. Nothing in `lab/` may be imported by the SDK.
2. **The two rings must stay byte-identical.** `sdk/go/ring.go` and `HashRing` in `sdk/python/mocache/client.py` both build virtual nodes labelled `{nodeURL}:{i}`, hash with MD5, and pick the first ring position at or after the key hash, wrapping to index 0. The orderings must agree: Go sorts and compares the raw 16-byte digests, Python reads the same digest as a big-endian unsigned integer and bisects. Changing the label format, the hash, the integer interpretation, or the default `vnodes=100` in one language without the other silently splits the keyspace: writes land on one node and reads on another. Change both, in the same commit, and update `docs/architecture.md`.
3. **`/livez` must not fail during drain.** `/readyz` and `/healthz` go 503 on SIGTERM so Endpoints drop the pod; `/livez` stays 200 so the kubelet does not SIGKILL a pod that is shutting down cleanly. See `docs/operations.md`.
4. **A cache miss is not an error.** HTTP `/get` returns 404, the Go SDK returns `(nil, false, nil)`, the Python SDK returns `None`. Only network and timeout failures raise `OpError` / `MoCacheError`. Do not "improve" this into an exception.
5. **The node cannot be allowed to OOM.** Every path into memory is capped: item count, total bytes, per-value, per-key, 1 MiB HTTP body, 4 MiB RPC frame, and `debug.SetMemoryLimit`. Adding a new way to store or buffer bytes means adding its cap too.
6. **Observability never blocks the cache.** The OTLP queue is bounded at 256 and drops spans rather than waiting on a collector. Keep it that way.
7. **CRC16 must stay CRC-16/XMODEM.** `internal/cluster` hashes keys exactly as Redis does; the check value over `"123456789"` is `0x31C3` and is asserted in the tests. Every Redis client computes slots itself, so a change here does not produce an error — it produces silent misrouting.
8. **Routing decisions happen before execution.** A command that belongs to another node must return `MOVED` without touching the LRU. Adding a command means giving it correct `first`/`last`/`step` key positions in the table in `internal/server/resp_commands.go`; that table also generates the `COMMAND` reply, so the two cannot drift.
9. **RESP read-modify-write commands are atomic.** `INCR`, `APPEND`, `SET NX`, `GETDEL` and friends run as one critical section in `internal/cache/ops.go`. Do not reimplement them as Get-then-Set in the server: concurrent clients would lose updates.

## Conventions

- Go 1.25, `gofmt`. Python 3.13, `from __future__ import annotations`, full type hints, standard library only.
- Comments explain *why* (a constraint, a failure mode, an ordering requirement), not what the line does. Match the density already in the file — these files are commented sparsely and deliberately.
- Public Go identifiers and every Python module get a doc comment naming the invariant they protect.
- Values are copied on `Set`/`Get` so callers cannot mutate entries behind the mutex. Preserve that in any new cache method.
- Errors name the operation, the key, and the node (`mocache unavailable get "user:123" via http://cache-1:8090: ...`) — that is what makes a failure debuggable when one node out of N is sick.

## Commands

```bash
make build            # native binary -> bin/mocache
make test             # go test ./...
make test-python      # PYTHONPATH=sdk/python python3 -m unittest discover -s sdk/python/tests
make run              # bin/mocache -http :8090 -rpc :8091
make image            # podman if installed, else docker

make compose-up       # 3 local nodes (docker-compose.yml)
make lab-up           # MinIO + API + 3 nodes (lab/)
make lab-seed         # upload random objects to MinIO
make lab-bench        # origin-only vs cache-aside benchmark
make lab-down
```

Run `make test` **and** `make test-python` after touching either SDK — ring changes only show up when both suites run. After touching anything under `internal/resp`, `internal/cluster`, or the RESP server, also run `make lab-up && make compat`: Go tests prove the server does what we think, the real clients prove it does what *they* think.

Large integers in Helm values render as `2.68435456e+08` unless passed through `int64`, and the binary rejects that. Keep the `int64` wrapper on numeric args in `templates/statefulset.yaml`.

## Changing things

| Change | Also do this |
|---|---|
| Ring, hash, or vnode default | Both SDKs, both test suites, `docs/architecture.md` |
| A RESP command | The table in `internal/server/resp_commands.go` (arity + key positions), `internal/server/resp_test.go`, the command list in `docs/redis.md` |
| Cluster routing or `CLUSTER` replies | `internal/cluster`, `internal/server/resp_cluster.go`, `docs/cluster.md`, and re-run `make compat` — the real clients are the test that matters |
| A new cache operation | `internal/cache/ops.go` under one lock, with byte accounting and the size caps |
| HTTP endpoint or status code | `internal/server/http_test.go`, both SDKs, `docs/api.md` |
| RPC frame layout | `internal/protocol/`, both SDK transports, `docs/protocol.md` — the frame has a magic and a version; bump the version rather than redefining fields |
| New process flag | `cmd/mocache/main.go`, `docs/operations.md`, `helm/example/values.yaml` + `templates/statefulset.yaml`, `deploy/openshift.yaml`, both compose files |
| New metric | `internal/obs/obs.go`, `docs/observability.md` |
| Node count | `helm/example/values.yaml` **and** every client's static node list — there is no rebalancing protocol, and ~1/N of keys move |

## The lab

`lab/` runs the scenario the cache exists for: random objects in MinIO (S3), a FastAPI service reading them cache-aside through MoCache, and a stdlib asyncio load generator that benchmarks cache-aside against origin-only and prints hit ratio, latency percentiles, and per-node `/metrics` deltas. It is the fastest way to see a change's effect end to end — and to check that a node going down degrades to misses rather than 5xx. See [lab/README.md](lab/README.md).

The lab is disposable: nothing in it is shipped, and breaking it is cheap. Breaking a hard rule above is not.

## Direction

The original design ruled out replication, membership and persistence. That decision has been **partly reversed by the project owner**:

- **Being built:** Redis-protocol compatibility and Redis Cluster sharding — done; see `docs/redis.md`.
- **Next, designed in `docs/cluster.md`:** replication, gossip failure detection, and failover by majority vote among primaries, following Redis Cluster's model. Bus framing is ours, not Redis's — clients must be compatible, other servers never will be.
- **Planned separately:** a native graph engine with GraphQL. It does not belong in the cache node: LRU eviction and TTL would silently corrupt query results, and a graph's data is not recomputable, so it needs durability the cache deliberately lacks. It should be its own binary in this repo, reusing the cluster layer.

Still out of scope: depending on an external cache product, and adding persistence to the **cache** node (a cache restart is expected to come back empty).

When implementing failover, do not skip the epoch/majority machinery to "make it work first". Two nodes claiming one slot is the failure mode the whole design exists to prevent, and it does not show up in a smoke test — it shows up in production, as data that depends on which node you asked.
