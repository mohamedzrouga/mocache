# MoCache

In-house distributed cache: dumb Go nodes, smart clients. No Redis, no Valkey, no third-party runtime libraries — but it **speaks the Redis protocol**, so stock Redis clients can use it unmodified.

Cache nodes are independent LRU processes. Go and Python SDKs hash keys onto the same MD5 virtual-node ring and talk to exactly one node per call. There is no replication, persistence, or cluster membership — a node restart loses its shard, which is accepted for a recomputable cache.

**Docs:** [docs/README.md](docs/README.md) — architecture, API, SDK, operations, observability, Helm.
**Contributing / agents:** [AGENTS.md](AGENTS.md) — the invariants a change must not break.

## Layout

```
cmd/mocache          cache-node server
internal/cache       byte+item capped LRU
internal/obs         Prometheus, JSON logs, OTLP
internal/server      HTTP + RPC + drain
internal/protocol    unary RPC framing
internal/server      HTTP + RPC + drain
sdk/go               Go client
sdk/python           Python 3.13 client
sdk/examples         runnable client samples
helm/example         StatefulSet chart
lab                  MinIO + FastAPI + benchmark scenario
docs                 operational and API docs
```

## Transports

| Protocol | Port | Client | Notes |
|---|---|---|---|
| HTTP | 8090 | MoCache SDK (default) | `/get` `/set` `/delete` `/invalidate` `/livez` `/readyz` `/healthz` `/metrics` |
| Unary RPC (`grpc`) | 8091 | MoCache SDK | Persistent TCP, length-prefixed frames. Not `google.golang.org/grpc`. |
| RESP | 6379 | **any Redis client** | RESP2/RESP3, 56 commands, Redis Cluster slots. Off unless `-resp` is set. |

The MoCache SDKs route with an MD5 ring; Redis clients route with CRC16 slots. Both reach the same LRU, and they place keys differently — use one scheme per keyspace ([docs/cluster.md](docs/cluster.md)).

## Server

Requires Go 1.25+.

```bash
make build && make run
make docker-build    # or: make podman-build
make image           # podman if installed, else docker
```

```bash
go run ./cmd/mocache -http :8090 -rpc :8091 -capacity 100000 -max-bytes 67108864 -mem-limit 134217728
```

Three nodes locally, the same shape as the 3-replica StatefulSet:

```bash
make compose-up     # :8090 :8092 :8094 (RPC :8091 :8093 :8095)
make compose-down
```

On SIGTERM the process fails `/readyz`, waits `-drain` (default 5s), then closes listeners and tracked RPC sockets. `/livez` stays 200 during drain so Kubernetes does not SIGKILL a shutting-down pod.

## Redis clients

```bash
go run ./cmd/mocache -resp :6379
```

```python
import redis
r = redis.Redis(host="127.0.0.1", port=6379)   # redis-py, unmodified
r.set("user:1", "alice", ex=300)
```

Sharded across three nodes, every node takes the same peer list:

```bash
mocache -resp :6379 -cluster-announce 10.0.0.1:6379 \
  -cluster-peer 10.0.0.1:6379=0-5460 \
  -cluster-peer 10.0.0.2:6379=5461-10922 \
  -cluster-peer 10.0.0.3:6379=10923-16383
```

`RedisCluster` and `ClusterClient` then discover the shards from `CLUSTER SLOTS` and follow `MOVED` — no MoCache-specific code. Strings and TTLs only; no lists, hashes, pub/sub, or scripting. **No replication or failover yet**: a node that is down takes its slots with it. See [docs/redis.md](docs/redis.md) and [docs/cluster.md](docs/cluster.md).

Verify with the real clients: `make lab-up && make compat`.

## MoCache SDK clients

```go
import (
    "context"
    mocache "github.com/mohamedzrouga/mocache/sdk/go"
)

c := mocache.New(nodes, mocache.WithTimeout(time.Second))
defer c.Close()
val, ok, err := c.Get("user:123") // miss => (nil, false, nil)
n, err := c.InvalidatePrefix("user:")
res := <-c.GetAsync(context.Background(), "user:123")
```

```python
from mocache import MoCacheClient, AsyncMoCacheClient

with MoCacheClient(nodes, timeout=1.0) as cache:
    cache.set("user:123", "some_value", ttl_seconds=300)
    cache.invalidate_regex(r"^user:")

async with AsyncMoCacheClient(nodes) as cache:
    await cache.get("user:123")
```

Samples: `go run ./sdk/examples/go`, `sdk/examples/python/sync.py`, `asyncio_example.py`, `fastapi_app.py`.

Both SDKs retry a broken connection once so a rolling restart is usually a miss, not an application error. Still treat `OpError` / `MoCacheError` as “cache unavailable” and fall back to source data.

## Lab

`lab/` runs the scenario the cache exists for: random objects in MinIO (S3), a FastAPI service reading them cache-aside through MoCache, and a stdlib load generator that benchmarks cache-aside against origin-only.

```bash
make lab-up      # minio + api + 3 cache nodes
make lab-seed    # 2000 random objects (~67 MiB)
make lab-bench   # origin-only vs cache-aside: RPS, hit ratio, p50/p99, per-node metrics
make lab-down
```

See [lab/README.md](lab/README.md) for the benchmark flags and the failure scenarios (node down, rolling restart, eviction pressure, invalidation).

## Helm

```bash
helm upgrade --install mocache ./helm/example --set image.tag=0.1.0
```

See [docs/helm.md](docs/helm.md), [docs/operations.md](docs/operations.md), and [docs/observability.md](docs/observability.md).

## Tests

```bash
go test ./...
PYTHONPATH=sdk/python python3.13 -m unittest discover -s sdk/python/tests -v
make compat     # real redis-py and go-redis against a running lab cluster
```
