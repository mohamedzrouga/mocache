# MoCache

In-house distributed cache: dumb Go nodes, smart clients. No Redis, no Valkey, no third-party runtime libraries.

Cache nodes are independent LRU processes. Go and Python SDKs hash keys onto the same MD5 virtual-node ring and talk to exactly one node per call. There is no replication, persistence, or cluster membership — a node restart loses its shard, which is accepted for a recomputable cache.

**Docs:** [docs/README.md](docs/README.md) — architecture, API, SDK, operations, observability, Helm.

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
docs                 operational and API docs
```

## Transports

| Protocol | Port | Client option | Notes |
|---|---|---|---|
| HTTP | 8090 | `http` (default) | `/get` `/set` `/delete` `/invalidate` `/livez` `/readyz` `/healthz` `/metrics` |
| Unary RPC (`grpc`) | 8091 | `grpc` | Persistent TCP, length-prefixed frames. Not `google.golang.org/grpc`. |

## Server

Requires Go 1.25+.

```bash
go run ./cmd/mocache -http :8090 -rpc :8091 -capacity 100000 -max-bytes 67108864 -mem-limit 134217728
```

On SIGTERM the process fails `/readyz`, waits `-drain` (default 5s), then closes listeners and tracked RPC sockets. `/livez` stays 200 during drain so Kubernetes does not SIGKILL a shutting-down pod.

## Clients

```go
import (
    "context"
    mocache "github.com/med/mocache/sdk/go"
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

## Helm

```bash
helm upgrade --install mocache ./helm/example --set image.tag=0.1.0
```

See [docs/helm.md](docs/helm.md), [docs/operations.md](docs/operations.md), and [docs/observability.md](docs/observability.md).

## Tests

```bash
go test ./...
PYTHONPATH=sdk/python python3.13 -m unittest discover -s sdk/python/tests -v
```
