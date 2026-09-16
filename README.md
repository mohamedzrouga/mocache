# MoCache

In-house distributed cache: dumb Go nodes, smart clients. No Redis, no Valkey, no third-party runtime libraries.

Cache nodes are independent LRU processes. Go and Python SDKs hash keys onto the same MD5 virtual-node ring and talk to exactly one node per call. There is no replication, persistence, or cluster membership — a node restart loses its shard, which is accepted for a recomputable cache.

## Transports

| Protocol | Port | Client option | Notes |
|---|---|---|---|
| HTTP | 8090 | `http` (default) | SRS API: `/get`, `/set`, `/delete`, `/healthz`, `/metrics` |
| Unary RPC (`grpc`) | 8091 | `grpc` | Persistent TCP, length-prefixed binary frames. Faster than HTTP. **Not** `google.golang.org/grpc` — that would add a third-party runtime. Same Get/Set/Delete semantics. |

## Server

Requires Go 1.25+.

```bash
go run ./cmd/mocache -http :8090 -rpc :8091 -capacity 100000
```

Flags: `-http`, `-rpc` (empty disables RPC), `-capacity` (item-count cap; LRU evicts when full). TTL `0` means no expiry.

## Go SDK

```go
import (
    "time"
    "github.com/med/mocache"
)

nodes := []string{
    "http://cache-0.cache-headless.svc.cluster.local:8090",
    "http://cache-1.cache-headless.svc.cluster.local:8090",
    "http://cache-2.cache-headless.svc.cluster.local:8090",
}

// HTTP
c := mocache.New(nodes, mocache.WithTimeout(time.Second))

// Fast unary RPC (gRPC-style). Same node list; RPC port defaults to 8091.
c = mocache.New(nodes, mocache.WithProtocol(mocache.ProtocolGRPC), mocache.WithTimeout(time.Second))

if err := c.Set("user:123", []byte("some_value"), 300*time.Second); err != nil { /* network */ }
val, ok, err := c.Get("user:123") // miss => (nil, false, nil); network => err
_ = c.Delete("user:123")
```

Network/timeout failures return `*mocache.OpError`. A miss is not an error.

## Python SDK

Python 3.13+, stdlib only (`urllib`, `socket`).

```bash
pip install -e sdk/python
```

```python
from mocache import MoCacheClient, MoCacheError

nodes = [
    "http://cache-0.cache-headless.svc.cluster.local:8090",
    "http://cache-1.cache-headless.svc.cluster.local:8090",
    "http://cache-2.cache-headless.svc.cluster.local:8090",
]
cache = MoCacheClient(nodes, timeout=1.0)                 # HTTP
cache = MoCacheClient(nodes, protocol="grpc", timeout=1.0)  # fast RPC

cache.set("user:123", "some_value", ttl_seconds=300)
val = cache.get("user:123")   # None on miss
cache.delete("user:123")
```

`MoCacheError` is raised on timeout/connection failure only.

Both SDKs use MD5, `vnodes=100`, and “first ring position at or after the key hash” so they route identically against the same node list.

## HTTP API

| Method | Path | Request | Response |
|---|---|---|---|
| GET | `/get?key=` | — | 200 raw bytes; 404 miss |
| POST | `/set` | `{"key","value","ttl_seconds"}` | 200 |
| DELETE | `/delete?key=` | — | 200 (idempotent) |
| GET | `/healthz` | — | 200 `ok` |
| GET | `/metrics` | — | `hits`, `misses`, `evictions`, `item_count` |

## OpenShift

`deploy/openshift.yaml` is a 3-pod StatefulSet behind a headless Service (`cache-0.cache-headless` …). Probes hit `/healthz`. No volume is required. Size `memory` limits with headroom above `capacity × average value × 1.3`.

Scaling node count is manual: change the StatefulSet replica count **and** the static node list in every client.

## RPC framing

Each message is `uint32be length` + payload. Payload starts with magic `MOC1`, version `1`. Ops: Get=1, Set=2, Delete=3, Health=4. Status: OK=0, Miss=1, Error=2. Max frame 4 MiB.

## Tests

```bash
go test ./...
python3.13 -m unittest discover -s sdk/python/tests -v
```
