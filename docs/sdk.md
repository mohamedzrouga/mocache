# SDKs

Code lives under [`sdk/`](../sdk). Runnable samples: [`sdk/examples/`](../sdk/examples).

Both clients:

- Hash with MD5, 100 virtual nodes by default, identical ring algorithm
- Take a **static** node list (HTTP URLs)
- Distinguish **miss** from **unavailable**
- Retry a broken connection once (rolling restart / crash)
- Expose `Close` so idle sockets are not leaked

## Go

```go
import (
    "time"
    mocache "github.com/med/mocache/sdk/go"
)

nodes := []string{
    "http://cache-0.cache-headless.svc.cluster.local:8090",
    "http://cache-1.cache-headless.svc.cluster.local:8090",
    "http://cache-2.cache-headless.svc.cluster.local:8090",
}

c := mocache.New(nodes, mocache.WithTimeout(time.Second))
defer c.Close()

// Fast path — same node list; RPC port defaults to 8091.
c = mocache.New(nodes, mocache.WithProtocol(mocache.ProtocolGRPC))

err := c.Set("user:123", []byte("some_value"), 300*time.Second)
val, ok, err := c.Get("user:123") // miss: nil, false, nil
err = c.Delete("user:123")
```

`*mocache.OpError` wraps timeouts (`IsTimeout()`) and connection failures.

Run the example:

```bash
go run ./sdk/examples/go
```

## Python

Python 3.13+, stdlib only.

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

with MoCacheClient(nodes, timeout=1.0) as cache:
    cache.set("user:123", "some_value", ttl_seconds=300)
    val = cache.get("user:123")   # None on miss
    cache.delete("user:123")
```

`protocol="grpc"` selects unary RPC (`rpc_port=8091`).

```bash
PYTHONPATH=sdk/python python3 sdk/examples/python/example.py
```

## Production guidance

Treat MoCache as best-effort. On `OpError` / `MoCacheError`, fall back to the authoritative source and optionally `Set` the result. Do not fail user requests because a cache node is rolling.
