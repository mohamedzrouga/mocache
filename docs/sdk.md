# SDKs

Code lives under [`sdk/`](../sdk). Samples: [`sdk/examples/`](../sdk/examples).

Both clients:

- Sync **and** async APIs
- Hash with MD5, 100 virtual nodes, identical ring — or route by Redis Cluster slots, see below
- Static node list
- Miss vs unavailable
- `InvalidatePrefix` / `InvalidateRegex` **broadcast to every node**
- Close idle sockets

## Go (`github.com/mohamedzrouga/mocache/sdk/go`)

| Style | Methods |
|---|---|
| Sync | `Get` `Set` `Delete` `InvalidatePrefix` `InvalidateRegex` |
| Context | `GetContext` `SetContext` … |
| Async (channel) | `GetAsync` `SetAsync` `DeleteAsync` |

```go
c := mocache.New(nodes, mocache.WithTimeout(time.Second))
defer c.Close()
val, ok, err := c.Get("user:123")
res := <-c.GetAsync(ctx, "user:123")
n, err := c.InvalidatePrefix("user:")
n, err = c.InvalidateRegex(`^session:`)
```

```bash
go run ./sdk/examples/go
```

## Python

`MoCacheClient` is blocking. `AsyncMoCacheClient` is for asyncio/FastAPI (`asyncio.to_thread` so there is no aiohttp dependency).

```python
from mocache import MoCacheClient, AsyncMoCacheClient

with MoCacheClient(nodes) as c:
    c.set("user:1", "x")
    c.invalidate_prefix("user:")
    c.invalidate_regex(r"^sess:")

async with AsyncMoCacheClient(nodes) as c:
    await c.get("user:1")
```

Examples:

```bash
PYTHONPATH=sdk/python python3 sdk/examples/python/sync.py
PYTHONPATH=sdk/python python3 sdk/examples/python/asyncio_example.py
pip install fastapi uvicorn
PYTHONPATH=sdk/python uvicorn fastapi_app:app --app-dir sdk/examples/python
```

The FastAPI sample shows a **module singleton** (`sync_singleton()`) and **`app.state.cache`** created in lifespan.

## Routing: ring or slots

By default both SDKs place a key with the MD5 virtual-node ring. A Redis client on the RESP port places it with CRC16 slots instead. Both reach the same LRU on the same process and neither redirects the other's traffic, so **a key written through the SDK and read through `redis-py` can land on different nodes and read as a miss**.

If one keyspace has to be reachable from both, tell the SDK to route by slots. The ranges are the ones each node's `-cluster-peer` entry declares:

```go
c := mocache.New(nodes, mocache.WithSlots(map[string]string{
    "http://cache-0:8090": "0-5460",
    "http://cache-1:8090": "5461-10922",
    "http://cache-2:8090": "10923-16383",
}))
```

```python
client = MoCacheClient(nodes, slots={
    "http://cache-0:8090": "0-5460",
    "http://cache-1:8090": "5461-10922",
    "http://cache-2:8090": "10923-16383",
})
```

The map must cover all 16384 slots exactly once; a gap or an overlap is refused rather than silently misrouted. Python raises `ValueError` at construction, Go reports it from every `Get`/`Set`/`Delete`, since `New` has no error to return.

The ring stays the default so upgrading does not move an existing deployment's keys. Choosing slots moves all of them — plan it like any other rehash.

**The slot map is static.** It does not follow a failover or a reshard: afterwards the affected slots are served by a node the map does not name, and the SDK will miss on them until it is updated. The ring has the same limitation; neither knows anything about cluster membership. A client that must track topology changes should use the RESP port with a real cluster client, which follows `MOVED`.

## Production guidance

Treat MoCache as best-effort. On `OpError` / `MoCacheError`, fall back to source data. Invalidate is best-effort across nodes: a down shard is skipped from the count and the first error is returned.
