# SDKs

Code lives under [`sdk/`](../sdk). Samples: [`sdk/examples/`](../sdk/examples).

Both clients:

- Sync **and** async APIs
- Hash with MD5, 100 virtual nodes, identical ring
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

## Production guidance

Treat MoCache as best-effort. On `OpError` / `MoCacheError`, fall back to source data. Invalidate is best-effort across nodes: a down shard is skipped from the count and the first error is returned.
