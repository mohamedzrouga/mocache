# Architecture

MoCache is a memcached-style cache: **nodes are independent processes**; **clients decide which node owns a key**. There is no gossip, consensus, replication, or membership protocol on the server.

```
Python / Go replicas                    MoCache nodes (StatefulSet)
┌────────────┐                          ┌──────────────┐
│ SDK        │  consistent hash         │ cache-0 LRU  │
│ (HTTP or   │─────────────────────────▶│ :8090 / :8091│
│  unary RPC)│                          └──────────────┘
└────────────┘                          ┌──────────────┐
                                        │ cache-1 LRU  │
                                        └──────────────┘
                                        ┌──────────────┐
                                        │ cache-2 LRU  │
                                        └──────────────┘
```

## Why this split

Organizational constraint: no Redis/Valkey/Olric/Couchbase (no third-party cache product). Implementing clustering in-house is higher risk than a small LRU plus a hash ring in the SDK. Data is recomputable; losing a node's memory on restart is expected.

## Cache node

A single Go binary:

- In-memory LRU (`internal/cache`), hard-capped by **item count**.
- Copies values on Set/Get so callers cannot mutate entries under the mutex.
- Lazy TTL on access plus an optional janitor (`-janitor`) so unused expired keys do not occupy slots until LRU eviction.
- HTTP on `-http` (default `:8090`) and unary RPC on `-rpc` (default `:8091`).
- Process-local only. No disk. A crash or rolling restart starts empty.

## Client routing

Both SDKs build the same MD5 virtual-node ring at construction (`vnodes=100` by default). Node lists are static. Adding or removing a node requires updating every client's config; ~1/N of keys move.

Go and Python must keep the same:

1. Virtual-node label `{nodeURL}:{i}`
2. MD5 digest interpreted as a big-endian unsigned integer
3. Owner = first ring position **at or after** the key hash (bisect_left), wrapping to 0

## Failure model

| Event | What clients see | What to do |
|---|---|---|
| Key absent or TTL elapsed | miss (`false` / `None`) | Recompute from source, Set |
| Node rolling / crash | brief `OpError` / `MoCacheError`, then misses on that shard until refill | Treat as miss; do not fail the request path |
| Whole process SIGTERM | `/readyz` goes 503, in-flight RPCs finish or get reset, then exit 0 | Endpoints drop the pod before sockets close |

There is **no** automatic failover to another node. A key always maps to the same pod name. That is what keeps Go and Python clients consistent.
