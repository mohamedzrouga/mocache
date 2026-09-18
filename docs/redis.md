# Redis compatibility (RESP)

MoCache speaks the Redis protocol on a third port, so stock Redis clients — `redis-py`, `go-redis`, `redis-cli`, anything that speaks RESP — can use it with no MoCache-specific code. The protocol is implemented in `internal/resp` and `internal/server/resp.go`; no Redis library is linked, the same way `internal/obs` writes Prometheus text by hand.

```bash
go run ./cmd/mocache -resp :6379
```

`-resp` is empty by default: the port only opens when you ask for it.

```python
import redis
r = redis.Redis(host="cache-0", port=6379)
r.set("user:1", "alice", ex=300)
r.get("user:1")
```

```go
c := redis.NewClient(&redis.Options{Addr: "cache-0:6379"})
c.Set(ctx, "user:1", "alice", 5*time.Minute)
```

Sharded across three nodes, use the cluster client instead — see [cluster.md](cluster.md).

## What it is, and what it is not

MoCache is a **capped LRU with TTLs**. Everything a client can do on the RESP port is subject to that: keys are evicted when the node hits `-capacity` or `-max-bytes`, nothing is persisted, and a restart comes back empty. Use it where you would use Redis as a cache, not where you would use Redis as a database.

The only value type is the **string**. There are no lists, hashes, sets, sorted sets, streams, pub/sub, transactions, or scripting. Those commands return `ERR unknown command`, which every client surfaces as an error rather than silently misbehaving.

## Protocol

| | |
|---|---|
| RESP2 | Default |
| RESP3 | After `HELLO 3` — maps, nulls (`_`), booleans, verbatim strings |
| Inline commands | Accepted (`printf 'PING\r\n' \| nc host 6379`) |
| Pipelining | Works: replies are written in request order, one connection at a time |
| Auth | `-resp-password` (or `MOCACHE_PASSWORD`) enables `AUTH` and `HELLO … AUTH` |
| Limits | 16 MiB per bulk argument, 1M arguments per command, `maxclients` 8192 |

`HELLO` reports `server: mocache`, so a client can tell what it is connected to.

## Commands

**Strings** — `GET` `SET` (`EX` `PX` `EXAT` `PXAT` `NX` `XX` `KEEPTTL` `GET`) `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `GETEX` `MGET` `MSET` `MSETNX` `APPEND` `STRLEN` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` `GETRANGE` `SUBSTR`

**Keyspace** — `DEL` `UNLINK` `EXISTS` `TOUCH` `TYPE` `TTL` `PTTL` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` (with `NX` `XX` `GT` `LT`) `PERSIST` `KEYS` `SCAN` (`MATCH` `COUNT` `TYPE`) `RANDOMKEY` `RENAME` `RENAMENX` `DBSIZE` `FLUSHDB` `FLUSHALL`

**Connection** — `PING` `ECHO` `HELLO` `AUTH` `SELECT` `RESET` `QUIT` `CLIENT` (`ID` `SETNAME` `GETNAME` `SETINFO` `INFO` `LIST`) `COMMAND` (`COUNT` `INFO` `DOCS` `GETKEYS`)

**Server** — `INFO` `CONFIG GET/SET` `TIME` `MEMORY USAGE` `READONLY` `READWRITE` `WAIT`

**Cluster** — `CLUSTER INFO/MYID/SLOTS/SHARDS/NODES/KEYSLOT/COUNTKEYSINSLOT/GETKEYSINSLOT/REPLICAS`, `CLUSTER FAILOVER [FORCE|TAKEOVER]`, `CLUSTER REPLICATE`

`COMMAND` output is generated from the same table that drives cluster routing, so what a client introspects cannot drift from what the server accepts.

## Behaviour worth knowing

- **`SELECT`** accepts only database 0. There is one keyspace.
- **`INCR` keeps the TTL.** A rate-limit counter set with `SET k 0 EX 60` still expires after `INCR`.
- **`TTL` rounds up**, as Redis does: 1500 ms of remaining life reports as `2`.
- **`CONFIG SET` is accepted and ignored.** MoCache's limits are process flags; `CONFIG GET` keeps reporting the real values so the no-op is visible.
- **Oversized values return `OOM`.** A value beyond `-max-value`, or an entry beyond `-max-bytes`, is refused rather than admitted and immediately evicted. `INFO` reports `maxmemory_policy:allkeys-lru`.
- **`KEYS` is capped** at 100,000 keys per reply. Prefer `SCAN`.
- **`SCAN` cursors are insertion sequence numbers**, not bucket indexes. The guarantees are Redis's: a key present for the whole scan is returned exactly once; keys added mid-scan may or may not appear; a key deleted and re-added can appear twice.
- **`FLUSHALL` and `FLUSHDB` are the same command** — one keyspace — and both are synchronous.
- **`WAIT numreplicas timeout` works**, and matters: replication is asynchronous, so it is the only way to know a write reached a replica before the primary died. A timeout of 0 means "wait forever" in Redis; here it is capped at 30 seconds rather than holding a connection open indefinitely.
- **`REPLICAOF` is refused in cluster mode**, as in Redis. Use `CLUSTER REPLICATE`.
- **`INFO` reports `redis_version:7.4.0`** because clients gate features on it, alongside `server_name:mocache` and `mocache_version` so it is clear what is actually answering.

## Metrics

The RESP front end is scraped from the same `/metrics` endpoint as everything else (`docs/observability.md`):

| Metric | Meaning |
|---|---|
| `mocache_resp_commands_total` | Commands handled |
| `mocache_resp_errors_total` | Error replies, redirects included |
| `mocache_resp_moved_total` | `MOVED` redirects — a rising rate means stale client slot maps |
| `mocache_resp_connections` | Open connections |
| `mocache_resp_duration_seconds_sum` | Total execution time |

`keyspace_hits` / `keyspace_misses` in `INFO` are the same counters as `mocache_hits_total` / `mocache_misses_total`: the RESP, HTTP, and RPC front ends share one LRU.

## Verifying it yourself

`lab/compat/` runs the real clients against a running node — nothing MoCache-specific in the path:

```bash
cd lab
docker compose run --rm compat-py             # redis-py, standalone
docker compose run --rm compat-py --cluster   # redis-py RedisCluster
docker compose run --rm compat-go             # go-redis, RESP3
docker compose run --rm compat-go -cluster    # go-redis ClusterClient
```

`go-redis` is a third-party library, so it lives in its own Go module under `lab/compat/go` and never enters MoCache's `go.mod`.
