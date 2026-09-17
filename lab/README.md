# MoCache lab

A throwaway stack with two jobs: **what does putting MoCache in front of object storage actually buy?**, and **do real Redis clients work against it?**

```
bench.py  ──HTTP──▶  FastAPI (cache-aside)  ──miss──▶  MinIO (S3)
                            │
                            └──hit/set──▶  cache1 · cache2 · cache3   (MD5 ring, 1 node per key)
```

Everything here is disposable. The lab may use pip packages (FastAPI, boto3); the cache node and both SDKs may not — see [AGENTS.md](../AGENTS.md).

## Run it

```bash
cd lab
docker compose up -d --build          # minio + 3 cache nodes + api
docker compose run --rm seed          # upload 2000 random objects (~67 MiB)
docker compose run --rm bench --compare
```

Redis-client compatibility, against the same three nodes:

```bash
docker compose run --rm compat-py             # redis-py, standalone
docker compose run --rm compat-py --cluster   # redis-py RedisCluster
docker compose run --rm compat-go             # go-redis, RESP3
docker compose run --rm compat-go -cluster    # go-redis ClusterClient
```

Teardown: `docker compose down -v` (`-v` also drops the MinIO volume).

From the repo root the same flow is `make lab-up`, `make lab-seed`, `make lab-bench`, `make lab-down`.

| Service | Host | Notes |
|---|---|---|
| API | http://127.0.0.1:8000 | `/objects/{key}`, `/keys`, `/stats`, `/invalidate` |
| MinIO console | http://127.0.0.1:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 | http://127.0.0.1:9000 | bucket `lab` |
| cache1/2/3 metrics | :18090 / :18092 / :18094 | `curl -s 127.0.0.1:18090/metrics` |
| cache1/2/3 RESP | :16379 / :16380 / :16381 | `redis-cli -p 16379` (see the note below) |
| Prometheus (optional) | http://127.0.0.1:9090 | `docker compose --profile obs up -d prometheus` |

## The scenario

`seed` writes `obj/000000.bin …` with sizes drawn uniformly from 4–64 KiB, plus a `_manifest.json` the API serves from `/keys`. Payloads are derived from `SEED` and the key, so two runs with the same seed upload byte-identical objects and benchmark numbers stay comparable.

`GET /objects/{key}` is plain cache-aside:

1. `AsyncMoCacheClient.get("obj:lab:<key>")` — hit ⇒ return, `X-Cache: HIT`.
2. Miss ⇒ `GetObject` from MinIO, `Set` with `CACHE_TTL_SECONDS`, `X-Cache: MISS`.
3. Cache unreachable ⇒ serve from MinIO anyway, `X-Cache: ERROR`. A dead cache node must never turn into a 5xx.
4. `?nocache=1` ⇒ skip the cache entirely, `X-Cache: BYPASS`. This is the benchmark's baseline.

Every response also carries `X-Cache-Node` (which of the three nodes owns the key) and `X-Origin-Ms`.

MoCache values are UTF-8 strings over HTTP, so binary objects are stored as `{"ct":…, "b64":…}`. Base64 inflates by 4/3, which is why `MAX_CACHEABLE_BYTES` (384 KiB) sits well under the node's `-max-value` (1 MiB); larger objects are served from origin and marked `X-Cache: TOO_LARGE`.

Cache nodes run deliberately small — `-capacity=20000 -max-bytes=33554432` — so a 67 MiB corpus does not fit in 3 × 32 MiB and you get real evictions instead of a permanently warm cache.

## Redis compatibility

The three cache nodes serve four things at once: HTTP `:8090`, unary RPC `:8091`, and RESP `:6379` in cluster mode, with slots `0-5460` / `5461-10922` / `10923-16383`. The FastAPI app keeps using the SDK's MD5 ring over HTTP; Redis clients use CRC16 slots over RESP. One LRU underneath, two routing schemes that place keys differently — see [../docs/cluster.md](../docs/cluster.md).

`lab/compat/` holds the checks. They are the stock clients, nothing MoCache-specific:

- `compat/py/check.py` — `redis-py`, as `Redis` and as `RedisCluster`
- `compat/go/main.go` — `go-redis`, as `Client` and as `ClusterClient`, in its own Go module so the library never touches MoCache's `go.mod`

```bash
docker compose run --rm compat-py --cluster
docker compose run --rm compat-go -cluster
# interactive, following redirects:
docker run --rm -it --network mocache-lab redis:7-alpine redis-cli -c -h cache1 -p 6379
```

From the host, the published ports (16379-16381) work for **single-node** access, but the nodes announce themselves as `cache1:6379` and so on, so a cluster client on the host cannot follow a `MOVED`. Run cluster clients inside the network (as the compat services do), or use the root `docker-compose.yml`, which announces `127.0.0.1` for exactly this reason.

Things worth trying:

```bash
# Watch a redirect happen. "abc" is slot 7638, which cache1 does not own.
printf 'SET hello v\r\nGET abc\r\nQUIT\r\n' | nc 127.0.0.1 16379

# Redirect rate and slot coverage, per node.
curl -s 127.0.0.1:18090/metrics | grep -E 'mocache_(resp|cluster)'

# A node down takes its slots with it — no failover yet.
docker compose stop cache2
docker compose run --rm compat-py --cluster   # expect errors for cache2's third
docker compose start cache2
```

## Benchmark

```bash
docker compose run --rm bench --compare                 # origin-only, then cache-aside
docker compose run --rm bench --concurrency 1,8,64,256  # sweep: find the latency knee
docker compose run --rm bench --distribution uniform    # no hot keys — worst case for an LRU
docker compose run --rm bench --flush --json /results/run.json
```

`bench.py` is a closed-loop asyncio HTTP/1.1 load generator on the standard library — no aiohttp, no k6 — with keep-alive connections, one per worker. Each phase runs an unmeasured warmup, then a measured window. Results land in `lab/results/` when `--json` is given.

| Flag | Default | Meaning |
|---|---|---|
| `--concurrency` | `64` | Workers; a comma list runs one phase per level |
| `--duration` / `--warmup` | `20` / `5` | Measured / unmeasured seconds per phase |
| `--keys` | `2000` | Distinct objects in the working set |
| `--distribution` / `--alpha` | `zipf` / `1.1` | Key skew; `uniform` spreads reads evenly |
| `--compare` | off | Run origin-only then cache-aside and print the ratio |
| `--nocache` | off | Origin-only, single phase |
| `--flush` | off | Invalidate the whole bucket prefix before each cached phase |
| `--seed` | `1` | Key-sampling seed |
| `--json` | — | Write the full report (config + every phase) |

Reported per phase: RPS, hit ratio from `X-Cache`, avg/p50/p90/p99/max latency, MiB/s, errors, HTTP status counts, key distribution across nodes, and each node's `/metrics` delta (hits, misses, evictions, final items and bytes).

The `X-Cache` hit ratio and the node counters are collected independently on purpose: a gap between them means requests are not landing where the ring says they should.

### What a run looks like

One laptop, 2000 objects, zipf α=1.1, 8 s phases, `--concurrency 32`:

```
metric         origin-only   cache-aside
requests             4,128         8,342
rps                  514.4       1,039.3
hit ratio             0.0%         92.5%
p50 ms               61.50         29.00
p99 ms               83.33         70.72
MiB/s                16.36         33.01

cache-aside vs origin-only: 2.02x throughput, 2.12x faster p50, 92.5% hit ratio
```

Read it as a floor, not a ceiling. At this object size the ceiling is the API process, not the cache: one Python worker spends most of its time base64-decoding and re-serializing ~34 KiB bodies under the GIL. Things worth trying:

- `API_WORKERS=4 docker compose up -d --force-recreate api` — measured ~2.5x at `--concurrency 64`. Note `/stats` is per-process, so with several workers it only reflects whichever one answered; `X-Cache` counts from the bench stay correct.
- `OBJECT_MAX_BYTES=4096 FORCE=1 docker compose run --rm seed` — small objects move the bottleneck off serialization and onto the cache round trip, which is what you want if you are benchmarking MoCache itself rather than FastAPI.
- `MOCACHE_PROTOCOL=grpc docker compose up -d --force-recreate api` — the unary RPC transport on `:8091` instead of HTTP.
- `THREAD_POOL_SIZE` — both boto3 and the "async" MoCache client are blocking calls dispatched with `asyncio.to_thread`, so this executor, not the event loop, bounds in-flight requests.

## Scenarios worth running

**Node failure is a miss, not an outage.**

```bash
docker compose stop cache2
for i in 0 1 2 3 4 5 6 7; do curl -sS -D- -o /dev/null \
  "http://127.0.0.1:8000/objects/obj/00000$i.bin" | grep -iE '^HTTP/|^x-cache:'; done
docker compose start cache2
```

Keys owned by `cache2` come back `200` with `X-Cache: ERROR` (served from MinIO); the other two thirds still `HIT`. Run `bench --compare` with a node down to see the hit ratio drop to roughly 2/3 while errors stay at 0.

**Rolling restart.** `docker compose restart cache1` — that node's shard is gone, ~1/3 of keys miss once and refill. Nothing to recover; this is the design ([docs/operations.md](../docs/operations.md)).

**Eviction pressure.** `docker compose run --rm bench --distribution uniform --duration 60` with the 67 MiB corpus against 3 × 32 MiB: `mocache_evictions_total` climbs and the hit ratio settles below the zipf number. Shrink `-max-bytes` in `docker-compose.yml` to make it worse, raise it to make it vanish.

**Invalidation.** `curl -X POST 'http://127.0.0.1:8000/invalidate?prefix='` broadcasts to all three nodes and returns the total deleted — matching keys can live on any shard, so invalidate is never a single-node call.

## Configuration

Compose reads these from the environment or an `.env` file next to `docker-compose.yml`:

| Variable | Default | Applies to |
|---|---|---|
| `OBJECT_COUNT` / `OBJECT_MIN_BYTES` / `OBJECT_MAX_BYTES` | `2000` / `4096` / `65536` | seed |
| `SEED` / `FORCE` | `1` / `0` | seed (`FORCE=1` rewrites an already-seeded bucket) |
| `MOCACHE_PROTOCOL` / `MOCACHE_TIMEOUT` | `http` / `1.0` | api |
| `CACHE_TTL_SECONDS` / `MAX_CACHEABLE_BYTES` | `300` / `393216` | api |
| `THREAD_POOL_SIZE` / `API_WORKERS` | `64` / `1` | api |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` / `S3_BUCKET` | `minioadmin` / `minioadmin` / `lab` | minio, api, seed |

Cache-node flags (`-capacity`, `-max-bytes`, `-drain`, …) are in the `x-cache-node` block of `docker-compose.yml`; they are the same flags [docs/operations.md](../docs/operations.md) documents.
