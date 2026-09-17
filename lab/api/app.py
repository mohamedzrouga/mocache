"""Lab API: FastAPI cache-aside in front of MinIO (S3), backed by MoCache.

    GET /objects/{key}  ->  MoCache hit          (X-Cache: HIT)
                        ->  miss: S3 GetObject, then MoCache Set (X-Cache: MISS)

This is the shape a real service uses: one AsyncMoCacheClient per process
created in the lifespan, blocking S3 calls pushed to a thread, and every cache
failure degrading to an origin read instead of an error (X-Cache: ERROR).

Env:
    MOCACHE_NODES         comma-separated node URLs (ring order must match every client)
    MOCACHE_PROTOCOL      http (default) | grpc  — unary RPC on :8091
    MOCACHE_TIMEOUT       per-call timeout, seconds
    THREAD_POOL_SIZE      worker threads shared by S3 and cache I/O (see lab/README.md)
    CACHE_TTL_SECONDS     TTL written on Set (<=0 = no expiry)
    MAX_CACHEABLE_BYTES   objects larger than this are never cached
    S3_ENDPOINT / S3_BUCKET / AWS_* credentials
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query, Response

from mocache import AsyncMoCacheClient, HashRing, MoCacheError

NODES = [n for n in os.environ.get("MOCACHE_NODES", "http://127.0.0.1:8090").split(",") if n]
PROTOCOL = os.environ.get("MOCACHE_PROTOCOL", "http")
TIMEOUT = float(os.environ.get("MOCACHE_TIMEOUT", "1.0"))
TTL = int(os.environ.get("CACHE_TTL_SECONDS", "300"))
# base64 inflates by 4/3, so keep this well under the node's -max-value (1Mi).
MAX_CACHEABLE = int(os.environ.get("MAX_CACHEABLE_BYTES", str(384 * 1024)))
BUCKET = os.environ.get("S3_BUCKET", "lab")
# Both boto3 and the "async" MoCache client are blocking calls on a thread, so
# the executor — not the event loop — is the ceiling on in-flight requests.
THREADS = int(os.environ.get("THREAD_POOL_SIZE", "64"))
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000")
KEY_PREFIX = f"obj:{BUCKET}:"
# Same MD5 virtual-node ring the SDK routes with; only used to label responses.
RING = HashRing(NODES)

_stats = {
    "hits": 0,
    "misses": 0,
    "bypass": 0,
    "cache_errors": 0,
    "origin_errors": 0,
    "origin_bytes": 0,
    "served_bytes": 0,
    "origin_seconds": 0.0,
    "cache_seconds": 0.0,
    "origin_reads": 0,
    "cache_reads": 0,
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},  # MinIO: no virtual-host buckets
            retries={"max_attempts": 2, "mode": "standard"},
            max_pool_connections=128,  # must exceed the load generator's concurrency
            connect_timeout=3,
            read_timeout=10,
        ),
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    pool = ThreadPoolExecutor(max_workers=THREADS, thread_name_prefix="io")
    asyncio.get_running_loop().set_default_executor(pool)  # asyncio.to_thread uses this
    app.state.s3 = _s3_client()
    app.state.cache = AsyncMoCacheClient(NODES, timeout=TIMEOUT, protocol=PROTOCOL)
    app.state.keys = None
    await asyncio.to_thread(_wait_for_s3, app.state.s3)
    try:
        yield
    finally:
        await app.state.cache.close()
        pool.shutdown(wait=False, cancel_futures=True)


def _wait_for_s3(s3, attempts: int = 60) -> None:
    """MinIO and the API start together; poll instead of ordering the compose graph."""
    for i in range(attempts):
        try:
            s3.list_buckets()
            return
        except (ClientError, BotoCoreError):
            time.sleep(min(1.0 + i * 0.1, 3.0))
    raise RuntimeError(f"S3 endpoint {ENDPOINT} never became reachable")


app = FastAPI(title="MoCache lab API", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    return {"status": "ok", "nodes": NODES, "protocol": PROTOCOL, "bucket": BUCKET, "threads": THREADS}


@app.get("/keys")
async def keys(limit: int = Query(100_000, ge=1, le=1_000_000)) -> dict[str, object]:
    """Object keys the load generator should hit. Served from the seeder's manifest."""
    if app.state.keys is None:
        app.state.keys = await asyncio.to_thread(_load_keys, app.state.s3)
    ks = app.state.keys[:limit]
    return {"bucket": BUCKET, "count": len(ks), "keys": ks}


def _load_keys(s3) -> list[str]:
    try:
        body = s3.get_object(Bucket=BUCKET, Key="_manifest.json")["Body"].read()
        return [o["key"] for o in json.loads(body)["objects"]]
    except ClientError:
        pass  # no manifest (bucket seeded by hand) — fall back to a listing
    out: list[str] = []
    token = None
    while True:
        kw = {"Bucket": BUCKET, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        out.extend(o["Key"] for o in resp.get("Contents", []) if not o["Key"].startswith("_"))
        if not resp.get("IsTruncated"):
            return out
        token = resp["NextContinuationToken"]


@app.get("/objects/{key:path}")
async def get_object(key: str, nocache: int = Query(0, ge=0, le=1)) -> Response:
    """Cache-aside read. nocache=1 measures the same path with the cache bypassed."""
    cache_key = KEY_PREFIX + key
    cache_down = False

    if not nocache:
        t0 = time.perf_counter()
        try:
            cached = await app.state.cache.get(cache_key)
            _stats["cache_seconds"] += time.perf_counter() - t0
            _stats["cache_reads"] += 1
        except MoCacheError:
            # Cache down must never fail the request: fall through to origin.
            _stats["cache_errors"] += 1
            cached = None
            cache_down = True
        if cached is not None:
            _stats["hits"] += 1
            body, ctype = _decode(cached)
            _stats["served_bytes"] += len(body)
            return _respond(body, ctype, "HIT", cache_key, 0.0)

    t0 = time.perf_counter()
    try:
        body, ctype = await asyncio.to_thread(_fetch, app.state.s3, key)
    except ClientError as e:
        _stats["origin_errors"] += 1
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NoSuchBucket"):
            raise HTTPException(status_code=404, detail=f"no such object: {key}") from e
        raise HTTPException(status_code=502, detail=f"origin error: {e}") from e
    except BotoCoreError as e:
        _stats["origin_errors"] += 1
        raise HTTPException(status_code=502, detail=f"origin error: {e}") from e
    origin_s = time.perf_counter() - t0
    _stats["origin_seconds"] += origin_s
    _stats["origin_reads"] += 1
    _stats["origin_bytes"] += len(body)
    _stats["served_bytes"] += len(body)

    if nocache:
        _stats["bypass"] += 1
        return _respond(body, ctype, "BYPASS", cache_key, origin_s)

    _stats["misses"] += 1
    status = "MISS"
    if cache_down:
        status = "ERROR"
    elif len(body) <= MAX_CACHEABLE:
        try:
            await app.state.cache.set(cache_key, _encode(body, ctype), ttl_seconds=TTL)
        except MoCacheError:
            _stats["cache_errors"] += 1
            status = "ERROR"
    else:
        status = "TOO_LARGE"
    return _respond(body, ctype, status, cache_key, origin_s)


@app.delete("/objects/{key:path}")
async def drop_object(key: str) -> dict[str, str]:
    """Evict one key from its owning node. The S3 object is untouched."""
    try:
        await app.state.cache.delete(KEY_PREFIX + key)
    except MoCacheError as e:
        raise HTTPException(status_code=503, detail=f"cache unavailable: {e}") from e
    return {"deleted": key}


@app.post("/invalidate")
async def invalidate(prefix: str = Query("", description="object key prefix; empty = whole bucket")) -> dict[str, int]:
    """Broadcast invalidate to every node (matching keys can live on any shard)."""
    try:
        n = await app.state.cache.invalidate_prefix(KEY_PREFIX + prefix)
    except MoCacheError as e:
        raise HTTPException(status_code=503, detail=f"cache unavailable: {e}") from e
    return {"deleted": n}


@app.get("/stats")
async def stats() -> dict[str, object]:
    reads = _stats["hits"] + _stats["misses"]
    return {
        **_stats,
        "hit_ratio": (_stats["hits"] / reads) if reads else 0.0,
        "avg_origin_ms": 1000 * _stats["origin_seconds"] / max(_stats["origin_reads"], 1),
        "avg_cache_ms": 1000 * _stats["cache_seconds"] / max(_stats["cache_reads"], 1),
    }


@app.post("/stats/reset")
async def reset_stats() -> dict[str, str]:
    for k in _stats:
        _stats[k] = 0 if isinstance(_stats[k], int) else 0.0
    return {"reset": "ok"}


def _fetch(s3, key: str) -> tuple[bytes, str]:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read(), obj.get("ContentType") or "application/octet-stream"


def _encode(body: bytes, ctype: str) -> str:
    """MoCache values are UTF-8 strings over HTTP, so binary objects are base64."""
    return json.dumps({"ct": ctype, "b64": base64.b64encode(body).decode("ascii")})


def _decode(value: str) -> tuple[bytes, str]:
    env = json.loads(value)
    return base64.b64decode(env["b64"]), env.get("ct", "application/octet-stream")


def _respond(body: bytes, ctype: str, status: str, cache_key: str, origin_s: float) -> Response:
    return Response(
        content=body,
        media_type=ctype,
        headers={
            "X-Cache": status,
            "X-Cache-Node": _node_for(cache_key),
            "X-Origin-Ms": f"{origin_s * 1000:.2f}",
        },
    )


def _node_for(cache_key: str) -> str:
    # Handy for showing key distribution across the three nodes in the lab.
    return RING.node(cache_key)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        workers=int(os.environ.get("API_WORKERS", "1")),
        access_log=False,
        log_level="info",
    )
