"""FastAPI + MoCache: module singleton AND app.state.

Install (example process only, not the SDK):

    pip install fastapi uvicorn
    PYTHONPATH=sdk/python uvicorn fastapi_app:app --app-dir sdk/examples/python

Two wiring styles are shown:

1. Module-level singleton ``_SYNC`` — simple, process-wide, sync client.
2. ``app.state.cache`` — preferred: created in lifespan, closed on shutdown,
   async client, one instance per app (not per request).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from fastapi import FastAPI, Request

from mocache import AsyncMoCacheClient, MoCacheClient

NODES = os.environ.get("MOCACHE_NODES", "http://127.0.0.1:8090").split(",")

# --- 1) Process singleton (sync). Import and reuse; close at process exit. ---
_SYNC: MoCacheClient | None = None


def sync_singleton() -> MoCacheClient:
    global _SYNC
    if _SYNC is None:
        _SYNC = MoCacheClient(NODES, timeout=1.0)
    return _SYNC


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # --- 2) app.state: one async client for the app lifetime. ---
    app.state.cache = AsyncMoCacheClient(NODES, timeout=1.0)
    try:
        yield
    finally:
        await app.state.cache.close()
        sync = _SYNC
        if sync is not None:
            sync.close()


app = FastAPI(lifespan=lifespan)


@app.get("/cache/{key}")
async def get_from_state(key: str, request: Request) -> dict[str, str | None]:
    """Reads via the async client stored on app.state."""
    val = await request.app.state.cache.get(key)
    return {"key": key, "value": val}


@app.put("/cache/{key}")
async def put_from_state(key: str, request: Request) -> dict[str, str]:
    body = await request.body()
    await request.app.state.cache.set(key, body.decode(), ttl_seconds=300)
    return {"ok": "true"}


@app.delete("/cache/prefix/{prefix}")
async def invalidate_prefix(prefix: str, request: Request) -> dict[str, int]:
    n = await request.app.state.cache.invalidate_prefix(prefix)
    return {"deleted": n}


@app.get("/sync-singleton/{key}")
def get_from_singleton(key: str) -> dict[str, str | None]:
    """Same process, blocking client — use only on non-async routes."""
    return {"key": key, "value": sync_singleton().get(key)}
