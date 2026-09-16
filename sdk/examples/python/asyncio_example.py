"""asyncio MoCache client example.

    PYTHONPATH=sdk/python python3 sdk/examples/python/asyncio_example.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from mocache import AsyncMoCacheClient, MoCacheError


async def main() -> None:
    nodes = os.environ.get("MOCACHE_NODES", "http://127.0.0.1:8090").split(",")
    async with AsyncMoCacheClient(nodes, timeout=1.0) as cache:
        await cache.set("user:123", "async-value", ttl_seconds=300)
        print("get", await cache.get("user:123"))
        print("prefix", await cache.invalidate_prefix("user:"))
        await cache.set("order:1", "x")
        print("regex", await cache.invalidate_regex(r"^order:"))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except MoCacheError as e:
        print("unavailable:", e, file=sys.stderr)
        sys.exit(1)
