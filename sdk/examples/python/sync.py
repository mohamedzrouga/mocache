"""Synchronous MoCache client example.

    PYTHONPATH=sdk/python python3 sdk/examples/python/sync.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from mocache import MoCacheClient, MoCacheError

nodes = os.environ.get(
    "MOCACHE_NODES",
    "http://127.0.0.1:8090",
).split(",")

with MoCacheClient(nodes, timeout=1.0, protocol="http") as cache:
    try:
        cache.set("user:123", "some_value", ttl_seconds=300)
        print("get", cache.get("user:123"))
        # Drop every key that starts with "user:" on all nodes.
        deleted = cache.invalidate_prefix("user:")
        print("invalidated", deleted)
        cache.set("sess:abc", "x", ttl_seconds=60)
        print("regex", cache.invalidate_regex(r"^sess:"))
    except MoCacheError as e:
        print("unavailable:", e, file=sys.stderr)
        sys.exit(1)
