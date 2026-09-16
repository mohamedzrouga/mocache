"""Minimal MoCache Python client example.

    PYTHONPATH=sdk/python python3 sdk/examples/python/example.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from mocache import MoCacheClient, MoCacheError

nodes = [
    "http://cache-0.cache-headless.svc.cluster.local:8090",
    "http://cache-1.cache-headless.svc.cluster.local:8090",
    "http://cache-2.cache-headless.svc.cluster.local:8090",
]
if os.environ.get("MOCACHE_NODES"):
    nodes = os.environ["MOCACHE_NODES"].split(",")

# protocol="grpc" selects the fast unary RPC path (port 8091).
with MoCacheClient(nodes, timeout=1.0, protocol="http") as cache:
    try:
        cache.set("user:123", "some_value", ttl_seconds=300)
        val = cache.get("user:123")  # None on miss
        print("hit:" if val is not None else "miss", val)
        cache.delete("user:123")
    except MoCacheError as e:
        # Node down / timeout — treat as a miss and recompute from source.
        print("unavailable:", e, file=sys.stderr)
        sys.exit(1)
