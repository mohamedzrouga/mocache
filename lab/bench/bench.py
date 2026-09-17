#!/usr/bin/env python3
"""Stress test and benchmark for the MoCache lab.

Closed-loop HTTP/1.1 load generator, standard library only (no aiohttp/httpx),
in the same spirit as the cache node and both SDKs.

    bench.py --compare                    # origin-only vs cache-aside, side by side
    bench.py --duration 60 --concurrency 128
    bench.py --concurrency 1,8,32,128,512 # concurrency sweep: find the latency knee
    bench.py --distribution uniform       # every key equally hot (worst case for an LRU)
    bench.py --json /results/run.json

Each phase: optional warmup (not measured), then a measured window. Hit ratio
comes from the API's X-Cache header; cache-node counters come from /metrics
deltas, so the report separates "the client saw a hit" from "the node recorded one".
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
import urllib.request
from bisect import bisect_left
from dataclasses import dataclass, field
from urllib.parse import urlsplit


@dataclass
class Phase:
    """One measured window of load."""

    name: str
    concurrency: int
    latencies: list[float] = field(default_factory=list)
    statuses: dict[int, int] = field(default_factory=dict)
    cache_status: dict[str, int] = field(default_factory=dict)
    nodes: dict[str, int] = field(default_factory=dict)
    bytes_read: int = 0
    errors: int = 0
    wall: float = 0.0
    node_metrics: dict[str, dict[str, float]] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return len(self.latencies)

    @property
    def rps(self) -> float:
        return self.count / self.wall if self.wall else 0.0

    @property
    def hit_ratio(self) -> float:
        hits = self.cache_status.get("HIT", 0)
        total = sum(self.cache_status.values())
        return hits / total if total else 0.0

    def pct(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        xs = sorted(self.latencies)
        idx = min(int(p / 100 * len(xs)), len(xs) - 1)
        return xs[idx]

    def summary(self) -> dict[str, object]:
        return {
            "name": self.name,
            "concurrency": self.concurrency,
            "requests": self.count,
            "wall_seconds": round(self.wall, 3),
            "rps": round(self.rps, 1),
            "errors": self.errors,
            "hit_ratio": round(self.hit_ratio, 4),
            "mib_per_s": round(self.bytes_read / 1048576 / self.wall, 2) if self.wall else 0.0,
            "latency_ms": {
                "avg": round(1000 * statistics.fmean(self.latencies), 3) if self.latencies else 0.0,
                "p50": round(1000 * self.pct(50), 3),
                "p90": round(1000 * self.pct(90), 3),
                "p99": round(1000 * self.pct(99), 3),
                "max": round(1000 * max(self.latencies), 3) if self.latencies else 0.0,
            },
            "http_status": self.statuses,
            "x_cache": self.cache_status,
            "key_distribution": self.nodes,
            "cache_nodes": self.node_metrics,
        }


class KeySampler:
    """Uniform, or Zipf-like (hot minority) key selection.

    Real object traffic is skewed: a few keys carry most reads. Zipf is the
    interesting case for a cache; uniform is the pathological one.
    """

    def __init__(self, keys: list[str], distribution: str, alpha: float, rnd: random.Random) -> None:
        self._keys = keys
        self._rnd = rnd
        self._uniform = distribution == "uniform"
        if self._uniform:
            return
        weights = [1.0 / ((i + 1) ** alpha) for i in range(len(keys))]
        total = sum(weights)
        acc = 0.0
        self._cum: list[float] = []
        for w in weights:
            acc += w / total
            self._cum.append(acc)
        self._cum[-1] = 1.0

    def pick(self) -> str:
        if self._uniform:
            return self._keys[self._rnd.randrange(len(self._keys))]
        return self._keys[bisect_left(self._cum, self._rnd.random())]


class Conn:
    """One keep-alive HTTP/1.1 connection; reconnects after any transport error."""

    def __init__(self, host: str, port: int) -> None:
        self._host, self._port = host, port
        self._r: asyncio.StreamReader | None = None
        self._w: asyncio.StreamWriter | None = None

    async def get(self, path: str, host_header: str) -> tuple[int, dict[str, str], int]:
        if self._r is None:
            self._r, self._w = await asyncio.open_connection(self._host, self._port)
        req = f"GET {path} HTTP/1.1\r\nHost: {host_header}\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n"
        self._w.write(req.encode("ascii"))
        await self._w.drain()
        status, headers, nbytes = await self._read(self._r)
        if headers.get("connection", "").lower() == "close":
            await self.close()
        return status, headers, nbytes

    @staticmethod
    async def _read(r: asyncio.StreamReader) -> tuple[int, dict[str, str], int]:
        head = await r.readuntil(b"\r\n\r\n")
        lines = head.split(b"\r\n")
        status = int(lines[0].split(b" ")[1])
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line:
                continue
            k, _, v = line.partition(b":")
            headers[k.decode("latin-1").lower()] = v.strip().decode("latin-1")
        if "content-length" in headers:
            n = int(headers["content-length"])
            if n:
                await r.readexactly(n)
            return status, headers, n
        if headers.get("transfer-encoding", "").lower() == "chunked":
            total = 0
            while True:
                size = int((await r.readuntil(b"\r\n")).strip() or b"0", 16)
                if size == 0:
                    await r.readuntil(b"\r\n")
                    return status, headers, total
                await r.readexactly(size + 2)  # chunk + CRLF
                total += size
        return status, headers, 0

    async def close(self) -> None:
        if self._w is not None:
            try:
                self._w.close()
                await self._w.wait_closed()
            except (OSError, asyncio.CancelledError):
                pass
        self._r = self._w = None


async def run_phase(
    name: str,
    args: argparse.Namespace,
    sampler: KeySampler,
    *,
    nocache: bool,
    concurrency: int,
    duration: float,
    record: bool,
) -> Phase:
    url = urlsplit(args.api)
    host, port = url.hostname or "127.0.0.1", url.port or 80
    host_header = f"{host}:{port}"
    suffix = "?nocache=1" if nocache else ""
    phase = Phase(name=name, concurrency=concurrency)
    deadline = time.monotonic() + duration

    async def worker() -> None:
        conn = Conn(host, port)
        try:
            while time.monotonic() < deadline:
                key = sampler.pick()
                t0 = time.perf_counter()
                try:
                    status, headers, nbytes = await conn.get(f"/objects/{key}{suffix}", host_header)
                except (OSError, asyncio.IncompleteReadError, ValueError, asyncio.LimitOverrunError):
                    await conn.close()
                    if record:
                        phase.errors += 1
                    continue
                dt = time.perf_counter() - t0
                if not record:
                    continue
                phase.latencies.append(dt)
                phase.statuses[status] = phase.statuses.get(status, 0) + 1
                phase.bytes_read += nbytes
                if status >= 400:
                    phase.errors += 1
                    continue
                cs = headers.get("x-cache", "-")
                phase.cache_status[cs] = phase.cache_status.get(cs, 0) + 1
                node = headers.get("x-cache-node", "-")
                phase.nodes[node] = phase.nodes.get(node, 0) + 1
        finally:
            await conn.close()

    before = scrape_nodes(args.cache_metrics) if record else {}
    start = time.monotonic()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    phase.wall = time.monotonic() - start
    if record:
        phase.node_metrics = diff_nodes(before, scrape_nodes(args.cache_metrics))
    return phase


# --- cache-node /metrics (hand-written Prometheus text, see internal/obs) ---

WANTED = (
    "mocache_hits_total",
    "mocache_misses_total",
    "mocache_evictions_total",
    "mocache_invalidations_total",
    "mocache_items",
    "mocache_bytes",
)


def scrape_nodes(nodes: list[str]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for node in nodes:
        try:
            with urllib.request.urlopen(node.rstrip("/") + "/metrics", timeout=3) as resp:
                body = resp.read().decode("utf-8", "replace")
        except OSError:
            continue
        vals: dict[str, float] = {}
        for line in body.splitlines():
            if line.startswith("#") or " " not in line:
                continue
            name, _, value = line.partition(" ")
            if name in WANTED:
                try:
                    vals[name] = float(value)
                except ValueError:
                    pass
        out[node] = vals
    return out


def diff_nodes(
    before: dict[str, dict[str, float]], after: dict[str, dict[str, float]]
) -> dict[str, dict[str, float]]:
    """Counters are deltas over the phase; gauges (items, bytes) are end values."""
    gauges = {"mocache_items", "mocache_bytes"}
    out: dict[str, dict[str, float]] = {}
    for node, post in after.items():
        pre = before.get(node, {})
        out[node] = {k: (v if k in gauges else v - pre.get(k, 0.0)) for k, v in post.items()}
    return out


def fetch_keys(api: str, limit: int) -> list[str]:
    with urllib.request.urlopen(f"{api.rstrip('/')}/keys?limit={limit}", timeout=30) as resp:
        return json.loads(resp.read())["keys"]


def post(api: str, path: str) -> None:
    req = urllib.request.Request(api.rstrip("/") + path, data=b"", method="POST")
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except OSError as e:
        print(f"warning: POST {path}: {e}", file=sys.stderr)


# --- reporting ---

ROWS = [
    ("requests", lambda s: f"{s['requests']:,}"),
    ("rps", lambda s: f"{s['rps']:,.1f}"),
    ("hit ratio", lambda s: f"{s['hit_ratio'] * 100:.1f}%"),
    ("avg ms", lambda s: f"{s['latency_ms']['avg']:.2f}"),
    ("p50 ms", lambda s: f"{s['latency_ms']['p50']:.2f}"),
    ("p90 ms", lambda s: f"{s['latency_ms']['p90']:.2f}"),
    ("p99 ms", lambda s: f"{s['latency_ms']['p99']:.2f}"),
    ("max ms", lambda s: f"{s['latency_ms']['max']:.2f}"),
    ("MiB/s", lambda s: f"{s['mib_per_s']:.2f}"),
    ("errors", lambda s: f"{s['errors']:,}"),
]


def print_table(phases: list[Phase]) -> None:
    sums = [p.summary() for p in phases]
    w = max(12, *(len(s["name"]) for s in sums)) + 2
    print("\n" + "metric".ljust(12) + "".join(str(s["name"]).rjust(w) for s in sums))
    print("-" * (12 + w * len(sums)))
    for label, fn in ROWS:
        print(label.ljust(12) + "".join(fn(s).rjust(w) for s in sums))


def print_nodes(phase: Phase) -> None:
    if not phase.node_metrics:
        return
    print(f"\ncache nodes during '{phase.name}' (counters = delta, items/bytes = final)")
    print(f"  {'node':<28}{'hits':>10}{'misses':>10}{'evictions':>11}{'items':>10}{'MiB':>9}")
    for node, m in sorted(phase.node_metrics.items()):
        print(
            f"  {node:<28}{m.get('mocache_hits_total', 0):>10,.0f}"
            f"{m.get('mocache_misses_total', 0):>10,.0f}"
            f"{m.get('mocache_evictions_total', 0):>11,.0f}"
            f"{m.get('mocache_items', 0):>10,.0f}"
            f"{m.get('mocache_bytes', 0) / 1048576:>9,.1f}"
        )
    if phase.nodes:
        total = sum(phase.nodes.values())
        share = ", ".join(f"{n.split('//')[-1]} {c / total * 100:.0f}%" for n, c in sorted(phase.nodes.items()))
        print(f"  key distribution: {share}")


def print_verdict(baseline: Phase, cached: Phase) -> None:
    b, c = baseline.summary(), cached.summary()
    rps = c["rps"] / b["rps"] if b["rps"] else 0.0
    p50 = b["latency_ms"]["p50"] / c["latency_ms"]["p50"] if c["latency_ms"]["p50"] else 0.0
    p99 = b["latency_ms"]["p99"] / c["latency_ms"]["p99"] if c["latency_ms"]["p99"] else 0.0
    print(
        f"\ncache-aside vs origin-only: {rps:.2f}x throughput, "
        f"{p50:.2f}x faster p50, {p99:.2f}x faster p99, "
        f"{c['hit_ratio'] * 100:.1f}% hit ratio"
    )


async def main() -> int:
    ap = argparse.ArgumentParser(description="MoCache lab stress test / benchmark")
    ap.add_argument("--api", default=os.environ.get("API_URL", "http://127.0.0.1:8000"))
    ap.add_argument("--concurrency", default="64", help="workers, or a comma list to sweep: 1,8,64,256")
    ap.add_argument("--duration", type=float, default=20.0, help="measured seconds per phase")
    ap.add_argument("--warmup", type=float, default=5.0, help="unmeasured seconds before each phase")
    ap.add_argument("--keys", type=int, default=2000, help="distinct objects in the working set")
    ap.add_argument("--distribution", choices=("zipf", "uniform"), default="zipf")
    ap.add_argument("--alpha", type=float, default=1.1, help="zipf skew; higher = hotter head")
    ap.add_argument("--seed", type=int, default=1, help="key-sampling seed (repeatable runs)")
    ap.add_argument("--nocache", action="store_true", help="bypass the cache (origin-only)")
    ap.add_argument("--compare", action="store_true", help="run origin-only, then cache-aside")
    ap.add_argument("--flush", action="store_true", help="invalidate the bucket before each cached phase")
    ap.add_argument(
        "--cache-metrics",
        default=os.environ.get("CACHE_METRICS", ""),
        help="comma-separated cache node base URLs to scrape /metrics from",
    )
    ap.add_argument("--json", dest="json_out", default="", help="write the full report here")
    args = ap.parse_args()
    args.cache_metrics = [n for n in args.cache_metrics.split(",") if n]
    levels = [int(c) for c in str(args.concurrency).split(",") if c.strip()]

    keys = fetch_keys(args.api, args.keys)
    if not keys:
        print("no objects in the bucket — run: docker compose run --rm seed", file=sys.stderr)
        return 1
    keys = keys[: args.keys]
    print(
        f"target {args.api} | {len(keys)} keys | {args.distribution}"
        + (f" alpha={args.alpha}" if args.distribution == "zipf" else "")
        + f" | {args.duration:g}s per phase (warmup {args.warmup:g}s)"
    )

    phases: list[Phase] = []
    for concurrency in levels:
        plan = [("origin-only", True), ("cache-aside", False)] if args.compare else [
            ("origin-only" if args.nocache else "cache-aside", args.nocache)
        ]
        for label, nocache in plan:
            name = f"{label} c={concurrency}" if len(levels) > 1 else label
            if args.flush and not nocache:
                post(args.api, "/invalidate?prefix=")
            sampler = KeySampler(keys, args.distribution, args.alpha, random.Random(args.seed))
            if args.warmup > 0:
                print(f"warming up: {name} ...", flush=True)
                await run_phase(name, args, sampler, nocache=nocache, concurrency=concurrency,
                                duration=args.warmup, record=False)
            print(f"running:    {name} ...", flush=True)
            post(args.api, "/stats/reset")
            sampler = KeySampler(keys, args.distribution, args.alpha, random.Random(args.seed))
            phase = await run_phase(name, args, sampler, nocache=nocache, concurrency=concurrency,
                                    duration=args.duration, record=True)
            phases.append(phase)

    print_table(phases)
    for phase in phases:
        if not phase.name.startswith("origin-only"):
            print_nodes(phase)
    if args.compare:
        for i in range(0, len(phases) - 1, 2):
            print_verdict(phases[i], phases[i + 1])

    if args.json_out:
        report = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "config": {k: v for k, v in vars(args).items() if k != "json_out"},
            "phases": [p.summary() for p in phases],
        }
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nwrote {args.json_out}")

    return 1 if any(p.errors for p in phases) else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
