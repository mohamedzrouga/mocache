"""Compatibility check: drive MoCache with the real redis-py client.

Nothing here knows about MoCache — it is the stock client, talking to the RESP
port. Run it against one node (standalone) and against the cluster:

    docker compose run --rm compat-py
    docker compose run --rm compat-py --cluster

Exits non-zero on the first failed expectation.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import redis
from redis.cluster import RedisCluster

FAILURES: list[str] = []


def check(label: str, got: object, want: object) -> None:
    ok = got == want
    print(f"  {'ok  ' if ok else 'FAIL'}  {label}: {got!r}" + ("" if ok else f" (want {want!r})"))
    if not ok:
        FAILURES.append(label)


def check_true(label: str, got: object) -> None:
    ok = bool(got)
    print(f"  {'ok  ' if ok else 'FAIL'}  {label}: {got!r}")
    if not ok:
        FAILURES.append(label)


def strings(r: redis.Redis | RedisCluster) -> None:
    # Every key carries the {compat} hash tag so the whole check stays on one
    # slot. Without it, a plain (non-cluster) client talking to a cluster-mode
    # node gets MOVED on single-key commands and CROSSSLOT on multi-key ones —
    # which is correct Redis behaviour, not a MoCache quirk.
    print("strings")
    r.delete("{compat}:str", "{compat}:n", "{compat}:app")
    check("set", r.set("{compat}:str", "hello"), True)
    check("get", r.get("{compat}:str"), b"hello")
    check("exists", r.exists("{compat}:str"), 1)
    check("strlen", r.strlen("{compat}:str"), 5)
    check("append", r.append("{compat}:app", "ab"), 2)
    check("append again", r.append("{compat}:app", "cd"), 4)
    check("get appended", r.get("{compat}:app"), b"abcd")
    check("getrange", r.getrange("{compat}:str", 0, 3), b"hell")
    check("incr", r.incr("{compat}:n"), 1)
    check("incrby", r.incrby("{compat}:n", 41), 42)
    check("decr", r.decr("{compat}:n"), 41)
    check("incrbyfloat", r.incrbyfloat("{compat}:n", 0.5), 41.5)
    check("getset", r.getset("{compat}:str", "world"), b"hello")
    check("getdel", r.getdel("{compat}:str"), b"world")
    check("get after getdel", r.get("{compat}:str"), None)
    check("set nx on free key", r.set("{compat}:nx", "1", nx=True), True)
    check("set nx on taken key", r.set("{compat}:nx", "2", nx=True), None)
    check("set xx on taken key", r.set("{compat}:nx", "3", xx=True), True)
    check("set get option", r.set("{compat}:nx", "4", get=True), b"3")
    r.delete("{compat}:nx")


def expiry(r: redis.Redis | RedisCluster) -> None:
    print("expiry")
    r.set("{compat}:ttl", "v", ex=100)
    check("ttl after set ex", r.ttl("{compat}:ttl"), 100)
    check_true("pttl is milliseconds", 99_000 < r.pttl("{compat}:ttl") <= 100_000)
    check("persist", r.persist("{compat}:ttl"), True)
    check("ttl after persist", r.ttl("{compat}:ttl"), -1)
    check("expire", r.expire("{compat}:ttl", 50), True)
    check("ttl after expire", r.ttl("{compat}:ttl"), 50)
    check("ttl of missing key", r.ttl("{compat}:absent"), -2)
    r.set("{compat}:quick", "v", px=50)
    time.sleep(0.15)
    check("expired key is gone", r.get("{compat}:quick"), None)
    r.delete("{compat}:ttl")


def keyspace(r: redis.Redis | RedisCluster) -> None:
    print("keyspace")
    # Hash tags keep the whole set on one slot so multi-key ops stay legal.
    pairs = {f"{{compat}}:tk{i}": str(i) for i in range(10)}
    check("mset", r.mset(pairs), True)
    check("mget", r.mget(list(pairs)[:3]), [b"0", b"1", b"2"])
    check("type", r.type("{compat}:tk0"), b"string")
    found = set()
    for key in r.scan_iter(match="{compat}:tk*", count=3):
        found.add(key.decode() if isinstance(key, bytes) else key)
    check("scan_iter found all 10", len(found), 10)
    check("delete", r.delete(*pairs), 10)
    check("mget after delete", r.mget(list(pairs)[:2]), [None, None])


def server(r: redis.Redis | RedisCluster, cluster: bool) -> None:
    print("server")
    # A keyless command has no slot, so redis-py's cluster client refuses to
    # guess a destination: it wants an explicit target. That is a client rule,
    # not a server one — MoCache answers these on any node.
    kw = {"target_nodes": RedisCluster.RANDOM} if cluster else {}
    check("ping", r.ping(**kw), True)
    check("echo", r.echo("hi", **kw), b"hi")
    info = r.info(**kw)
    check("info server_name", info.get("server_name"), "mocache")
    check_true("info reports a redis_version", bool(info.get("redis_version")))
    check_true("dbsize is an int", isinstance(r.dbsize(**kw), (int, dict)))
    check_true("config_get returns maxmemory", "maxmemory" in (r.config_get("maxmemory", **kw) or {}))


def cluster_topology(r: RedisCluster) -> None:
    print("cluster")
    nodes = r.get_nodes()
    check_true(f"client discovered {len(nodes)} nodes", len(nodes) >= 3)
    primaries = r.get_primaries()
    check("primaries", len(primaries), 3)
    # Keys spread across all three shards: the client must follow the slot map.
    keys = [f"spread:{i}" for i in range(60)]
    for k in keys:
        r.set(k, k)
    got = [r.get(k) for k in keys]
    check("all keys read back across shards", sum(1 for k, v in zip(keys, got) if v == k.encode()), 60)
    owners = {r.get_node_from_key(k).name for k in keys}
    check_true(f"keys landed on {len(owners)} distinct nodes", len(owners) >= 2)
    for k in keys:
        r.delete(k)
    check_true("keyslot is deterministic", r.keyslot("foo") == 12182)


def follow_moved(r: redis.Redis, host: str, port: int, protocol: int) -> redis.Redis:
    """Point a plain client at whichever node currently owns our keyspace.

    A non-cluster client does not follow MOVED, and the owner of a given slot
    changes after a failover. Resolving it once here keeps the standalone check
    meaningful whatever state the cluster is in — and proves the redirect
    carries a usable address.
    """
    try:
        r.set("{compat}:probe", "1")
        r.delete("{compat}:probe")
        return r
    except redis.exceptions.MovedError as e:
        target = str(e).split()[-1]
        new_host, _, new_port = target.rpartition(":")
        print(f"  (slot moved: following the redirect to {target})")
        r.close()
        return redis.Redis(host=new_host, port=int(new_port), protocol=protocol)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("REDIS_HOST", "cache1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", "6379")))
    ap.add_argument("--cluster", action="store_true")
    ap.add_argument("--protocol", type=int, default=2, choices=(2, 3))
    args = ap.parse_args()

    print(f"redis-py {redis.__version__} -> {args.host}:{args.port} "
          f"({'cluster' if args.cluster else 'standalone'}, RESP{args.protocol})\n")

    if args.cluster:
        r = RedisCluster(host=args.host, port=args.port, protocol=args.protocol)
    else:
        r = redis.Redis(host=args.host, port=args.port, protocol=args.protocol)
        r = follow_moved(r, args.host, args.port, args.protocol)

    try:
        server(r, args.cluster)
        strings(r)
        expiry(r)
        keyspace(r)
        if args.cluster:
            cluster_topology(r)
    finally:
        r.close()

    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} check(s): {', '.join(FAILURES)}")
        return 1
    print("all redis-py checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
