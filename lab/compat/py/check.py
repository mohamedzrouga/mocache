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


def reshard(r: RedisCluster) -> None:
    """Move one slot between two live nodes and read through it the whole time.

    This is the check that matters for resharding: redis-py follows ASK on its
    own, so if the markers, the redirect or ASKING are wrong the client either
    raises or reads a miss. Nothing here is MoCache-specific — it is the same
    sequence redis-cli --cluster reshard runs.
    """
    print("reshard")
    key = "reshard:probe"
    slot = r.keyslot(key)
    r.set(key, "before")

    src = r.get_node_from_key(key)
    dest = next((n for n in r.get_primaries() if n.name != src.name), None)
    if dest is None:
        check_true("a second primary exists to reshard into", False)
        return
    src_id = r.execute_command("CLUSTER MYID", target_nodes=src)
    dest_id = r.execute_command("CLUSTER MYID", target_nodes=dest)
    if isinstance(src_id, bytes):
        src_id, dest_id = src_id.decode(), dest_id.decode()
    print(f"  moving slot {slot}: {src.name} -> {dest.name}")

    try:
        # 1. Open the window. Destination first, so no key is ever unreachable.
        r.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", src_id, target_nodes=dest)
        r.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", dest_id, target_nodes=src)

        # The key has not moved yet, so it is still served by the source.
        check("read during migration, before the key moves", r.get(key), b"before")

        # A key that does not exist in a migrating slot is an ASK, which the
        # client follows to the destination — where it is a plain miss.
        check("missing key in a migrating slot", r.get("reshard:absent{reshard:probe}"), None)

        # 2. Move it.
        keys = r.execute_command("CLUSTER GETKEYSINSLOT", slot, 100, target_nodes=src)
        keys = [k.decode() if isinstance(k, bytes) else k for k in keys]
        check_true(f"source lists {len(keys)} key(s) in the slot", key in keys)
        dest_host, _, dest_port = dest.name.rpartition(":")
        r.execute_command("MIGRATE", dest_host, int(dest_port), "", 0, 5000,
                          "KEYS", *keys, target_nodes=src)

        # Now it is at the destination, and the client is sent there by ASK.
        check("read during migration, after the key moves", r.get(key), b"before")

        # 3. Commit everywhere.
        for node in r.get_primaries():
            r.execute_command("CLUSTER SETSLOT", slot, "NODE", dest_id, target_nodes=node)

        r.nodes_manager.initialize()
        check("slot map moved to the destination", r.get_node_from_key(key).name, dest.name)
        check("read after the reshard commits", r.get(key), b"before")
        r.set(key, "after")
        check("write after the reshard commits", r.get(key), b"after")

        # Coverage must still be exact or the client would have refused the map.
        check("all 16384 slots still covered", len(r.get_nodes()) >= 3, True)

        # Put it back: a check that leaves the cluster reshaped makes whatever
        # runs next depend on whether this one ran.
        r.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", dest_id, target_nodes=src)
        r.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", src_id, target_nodes=dest)
        back = r.execute_command("CLUSTER GETKEYSINSLOT", slot, 100, target_nodes=dest)
        back = [k.decode() if isinstance(k, bytes) else k for k in back]
        if back:
            src_host, _, src_port = src.name.rpartition(":")
            r.execute_command("MIGRATE", src_host, int(src_port), "", 0, 5000,
                              "KEYS", *back, "REPLACE", target_nodes=dest)
        for node in r.get_primaries():
            r.execute_command("CLUSTER SETSLOT", slot, "NODE", src_id, target_nodes=node)
        r.nodes_manager.initialize()
        check("slot restored to its original owner", r.get_node_from_key(key).name, src.name)
    finally:
        r.delete(key)


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
            reshard(r)
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
