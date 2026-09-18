"""MoCache Python SDK — consistent-hash client with HTTP and unary RPC transports.

Go import counterpart: github.com/mohamedzrouga/mocache/sdk/go

A cache miss is None. Network/timeout failures raise MoCacheError. Call close()
(or use the context manager) so RPC sockets are not leaked across process
lifetime. Node restarts empty the shard; the client retries a broken TCP
session once so a rolling restart is a miss, not a crash.
"""

from __future__ import annotations

import hashlib
import json
import socket
import struct
import threading
from bisect import bisect_left
from typing import Final
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

_MAGIC: Final = b"MOC1"
_VERSION: Final = 1
_OP_GET, _OP_SET, _OP_DELETE, _OP_INVALIDATE = 1, 2, 3, 5
_STATUS_OK, _STATUS_MISS, _STATUS_ERROR = 0, 1, 2
_MAX_FRAME: Final = 4 << 20


class MoCacheError(Exception):
    """Raised on network/timeout failure; a cache miss is NOT an error and returns None."""

    def __init__(self, message: str, *, timeout: bool = False) -> None:
        super().__init__(message)
        self.timeout = timeout


class HashRing:
    """MD5 consistent hash with virtual nodes; identical to the Go SDK.

    Lookup is the first ring position at or after the key hash (bisect_left),
    wrapping to index 0 so the ring is circular.
    """

    def __init__(self, nodes: list[str], vnodes: int = 100) -> None:
        if vnodes < 1:
            vnodes = 100
        ring: dict[int, str] = {}
        for node in nodes:
            for i in range(vnodes):
                ring[self._hash(f"{node}:{i}")] = node
        self._keys = sorted(ring)
        self._ring = ring

    @staticmethod
    def _hash(s: str) -> int:
        return int.from_bytes(hashlib.md5(s.encode("utf-8"), usedforsecurity=False).digest(), "big")

    def node(self, key: str) -> str:
        """Lookup is first ring position at or after the key hash (bisect_left)."""
        if not self._keys:
            return ""
        idx = bisect_left(self._keys, self._hash(key))
        if idx == len(self._keys):
            idx = 0
        return self._ring[self._keys[idx]]


SLOT_COUNT: Final = 16384


def _crc16(data: bytes) -> int:
    """CRC-16/XMODEM (poly 0x1021, zero init, no reflection) — Redis's variant.

    Its check value over b"123456789" is 0x31C3, asserted in the tests. A client
    that computes slots differently does not raise, it silently misroutes.
    """
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def hash_tag(key: str) -> str:
    """Substring between the first '{' and the next '}', when non-empty; else the key.

    It is how a caller forces related keys onto one node ("{user:1}:name").
    """
    open_at = key.find("{")
    if open_at < 0:
        return key
    closing = key.find("}", open_at + 1)
    if closing <= open_at + 1:
        return key
    return key[open_at + 1 : closing]


def key_slot(key: str) -> int:
    """CRC16 of the hash tag modulo 16384 — the slot every Redis client computes."""
    return _crc16(hash_tag(key).encode("utf-8")) % SLOT_COUNT


def parse_slot_ranges(spec: str) -> list[tuple[int, int]]:
    """Read the slot list form -cluster-peer uses: "0-5460" or "5461-10922,12000-12100"."""
    out: list[tuple[int, int]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        lo, sep, hi = part.partition("-")
        try:
            start = int(lo.strip())
            end = int(hi.strip()) if sep else start
        except ValueError:
            raise ValueError(f"bad slot range {part!r}") from None
        if start < 0 or end >= SLOT_COUNT or start > end:
            raise ValueError(f"slot range {part!r} outside 0-{SLOT_COUNT - 1}")
        out.append((start, end))
    if not out:
        raise ValueError(f"no slots in {spec!r}")
    return out


class SlotRouter:
    """Redis Cluster slot routing; identical to the Go SDK's slotRouter.

    The SDK's hash ring and a Redis client's CRC16 slots do not agree on where a
    key belongs, so a key written through this SDK and read through redis-py can
    land on different nodes and read as a miss. Routing by slots is what lets one
    keyspace be shared by both access paths.

    The map is static. It does not follow a failover: afterwards the promoted
    shard's slots are served by a node the map does not name, and lookups miss
    until it is updated. A client that must track failover should use the RESP
    port with a real cluster client, which follows MOVED.
    """

    def __init__(self, slots: dict[str, str]) -> None:
        owner: list[str | None] = [None] * SLOT_COUNT
        # Sorted so a duplicate is always reported against the same pair of
        # nodes, whichever order the caller built the mapping in.
        for node in sorted(slots):
            for start, end in parse_slot_ranges(slots[node]):
                for slot in range(start, end + 1):
                    if owner[slot] is not None:
                        raise ValueError(f"slot {slot} claimed by both {owner[slot]} and {node}")
                    owner[slot] = node
        missing = owner.index(None) if None in owner else -1
        if missing >= 0:
            raise ValueError(f"slot {missing} has no node; the map must cover all {SLOT_COUNT} slots")
        self._owner: list[str] = owner  # type: ignore[assignment]

    def node(self, key: str) -> str:
        return self._owner[key_slot(key)]


class MoCacheClient:
    """Synchronous client. Safe to share across threads (one mutex per RPC node).

    For asyncio / FastAPI prefer :class:`AsyncMoCacheClient`.
    """
    def __init__(
        self,
        nodes: list[str],
        vnodes: int = 100,
        timeout: float = 1.0,
        protocol: str = "http",
        rpc_port: int = 8091,
        slots: dict[str, str] | None = None,
    ) -> None:
        if protocol not in ("http", "grpc"):
            raise ValueError("protocol must be 'http' or 'grpc'")
        # slots routes by CRC16 like a Redis client; without it the MD5 ring
        # stays the default, so an existing deployment's keys do not move.
        self._ring: HashRing | SlotRouter = SlotRouter(slots) if slots else HashRing(nodes, vnodes)
        self._nodes = list(nodes)
        self._timeout = timeout
        self._protocol = protocol
        self._rpc_port = rpc_port
        self._closed = False
        self._close_lock = threading.Lock()
        self._rpc: dict[str, _RPCConn] = {}
        if protocol == "grpc":
            for node in nodes:
                self._rpc[node] = _RPCConn(_rpc_addr(node, rpc_port), timeout)

    def get(self, key: str) -> str | None:
        self._check_open()
        node = self._ring.node(key)
        if self._protocol == "grpc":
            status, value = self._rpc[node].call(_OP_GET, key)
            if status == _STATUS_MISS:
                return None
            return value.decode("utf-8")
        code, body = self._http("GET", f"{node}/get?{urlencode({'key': key})}")
        if code == 404:
            return None
        if code != 200:
            raise MoCacheError(f"GET {key!r} via {node}: http {code}")
        return body.decode("utf-8")

    def set(self, key: str, value: str, ttl_seconds: int = 300) -> None:
        self._check_open()
        node = self._ring.node(key)
        if self._protocol == "grpc":
            ttl = ttl_seconds if ttl_seconds > 0 else 0
            self._rpc[node].call(_OP_SET, key, value.encode("utf-8"), ttl)
            return
        payload = json.dumps({"key": key, "value": value, "ttl_seconds": ttl_seconds}).encode()
        code, _ = self._http("POST", f"{node.rstrip('/')}/set", payload, "application/json")
        if code != 200:
            raise MoCacheError(f"SET {key!r} via {node}: http {code}")

    def delete(self, key: str) -> None:
        self._check_open()
        node = self._ring.node(key)
        if self._protocol == "grpc":
            self._rpc[node].call(_OP_DELETE, key)
            return
        code, _ = self._http("DELETE", f"{node}/delete?{urlencode({'key': key})}")
        if code != 200:
            raise MoCacheError(f"DELETE {key!r} via {node}: http {code}")

    def invalidate_prefix(self, prefix: str) -> int:
        """Delete keys starting with prefix on every node. Returns total deleted."""
        return self._invalidate("prefix", prefix)

    def invalidate_regex(self, pattern: str) -> int:
        """Delete keys matching pattern (compiled as RE2 on the server)."""
        return self._invalidate("regex", pattern)

    def _invalidate(self, kind: str, pattern: str) -> int:
        # Broadcast: consistent hashing can place matching keys on any shard.
        self._check_open()
        total = 0
        first: MoCacheError | None = None
        for node in self._nodes:
            try:
                if self._protocol == "grpc":
                    _, body = self._rpc[node].call(_OP_INVALIDATE, pattern, kind.encode())
                    total += int(body.decode() or "0")
                    continue
                payload = json.dumps({kind: pattern}).encode()
                code, body = self._http("POST", f"{node.rstrip('/')}/invalidate", payload, "application/json")
                if code != 200:
                    raise MoCacheError(f"invalidate via {node}: http {code}")
                total += int(json.loads(body.decode()).get("deleted", 0))
            except MoCacheError as e:
                if first is None:
                    first = e
        if first is not None:
            raise first
        return total

    def close(self) -> None:
        with self._close_lock:
            self._closed = True
            for conn in self._rpc.values():
                conn.close()

    def __enter__(self) -> MoCacheClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise MoCacheError("client closed")

    def _http(self, method: str, url: str, data: bytes | None = None, content_type: str = "") -> tuple[int, bytes]:
        try:
            return self._http_once(method, url, data, content_type)
        except MoCacheError as e:
            # Rolling restart often RSTs a keep-alive socket; retry once, not on timeout.
            if e.timeout:
                raise
            return self._http_once(method, url, data, content_type)

    def _http_once(self, method: str, url: str, data: bytes | None, content_type: str) -> tuple[int, bytes]:
        headers = {"Content-Type": content_type} if content_type else {}
        req = Request(url, data=data, method=method, headers=headers)
        try:
            with urlopen(req, timeout=self._timeout) as resp:
                return resp.status, resp.read()
        except HTTPError as e:
            body = e.read()
            if e.code == 404 and method == "GET":
                return 404, body
            raise MoCacheError(f"{method} {url}: http {e.code}") from e
        except TimeoutError as e:
            raise MoCacheError(f"{method} {url}: timeout", timeout=True) from e
        except URLError as e:
            timeout = bool(getattr(e, "reason", None) and getattr(e.reason, "timeout", False)) or isinstance(
                e.reason, TimeoutError
            )
            raise MoCacheError(f"{method} {url}: {e}", timeout=timeout) from e


def _rpc_addr(node: str, port: int) -> str:
    u = urlparse(node)
    host = u.hostname or node.split(":")[0]
    if ":" in host:
        return f"[{host}]:{port}"
    return f"{host}:{port}"


class _RPCConn:
    """One persistent TCP session per cache node. The lock serializes frames
    (the protocol is not multiplexed) and reconnect after a peer restart."""

    def __init__(self, addr: str, timeout: float) -> None:
        self._addr = addr
        self._timeout = timeout
        self._lock = threading.Lock()
        self._sock: socket.socket | None = None
        self._next_id = 0

    def call(self, op: int, key: str, value: bytes = b"", ttl: int = 0) -> tuple[int, bytes]:
        with self._lock:
            try:
                return self._call_locked(op, key, value, ttl)
            except MoCacheError as e:
                if e.timeout:
                    raise
                self._reset()
                return self._call_locked(op, key, value, ttl)

    def _call_locked(self, op: int, key: str, value: bytes, ttl: int) -> tuple[int, bytes]:
        self._next_id = (self._next_id + 1) & 0xFFFFFFFF
        req_id = self._next_id
        try:
            sock = self._ensure()
            sock.sendall(_encode_request(op, req_id, key, value, ttl))
            status, rid, body, err = _read_response(sock)
        except (TimeoutError, socket.timeout, OSError) as e:
            self._reset()
            timeout = isinstance(e, (TimeoutError, socket.timeout))
            raise MoCacheError(f"rpc {self._addr} {key!r}: {e}", timeout=timeout) from e
        if rid != req_id:
            self._reset()
            raise MoCacheError(f"rpc {self._addr}: response id mismatch")
        if status == _STATUS_ERROR:
            raise MoCacheError(err or "rpc error")
        return status, body

    def close(self) -> None:
        self._reset()

    def _ensure(self) -> socket.socket:
        if self._sock is None:
            sock = socket.create_connection(_split_addr(self._addr), timeout=self._timeout)
            sock.settimeout(self._timeout)
            self._sock = sock
        return self._sock

    def _reset(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None


def _split_addr(addr: str) -> tuple[str, int]:
    host, _, port = addr.rpartition(":")
    return host, int(port)


def _encode_request(op: int, req_id: int, key: str, value: bytes, ttl: int) -> bytes:
    kb = key.encode("utf-8")
    payload = struct.pack("!4sBBIH", _MAGIC, _VERSION, op, req_id, len(kb)) + kb
    payload += struct.pack("!II", ttl, len(value)) + value
    if len(payload) > _MAX_FRAME:
        raise MoCacheError("frame too large")
    return struct.pack("!I", len(payload)) + payload


def _recvall(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise OSError("connection closed")
        buf.extend(chunk)
    return bytes(buf)


def _read_response(sock: socket.socket) -> tuple[int, int, bytes, str]:
    (n,) = struct.unpack("!I", _recvall(sock, 4))
    if n == 0 or n > _MAX_FRAME:
        raise MoCacheError("frame too large")
    payload = _recvall(sock, n)
    if len(payload) < 16:
        raise MoCacheError("truncated frame")
    magic, ver, status, req_id, vlen = struct.unpack("!4sBBII", payload[:14])
    if magic != _MAGIC or ver != _VERSION:
        raise MoCacheError("bad magic or version")
    value = payload[14 : 14 + vlen]
    off = 14 + vlen
    (elen,) = struct.unpack("!H", payload[off : off + 2])
    err = payload[off + 2 : off + 2 + elen].decode("utf-8", "replace")
    return status, req_id, value, err
