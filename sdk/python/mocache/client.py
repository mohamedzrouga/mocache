"""MoCache Python SDK — consistent-hash client with HTTP and unary RPC transports."""

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
_OP_GET, _OP_SET, _OP_DELETE = 1, 2, 3
_STATUS_OK, _STATUS_MISS, _STATUS_ERROR = 0, 1, 2
_MAX_FRAME: Final = 4 << 20


class MoCacheError(Exception):
    """Raised on network/timeout failure; a cache miss is NOT an error and returns None."""

    def __init__(self, message: str, *, timeout: bool = False) -> None:
        super().__init__(message)
        self.timeout = timeout


class HashRing:
    """MD5 consistent hash with virtual nodes; identical to the Go SDK."""

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
        if not self._keys:
            return ""
        idx = bisect_left(self._keys, self._hash(key))
        if idx == len(self._keys):
            idx = 0
        return self._ring[self._keys[idx]]


class MoCacheClient:
    def __init__(
        self,
        nodes: list[str],
        vnodes: int = 100,
        timeout: float = 1.0,
        protocol: str = "http",
        rpc_port: int = 8091,
    ) -> None:
        if protocol not in ("http", "grpc"):
            raise ValueError("protocol must be 'http' or 'grpc'")
        self._ring = HashRing(nodes, vnodes)
        self._timeout = timeout
        self._protocol = protocol
        self._rpc_port = rpc_port
        self._rpc: dict[str, _RPCConn] = {}
        if protocol == "grpc":
            for node in nodes:
                self._rpc[node] = _RPCConn(_rpc_addr(node, rpc_port), timeout)

    def get(self, key: str) -> str | None:
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
        node = self._ring.node(key)
        if self._protocol == "grpc":
            self._rpc[node].call(_OP_DELETE, key)
            return
        code, _ = self._http("DELETE", f"{node}/delete?{urlencode({'key': key})}")
        if code != 200:
            raise MoCacheError(f"DELETE {key!r} via {node}: http {code}")

    def close(self) -> None:
        for conn in self._rpc.values():
            conn.close()

    def _http(self, method: str, url: str, data: bytes | None = None, content_type: str = "") -> tuple[int, bytes]:
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
    def __init__(self, addr: str, timeout: float) -> None:
        self._addr = addr
        self._timeout = timeout
        self._lock = threading.Lock()
        self._sock: socket.socket | None = None
        self._next_id = 0

    def call(self, op: int, key: str, value: bytes = b"", ttl: int = 0) -> tuple[int, bytes]:
        with self._lock:
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
