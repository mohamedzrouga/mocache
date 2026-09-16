"""Unit and integration tests for the MoCache Python SDK."""

from __future__ import annotations

import socket
import subprocess
import sys
import time
import unittest
import urllib.request
from pathlib import Path

from mocache.client import HashRing, MoCacheClient, MoCacheError

REPO = Path(__file__).resolve().parents[3]
NODES = [
    "http://cache-0.cache-headless.svc.cluster.local:8090",
    "http://cache-1.cache-headless.svc.cluster.local:8090",
    "http://cache-2.cache-headless.svc.cluster.local:8090",
]


class HashRingTests(unittest.TestCase):
    def test_stable_and_distributed(self) -> None:
        ring = HashRing(NODES, 100)
        self.assertEqual(ring.node("user:123"), ring.node("user:123"))
        owners = {ring.node(f"k{i}") for i in range(300)}
        self.assertEqual(owners, set(NODES))

    def test_wraparound_bisect(self) -> None:
        ring = HashRing(["http://n0:8090"], 8)
        self.assertEqual(ring.node("anything"), "http://n0:8090")


class LiveServerTests(unittest.TestCase):
    http_url: str
    rpc_port: int
    proc: subprocess.Popen[bytes]

    @classmethod
    def setUpClass(cls) -> None:
        http_port = _free_port()
        cls.rpc_port = _free_port()
        cls.http_url = f"http://127.0.0.1:{http_port}"
        cls.proc = subprocess.Popen(
            [
                "go",
                "run",
                "./cmd/mocache",
                "-http",
                f"127.0.0.1:{http_port}",
                "-rpc",
                f"127.0.0.1:{cls.rpc_port}",
                "-capacity",
                "1000",
                "-drain",
                "0s",
                "-janitor",
                "0",
            ],
            cwd=REPO,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 15
        last_err: Exception | None = None
        while time.time() < deadline:
            if cls.proc.poll() is not None:
                out = cls.proc.stdout.read() if cls.proc.stdout else b""
                raise RuntimeError(f"server exited: {out!r}")
            try:
                urllib.request.urlopen(cls.http_url + "/healthz", timeout=0.2)
                return
            except Exception as e:  # noqa: BLE001 — poll until up
                last_err = e
                time.sleep(0.05)
        raise TimeoutError(f"server did not start: {last_err}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.proc.terminate()
        try:
            cls.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.proc.kill()
        if cls.proc.stdout:
            cls.proc.stdout.close()

    def test_http_round_trip(self) -> None:
        c = MoCacheClient([self.http_url], timeout=2.0, protocol="http")
        try:
            c.set("user:123", "some_value", ttl_seconds=300)
            self.assertEqual(c.get("user:123"), "some_value")
            c.delete("user:123")
            self.assertIsNone(c.get("user:123"))
        finally:
            c.close()

    def test_grpc_round_trip(self) -> None:
        c = MoCacheClient([self.http_url], timeout=2.0, protocol="grpc", rpc_port=self.rpc_port)
        try:
            c.set("rpc-key", "rpc-value", ttl_seconds=60)
            self.assertEqual(c.get("rpc-key"), "rpc-value")
            c.delete("rpc-key")
            self.assertIsNone(c.get("rpc-key"))
        finally:
            c.close()

    def test_miss_is_none(self) -> None:
        c = MoCacheClient([self.http_url], timeout=2.0)
        try:
            self.assertIsNone(c.get("absent"))
        finally:
            c.close()

    def test_unavailable_is_error(self) -> None:
        c = MoCacheClient(["http://127.0.0.1:1"], timeout=0.2)
        try:
            with self.assertRaises(MoCacheError):
                c.get("x")
        finally:
            c.close()

    def test_invalidate_prefix_and_regex(self) -> None:
        c = MoCacheClient([self.http_url], timeout=2.0)
        try:
            c.set("user:1", "a")
            c.set("user:2", "b")
            c.set("other", "c")
            self.assertEqual(c.invalidate_prefix("user:"), 2)
            self.assertIsNone(c.get("user:1"))
            self.assertEqual(c.get("other"), "c")
            c.set("sess:x", "1")
            self.assertEqual(c.invalidate_regex(r"^sess:"), 1)
        finally:
            c.close()

    def test_async_client(self) -> None:
        import asyncio

        from mocache import AsyncMoCacheClient

        async def run() -> None:
            async with AsyncMoCacheClient([self.http_url], timeout=2.0) as c:
                await c.set("async:k", "v")
                self.assertEqual(await c.get("async:k"), "v")
                self.assertEqual(await c.invalidate_prefix("async:"), 1)

        asyncio.run(run())


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


if __name__ == "__main__":
    sys.exit(unittest.main())
