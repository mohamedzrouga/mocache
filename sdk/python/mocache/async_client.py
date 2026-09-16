"""Async MoCache client.

FastAPI and asyncio apps should use this type. Network I/O is offloaded with
``asyncio.to_thread`` so we stay on the Python standard library (no aiohttp).
The public API is still fully async: await get/set/delete/invalidate/aclose.
"""

from __future__ import annotations

import asyncio

from .client import MoCacheClient, MoCacheError


class AsyncMoCacheClient:
    def __init__(
        self,
        nodes: list[str],
        vnodes: int = 100,
        timeout: float = 1.0,
        protocol: str = "http",
        rpc_port: int = 8091,
    ) -> None:
        # One sync client under the hood; to_thread keeps the event loop free.
        self._sync = MoCacheClient(nodes, vnodes=vnodes, timeout=timeout, protocol=protocol, rpc_port=rpc_port)

    async def get(self, key: str) -> str | None:
        return await asyncio.to_thread(self._sync.get, key)

    async def set(self, key: str, value: str, ttl_seconds: int = 300) -> None:
        await asyncio.to_thread(self._sync.set, key, value, ttl_seconds)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._sync.delete, key)

    async def invalidate_prefix(self, prefix: str) -> int:
        return await asyncio.to_thread(self._sync.invalidate_prefix, prefix)

    async def invalidate_regex(self, pattern: str) -> int:
        return await asyncio.to_thread(self._sync.invalidate_regex, pattern)

    async def close(self) -> None:
        await asyncio.to_thread(self._sync.close)

    aclose = close

    async def __aenter__(self) -> AsyncMoCacheClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


__all__ = ["AsyncMoCacheClient", "MoCacheError"]
