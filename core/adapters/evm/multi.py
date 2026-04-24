"""Multi-chain EVM adapter for parallel ingestion across chains."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Iterator

from core.adapters.base import CanonicalEvent
from core.adapters.evm import EVMAdapter


@dataclass
class ChainConfig:
    chain: str
    rpc_url: str | None = None
    hyper_url: str | None = None
    page_size: int = 2_000


class MultiChainAdapter:
    """Parallel ingestion across multiple EVM chains.

    Usage:
        adapter = MultiChainAdapter([
            ChainConfig("base"),
            ChainConfig("ethereum"),
            ChainConfig("arbitrum"),
        ])
        for event in adapter.ingest(start, end):
            ...
    """

    def __init__(
        self,
        chains: list[ChainConfig],
        hyper_token: str | None = None,
        shared_registry: Any | None = None,
    ) -> None:
        self.adapters: dict[str, EVMAdapter] = {}
        for cfg in chains:
            self.adapters[cfg.chain] = EVMAdapter(
                chain=cfg.chain,
                rpc_url=cfg.rpc_url,
                hyper_token=hyper_token,
                hyper_url=cfg.hyper_url,
                page_size=cfg.page_size,
                registry=shared_registry,
            )

    def ingest(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[CanonicalEvent]:
        """Sequential ingestion across all chains."""
        for adapter in self.adapters.values():
            yield from adapter.ingest(start, end, addresses)

    def ingest_raw(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Sequential raw-log ingestion across all chains."""
        for adapter in self.adapters.values():
            yield from adapter.ingest_raw(start, end, addresses)

    async def ingest_async(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> AsyncIterator[CanonicalEvent]:
        """Parallel async ingestion across all chains."""
        tasks = [
            asyncio.to_thread(lambda a=adapter: list(a.ingest(start, end, addresses)))
            for adapter in self.adapters.values()
        ]
        results = await asyncio.gather(*tasks)
        for events in results:
            for ev in events:
                yield ev

    async def ingest_raw_async(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Parallel async raw-log ingestion across all chains."""
        tasks = [
            asyncio.to_thread(lambda a=adapter: list(a.ingest_raw(start, end, addresses)))
            for adapter in self.adapters.values()
        ]
        results = await asyncio.gather(*tasks)
        for logs in results:
            for log in logs:
                yield log

    def close(self) -> None:
        for adapter in self.adapters.values():
            adapter.close()
