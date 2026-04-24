from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from core.adapters.prices.coingecko import CoinGeckoPriceAdapter, TokenPrice
from core.enrichment.metadata import STABLECOINS, TokenMetadataLoader


@dataclass
class EnrichmentConfig:
    host: str = "localhost"
    port: int = 8124
    username: str = "default"
    password: str = "nexus"
    database: str = "default"


class PriceFetcher:
    def __init__(
        self,
        config: EnrichmentConfig | None = None,
        adapter: CoinGeckoPriceAdapter | None = None,
        client: Client | None = None,
    ) -> None:
        self.config = config or EnrichmentConfig()
        self.adapter = adapter or CoinGeckoPriceAdapter()
        self._client = client

    def _ensure_client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database=self.config.database,
            )
        return self._client

    def update_prices(
        self,
        chain: str,
        addresses: list[str],
    ) -> int:
        prices = self.adapter.fetch_prices(chain, addresses)
        if not prices:
            return 0
        rows = [
            [
                p.token_address,
                p.chain,
                p.timestamp,
                p.price_usd,
                p.source,
                p.volume_24h_usd,
                datetime.now(timezone.utc),
            ]
            for p in prices
        ]
        client = self._ensure_client()
        client.insert("token_prices", rows)
        return len(rows)

    def update_all_chains(
        self,
        tokens_by_chain: dict[str, list[str]],
    ) -> int:
        total = 0
        for chain, addresses in tokens_by_chain.items():
            total += self.update_prices(chain, addresses)
        return total

    def close(self) -> None:
        self.adapter.close()
        if self._client is not None:
            self._client.close()
