from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx


@dataclass
class TokenPrice:
    token_address: str
    chain: str
    timestamp: datetime
    price_usd: Decimal
    source: str
    volume_24h_usd: Decimal | None


CHAIN_TO_PLATFORM = {
    "ethereum": "ethereum",
    "base": "base",
    "arbitrum": "arbitrum-one",
    "optimism": "optimistic-ethereum",
}


class CoinGeckoPriceAdapter:
    def __init__(self, endpoint: str = "https://api.coingecko.com/api/v3") -> None:
        self.endpoint = endpoint
        self._client: httpx.Client | None = None

    def _ensure_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def fetch_prices(
        self,
        chain: str,
        addresses: list[str],
    ) -> list[TokenPrice]:
        platform = CHAIN_TO_PLATFORM.get(chain)
        if not platform or not addresses:
            return []

        client = self._ensure_client()
        joined = ",".join(addresses)
        url = f"{self.endpoint}/simple/token_price/{platform}"
        params = {
            "contract_addresses": joined,
            "vs_currencies": "usd",
            "include_24hr_vol": "true",
        }

        try:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return []

        now = datetime.now(timezone.utc)
        results: list[TokenPrice] = []
        for addr in addresses:
            key = addr.lower()
            info = data.get(key)
            if not info:
                continue
            price = info.get("usd")
            if price is None:
                continue
            vol = info.get("usd_24h_vol")
            results.append(
                TokenPrice(
                    token_address=addr,
                    chain=chain,
                    timestamp=now,
                    price_usd=Decimal(str(price)),
                    source="coingecko",
                    volume_24h_usd=Decimal(str(vol)) if vol is not None else None,
                )
            )
        return results

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
