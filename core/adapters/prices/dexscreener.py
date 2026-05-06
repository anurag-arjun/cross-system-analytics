"""Token price adapters — CoinGecko (preferred, needs key) + DexScreener (fallback, no key).

Usage:
    from core.adapters.prices.dexscreener import DexScreenerPriceAdapter
    adapter = DexScreenerPriceAdapter()
    prices = adapter.fetch_prices('base', ['0x4200...0006'])
"""

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


# DexScreener chain IDs for the /latest/dex/tokens/{address} endpoint
DEXSCREENER_CHAIN_IDS: dict[str, str] = {
    "ethereum": "ethereum",
    "base": "base",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
}


class DexScreenerPriceAdapter:
    """Fetch token prices from DexScreener's free API.

    No API key required.  Rate limit: 60 req/min for token profiles.
    Uses the /latest/dex/tokens/{address} endpoint for chain-scoped prices.
    """

    def __init__(self, endpoint: str = "https://api.dexscreener.com") -> None:
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
        """Fetch current USD prices for a list of token addresses on a chain.

        DexScreener returns DEX pair data; we take the price from the first
        pair with sufficient liquidity.
        """
        if not addresses:
            return []

        client = self._ensure_client()
        results: list[TokenPrice] = []

        for addr in addresses:
            try:
                url = f"{self.endpoint}/latest/dex/tokens/{addr}"
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                continue

            pairs = data.get("pairs", [])
            if not pairs:
                continue

            # Pick the pair with highest liquidity on the target chain
            best_price = None
            best_liq = Decimal(0)
            for pair in pairs:
                if pair.get("chainId", "").lower() != chain.lower():
                    continue
                price_str = pair.get("priceUsd")
                liq_str = pair.get("liquidity", {}).get("usd", 0) or 0
                if price_str:
                    liq = Decimal(str(liq_str))
                    if liq >= best_liq:
                        best_liq = liq
                        best_price = Decimal(str(price_str))

            if best_price is not None:
                results.append(
                    TokenPrice(
                        token_address=addr,
                        chain=chain,
                        timestamp=datetime.now(timezone.utc),
                        price_usd=best_price,
                        source="dexscreener",
                        volume_24h_usd=best_liq if best_liq > 0 else None,
                    )
                )

        return results

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
