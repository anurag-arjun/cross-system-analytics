from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class TokenMetadata:
    address: str
    chain: str
    symbol: str
    decimals: int
    name: str


STABLECOINS = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0x4fabb145d64652a948d72533023f6e7a623c7c53": "BUSD",
}

CHAIN_TO_PLATFORM = {
    "ethereum": "ethereum",
    "base": "base",
    "arbitrum": "arbitrum-one",
    "optimism": "optimistic-ethereum",
}


class TokenMetadataLoader:
    def __init__(self, endpoint: str = "https://api.coingecko.com/api/v3") -> None:
        self.endpoint = endpoint
        self._client: httpx.Client | None = None

    def _ensure_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def load_hardcoded(self) -> list[TokenMetadata]:
        tokens = [
            TokenMetadata(
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "ethereum",
                "WETH",
                18,
                "Wrapped Ether",
            ),
            TokenMetadata(
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "ethereum", "USDC", 6, "USD Coin"
            ),
            TokenMetadata(
                "0xdac17f958d2ee523a2206206994597c13d831ec7", "ethereum", "USDT", 6, "Tether"
            ),
            TokenMetadata(
                "0x6b175474e89094c44da98b954eedeac495271d0f",
                "ethereum",
                "DAI",
                18,
                "Dai Stablecoin",
            ),
            TokenMetadata(
                "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "ethereum", "WBTC", 8, "Wrapped BTC"
            ),
            TokenMetadata(
                "0x4200000000000000000000000000000000000006", "base", "WETH", 18, "Wrapped Ether"
            ),
            TokenMetadata(
                "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", "base", "USDC", 6, "USD Coin"
            ),
            TokenMetadata(
                "0x50c5725949a6f0c72e6c4a641f24049a917db0cb", "base", "DAI", 18, "Dai Stablecoin"
            ),
            TokenMetadata(
                "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                "arbitrum",
                "WETH",
                18,
                "Wrapped Ether",
            ),
            TokenMetadata(
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831", "arbitrum", "USDC", 6, "USD Coin"
            ),
            TokenMetadata(
                "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "arbitrum", "USDT", 6, "Tether"
            ),
            TokenMetadata(
                "0x4200000000000000000000000000000000000042", "optimism", "OP", 18, "Optimism"
            ),
            TokenMetadata(
                "0x0b2c639c533813f4aa9d7837caf62653d097ff85", "optimism", "USDC", 6, "USD Coin"
            ),
        ]
        return tokens

    def fetch_contract_info(
        self,
        chain: str,
        address: str,
    ) -> TokenMetadata | None:
        platform = CHAIN_TO_PLATFORM.get(chain)
        if not platform:
            return None
        client = self._ensure_client()
        url = f"{self.endpoint}/coins/{platform}/contract/{address}"
        try:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            data = resp.json()
            detail = data.get("detail_platforms", {}).get(platform, {})
            return TokenMetadata(
                address=address,
                chain=chain,
                symbol=data.get("symbol", "").upper(),
                decimals=detail.get("decimal_place", 18),
                name=data.get("name", ""),
            )
        except Exception:
            return None

    def is_stablecoin(self, address: str) -> bool:
        return address.lower() in STABLECOINS

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
