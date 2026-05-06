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


# Comprehensive stablecoin registry — all addresses are lowercase for reliable
# matching against decoded event data (which is always lowercase).
# Includes native and bridged versions across Ethereum, Base, Arbitrum, Optimism.
#
# Sources: Circle, Tether, MakerDAO, Frax, Liquity official docs + CoinGecko

STABLECOINS: dict[str, str] = {
    # === Ethereum ===
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0x4fabb145d64652a948d72533023f6e7a623c7c53": "BUSD",
    "0x853d955acef822db058eb8505911ed77f175b99e": "FRAX",
    "0x5f98805a4e8be255a32880fdec7f6728c6568ba0": "LUSD",
    "0x8e870d67f660d95d5be530380d0ec0bd388289e1": "USDP",
    "0x0000000000085d4780b73119b644ae5ecd22b376": "TUSD",
    # === Base ===
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": "DAI",
    "0xb79dd08ea68a908a97220c76d658423e0d7e4b9d": "USD+ (bridged)",  # USDbC
    # === Arbitrum ===
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "USDC",
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "USDT",
    "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
    "0x17fc002b466eec40dae837fc4be5c67993ddbd6f": "FRAX",
    "0x93b346b6bc2548da6a1e7d98e9a421b42541425b": "LUSD",
    "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": "USDC.e (bridged)",
    # === Optimism ===
    "0x0b2c639c533813f4aa9d7837caf62653d097ff85": "USDC",
    "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58": "USDT",
    "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
    "0x2e3d870790dc77a83dd1d18184acc7439a53f475": "FRAX",
    "0xc40f949f8a4e094d1b49a23ea9241d289b7b3679": "LUSD",
    "0x7f5c764cbc14f9669b88837ca1490cca17c31607": "USDC.e (bridged)",
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
