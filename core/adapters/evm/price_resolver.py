"""In-ingestion USD price resolution for EVM adapter.

Computes amount_in_usd / amount_out_usd at CanonicalEvent construction time
using stablecoin overrides + a cached view of the token_prices table.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from core.enrichment.metadata import STABLECOINS


class PriceResolver:
    """Resolve token prices to USD at ingestion time.

    Maintains a memory cache of recent token prices and decimals, queried
    from ClickHouse token_prices + token_metadata tables.  Cache is refreshed
    on init and can be re-warmed periodically.

    Stablecoins always resolve to price_usd = 1.0 regardless of cache state.
    """

    def __init__(self, clickhouse_client: Any | None = None) -> None:
        self._client = clickhouse_client
        # {(chain, token_address): (price_usd, decimals)}
        self._cache: dict[tuple[str, str], tuple[Decimal, int]] = {}
        self._last_refresh: datetime | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(
        self,
        chain: str,
        token_address: str | None,
        raw_amount: Any | None,
    ) -> Decimal | None:
        """Compute USD value for a token amount.

        Returns None if the price or decimals cannot be resolved (the
        async enrichment pass will handle it later).
        """
        if raw_amount is None or token_address is None:
            return None

        addr = str(token_address).lower()
        amount = Decimal(str(raw_amount))

        # Stablecoins are always $1.00
        if addr in STABLECOINS:
            decimals = self._get_decimals(chain, addr)
            return amount / Decimal(10 ** decimals)

        # Look up in price cache
        price_info = self._cache.get((chain, addr))
        if price_info is None:
            return None

        price_usd, decimals = price_info
        if price_usd <= 0 or decimals < 0:
            return None

        scaled = amount / Decimal(10**decimals)
        return scaled * price_usd

    def warm(self) -> int:
        """Refresh the price cache from ClickHouse. Returns entries loaded."""
        if self._client is None:
            return 0
        try:
            rows = self._client.query(
                """
                SELECT
                    p.chain,
                    p.token_address,
                    p.price_usd,
                    COALESCE(m.decimals, 18) AS decimals
                FROM token_prices p
                LEFT JOIN token_metadata m
                    ON p.chain = m.chain
                   AND p.token_address = m.token_address
                WHERE p.timestamp >= {start:DateTime64(3)}
                """,
                parameters={"start": datetime.now(timezone.utc) - timedelta(hours=2)},
            )
            for row in rows.result_rows:
                chain, addr, price, dec = row
                self._cache[(chain, addr.lower())] = (
                    Decimal(str(price)),
                    int(dec),
                )
            self._last_refresh = datetime.now(timezone.utc)
            return len(rows.result_rows)
        except Exception:
            return 0

    def _get_decimals(self, chain: str, addr: str) -> int:
        """Get token decimals from cache, default to 6 for stablecoins."""
        info = self._cache.get((chain, addr))
        if info is not None:
            return info[1]
        return 6  # USDC/USDT default

    @property
    def cache_size(self) -> int:
        return len(self._cache)

    @property
    def last_refresh(self) -> datetime | None:
        return self._last_refresh
