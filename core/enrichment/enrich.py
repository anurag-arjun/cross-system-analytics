from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from core.enrichment.metadata import STABLECOINS


@dataclass
class EnrichmentConfig:
    host: str = "localhost"
    port: int = 8124
    username: str = "default"
    password: str = "nexus"
    database: str = "default"
    batch_size: int = 10_000


class PriceEnrichment:
    def __init__(
        self,
        config: EnrichmentConfig | None = None,
        client: Client | None = None,
    ) -> None:
        self.config = config or EnrichmentConfig()
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

    def enrich_batch(
        self,
        start: datetime,
        end: datetime,
    ) -> int:
        client = self._ensure_client()

        select_sql = """
            SELECT
                e.event_id,
                e.amount_in,
                e.amount_out,
                e.token_in,
                e.token_out,
                e.timestamp,
                e.chain,
                m_in.decimals AS decimals_in,
                m_out.decimals AS decimals_out,
                p_in.price_usd AS price_in,
                p_out.price_usd AS price_out
            FROM canonical_events e
            LEFT JOIN token_metadata m_in
                ON e.chain = m_in.chain AND e.token_in = m_in.token_address
            LEFT JOIN token_metadata m_out
                ON e.chain = m_out.chain AND e.token_out = m_out.token_address
            ASOF LEFT JOIN token_prices p_in
                ON e.chain = p_in.chain
               AND e.token_in = p_in.token_address
               AND e.timestamp >= p_in.timestamp
            ASOF LEFT JOIN token_prices p_out
                ON e.chain = p_out.chain
               AND e.token_out = p_out.token_address
               AND e.timestamp >= p_out.timestamp
            WHERE e.timestamp >= {start:DateTime64(3)}
              AND e.timestamp <= {end:DateTime64(3)}
              AND e.amount_in_usd IS NULL
              AND (e.amount_in IS NOT NULL OR e.amount_out IS NOT NULL)
            LIMIT {limit:UInt32}
        """

        result = client.query(
            select_sql,
            parameters={
                "start": start,
                "end": end,
                "limit": self.config.batch_size,
            },
        )

        if not result.result_rows:
            return 0

        updates: list[tuple] = []
        for row in result.result_rows:
            (
                event_id,
                amount_in,
                amount_out,
                token_in,
                token_out,
                ts,
                chain,
                dec_in,
                dec_out,
                price_in,
                price_out,
            ) = row

            amount_in_usd = self._compute_usd(amount_in, token_in, dec_in, price_in)
            amount_out_usd = self._compute_usd(amount_out, token_out, dec_out, price_out)

            if amount_in_usd is not None or amount_out_usd is not None:
                updates.append((amount_in_usd, amount_out_usd, event_id))

        if not updates:
            return 0

        client.insert("canonical_events_updates", updates)
        return len(updates)

    def _compute_usd(
        self,
        raw_amount: Any,
        token_address: Any,
        decimals: Any,
        price_usd: Any,
    ) -> Decimal | None:
        if raw_amount is None:
            return None

        if token_address and str(token_address).lower() in STABLECOINS:
            return Decimal(str(raw_amount)) / Decimal(10 ** int(decimals or 6))

        if decimals is None or price_usd is None:
            return None

        scaled = Decimal(str(raw_amount)) / Decimal(10 ** int(decimals))
        return scaled * Decimal(str(price_usd))

    def run_batch(
        self,
        window: timedelta = timedelta(hours=1),
    ) -> int:
        end = datetime.now(timezone.utc)
        start = end - window
        return self.enrich_batch(start, end)

    def coverage(self, window: timedelta = timedelta(days=1)) -> dict[str, Any]:
        client = self._ensure_client()
        end = datetime.now(timezone.utc)
        start = end - window

        sql = """
            SELECT
                countIf(amount_in IS NOT NULL) AS total_with_amount,
                countIf(amount_in_usd IS NOT NULL) AS enriched,
                countIf(amount_in IS NOT NULL AND amount_in_usd IS NULL) AS pending
            FROM canonical_events
            WHERE timestamp >= {start:DateTime64(3)}
              AND timestamp <= {end:DateTime64(3)}
        """
        result = client.query(sql, parameters={"start": start, "end": end})
        if not result.result_rows:
            return {"total": 0, "enriched": 0, "pending": 0, "rate": 0.0}

        total, enriched, pending = result.result_rows[0]
        rate = enriched / total if total else 0.0
        return {
            "total": total,
            "enriched": enriched,
            "pending": pending,
            "rate": round(rate, 4),
        }

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
