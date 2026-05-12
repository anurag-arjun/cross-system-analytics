"""Seed nexus.token_metadata with the curated TokenMetadataLoader.load_hardcoded() list.

USD enrichment (token_prices asset, PriceResolver) reads token_metadata to
discover which tokens to fetch prices for. With the table empty, every
canonical_event lands with amount_usd = 0. This script seeds the ~13 major
tokens (WETH/USDC/USDT/DAI/WBTC across the in-scope chains) — covers the
bulk of bridge + swap volume, and is enough to populate the dashboard's
USD numbers.

Re-running is safe: the table is ReplacingMergeTree on (chain, address)
with `inserted_at` as version, so the latest row wins on read.

Usage:
    PYTHONPATH=. python ops/seed_token_metadata.py
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import clickhouse_connect
from dotenv import load_dotenv

from core.enrichment.metadata import TokenMetadataLoader

load_dotenv()


def main() -> int:
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8124")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "nexus"),
        database=os.environ.get("CLICKHOUSE_DB", "nexus"),
    )

    loader = TokenMetadataLoader()
    tokens = loader.load_hardcoded()

    now = datetime.now(timezone.utc)
    rows = [
        [
            t.address.lower(),
            t.chain,
            t.symbol,
            t.decimals,
            t.name,
            None,  # coingecko_id — not on the hardcoded list
            now,
        ]
        for t in tokens
    ]
    cols = ["token_address", "chain", "symbol", "decimals", "name", "coingecko_id", "inserted_at"]

    client.insert("token_metadata", rows, column_names=cols)
    print(f"seeded {len(rows)} tokens into nexus.token_metadata")

    # Show what landed
    r = client.query("SELECT chain, symbol FROM token_metadata FINAL ORDER BY chain, symbol")
    for row in r.result_rows:
        print(f"  {row[0]:>10} {row[1]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
