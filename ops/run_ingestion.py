"""One-shot ingestion runner — meant for cron.

Calls the Dagster `raw_logs` + `decoded_events` + `bridge_links` assets
programmatically, then exits. No daemon, no UI, no schedules — just runs
the work once and returns. Designed to be invoked from a `0 * * * *`
cron entry on both your laptop and the VPS.

The token_prices asset runs at most once an hour (cheap CoinGecko poll).
The dune_parity asset is gated to Monday 06:00 UTC to keep within the
free-tier credit budget.

Usage:
    PYTHONPATH=. python ops/run_ingestion.py                     # hourly default
    PYTHONPATH=. python ops/run_ingestion.py --lookback 60       # override window
    PYTHONPATH=. python ops/run_ingestion.py --skip-bridge-links # for backfill mode
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure the Dagster package on the import path before importing assets.
sys.path.insert(0, str(Path(__file__).resolve().parent / "dagster"))

from dagster import materialize  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from nexus_pipeline.assets import (  # noqa: E402
    bridge_links,
    decoded_events,
    dune_parity,
    raw_logs,
    token_prices,
)
from nexus_pipeline.resources import (  # noqa: E402
    ClickHouseResource,
    EVMIngestionResource,
    PostgresResource,
)

load_dotenv()

logger = logging.getLogger("ops.run_ingestion")


def should_run_weekly() -> bool:
    """True only on Monday between 06:00 and 06:59 UTC.

    The cron fires hourly; this gate keeps the Dune-parity check at the
    weekly cadence the BD doc asks for without needing a separate cron.
    """
    now = datetime.now(timezone.utc)
    return now.weekday() == 0 and now.hour == 6


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback", type=int, default=60, help="Minutes of history to ingest.")
    parser.add_argument("--skip-bridge-links", action="store_true")
    parser.add_argument("--skip-prices", action="store_true")
    parser.add_argument(
        "--skip-raw-logs",
        action="store_true",
        help="Skip the raw_logs asset (writes every HyperSync log to canonical_logs). "
        "Filtered ingest in decoded_events covers the useful subset; raw_logs is only "
        "needed to support re-decoding history when a new decoder is added.",
    )
    parser.add_argument("--force-parity", action="store_true", help="Run Dune parity regardless of weekday/hour.")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    assets = [decoded_events]
    if not args.skip_raw_logs:
        assets.insert(0, raw_logs)
    if not args.skip_bridge_links:
        assets.append(bridge_links)
    if not args.skip_prices:
        assets.append(token_prices)
    if args.force_parity or should_run_weekly():
        assets.append(dune_parity)
        logger.info("including dune_parity in this run")

    # Resource defaults are baked at class definition time (dev's
    # localhost:8124 / 5434), so prod must override via env. We read the
    # same vars api/ch.py uses so .env is the single source of truth.
    ch_kwargs = {
        k: v
        for k, v in {
            "host": os.environ.get("CLICKHOUSE_HOST"),
            "port": int(os.environ["CLICKHOUSE_PORT"]) if os.environ.get("CLICKHOUSE_PORT") else None,
            "username": os.environ.get("CLICKHOUSE_USER"),
            "password": os.environ.get("CLICKHOUSE_PASSWORD"),
            "database": os.environ.get("CLICKHOUSE_DB"),
        }.items()
        if v is not None
    }
    pg_dsn = os.environ.get("PROTOCOL_CONTRACTS_DSN")
    pg_kwargs: dict = {}
    if pg_dsn:
        from urllib.parse import urlparse
        u = urlparse(pg_dsn)
        pg_kwargs = {
            "host": u.hostname or "localhost",
            "port": u.port or 5432,
            "username": u.username or "nexus",
            "password": u.password or "nexus",
            "database": (u.path or "/nexus_ops").lstrip("/"),
        }

    resources = {
        "clickhouse": ClickHouseResource(**ch_kwargs),
        "evm": EVMIngestionResource(lookback_minutes=args.lookback),
        "postgres": PostgresResource(**pg_kwargs),
    }

    t0 = time.time()
    logger.info("starting materialize: assets=%s lookback=%dm", [a.key.to_user_string() for a in assets], args.lookback)
    result = materialize(assets, resources=resources)
    elapsed = time.time() - t0

    if result.success:
        logger.info("DONE in %.1fs", elapsed)
        return 0
    logger.error("FAILED after %.1fs — see Dagster logs above", elapsed)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
