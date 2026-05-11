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
    parser.add_argument("--force-parity", action="store_true", help="Run Dune parity regardless of weekday/hour.")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    assets = [raw_logs, decoded_events]
    if not args.skip_bridge_links:
        assets.append(bridge_links)
    if not args.skip_prices:
        assets.append(token_prices)
    if args.force_parity or should_run_weekly():
        assets.append(dune_parity)
        logger.info("including dune_parity in this run")

    resources = {
        "clickhouse": ClickHouseResource(),
        "evm": EVMIngestionResource(lookback_minutes=args.lookback),
        "postgres": PostgresResource(),
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
