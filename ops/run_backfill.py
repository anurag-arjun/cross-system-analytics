"""Historical backfill — iterate a window in 1-hour chunks, write canonical
logs + events.

The hourly cron (ops/run_ingestion.py) only catches the latest 60 minutes;
this script fills the gap when you want N days of history at once. It is
idempotent — canonical_events and canonical_logs are ReplacingMergeTree,
so duplicate rows from re-running an overlapping window collapse at merge
time.

Chunked to keep individual HyperSync queries bounded — the 5-min smoke
showed ~140k decoded events on Polygon alone, so 30 days × 5 chains in
one shot would exceed any reasonable batch.

Usage:
    PYTHONPATH=. python ops/run_backfill.py --days 30
    PYTHONPATH=. python ops/run_backfill.py --start 2026-04-11T00:00:00 --end 2026-05-11T00:00:00
    PYTHONPATH=. python ops/run_backfill.py --chains base polygon --hours 24
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "dagster"))

from dotenv import load_dotenv  # noqa: E402

from core.adapters.evm.multi import ChainConfig, MultiChainAdapter  # noqa: E402
from core.registry.protocol_contracts import (  # noqa: E402
    PostgresProtocolContractStore,
    make_cached_resolver,
)
from core.sink import RawLogSink, ClickHouseSink, SinkConfig  # noqa: E402
from nexus_pipeline.assets import CHAINS  # noqa: E402

load_dotenv()

logger = logging.getLogger("ops.run_backfill")


def parse_ts(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=None, help="Backfill last N days.")
    parser.add_argument("--hours", type=int, default=None, help="Backfill last N hours.")
    parser.add_argument("--start", type=str, default=None, help="ISO start (UTC).")
    parser.add_argument("--end", type=str, default=None, help="ISO end (UTC); default now.")
    parser.add_argument(
        "--chunk-minutes", type=int, default=60,
        help="Chunk size in minutes (default 60).",
    )
    parser.add_argument(
        "--sink-batch-size", type=int, default=10_000,
        help="ClickHouse insert batch size for raw logs and decoded events (default 10000).",
    )
    parser.add_argument(
        "--raw-only", action="store_true",
        help="Write canonical_logs only; skip decoding and canonical_events inserts.",
    )
    parser.add_argument(
        "--chains", nargs="+", default=None,
        help="Subset of chains to backfill (default all).",
    )
    parser.add_argument(
        "--postgres",
        default=os.environ.get(
            "PROTOCOL_CONTRACTS_DSN",
            "postgresql://nexus:nexus@localhost:5434/nexus_ops",
        ),
    )
    parser.add_argument("--ch-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8124")))
    parser.add_argument("--ch-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CLICKHOUSE_PASSWORD", "nexus"))
    parser.add_argument("--ch-database", default=os.getenv("CLICKHOUSE_DB", "nexus"))
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # Determine window
    end = parse_ts(args.end) if args.end else datetime.now(timezone.utc)
    if args.start:
        start = parse_ts(args.start)
    elif args.days is not None:
        start = end - timedelta(days=args.days)
    elif args.hours is not None:
        start = end - timedelta(hours=args.hours)
    else:
        logger.error("must specify one of --days / --hours / --start")
        return 2

    chain_filter = set(args.chains) if args.chains else None
    chains = [c for c in CHAINS if chain_filter is None or c.chain in chain_filter]
    if not chains:
        logger.error("no matching chains in CHAINS")
        return 2

    # Resolver: address-first decoder lookup, preloaded into memory.
    # Raw-only backfill does not decode, so skip the Postgres lookup entirely.
    resolver = None
    if not args.raw_only:
        pc_store = PostgresProtocolContractStore(args.postgres)
        resolver = make_cached_resolver(pc_store) if pc_store.count() > 0 else None
        if resolver is None:
            logger.warning("protocol_contracts is empty — decode-time lookup disabled")

    adapter = MultiChainAdapter(
        chains=chains,
        hyper_token=os.getenv("HYPERSYNC_TOKEN"),
        protocol_resolver=resolver,
    )

    sink_cfg_kwargs = dict(
        host=args.ch_host, port=args.ch_port,
        username=args.ch_user, password=args.ch_password,
        database=args.ch_database, batch_size=args.sink_batch_size,
    )
    raw_sink = RawLogSink(SinkConfig(table="canonical_logs", **sink_cfg_kwargs))
    event_sink = None if args.raw_only else ClickHouseSink(SinkConfig(table="canonical_events", **sink_cfg_kwargs))

    chunk = timedelta(minutes=args.chunk_minutes)
    total_raw = 0
    total_events = 0
    total_chunks = 0

    logger.info(
        "backfill window=%s..%s (%.1fh) chunks=%dm chains=%s",
        start.isoformat(), end.isoformat(),
        (end - start).total_seconds() / 3600.0,
        args.chunk_minutes, [c.chain for c in chains],
    )
    logger.info("clickhouse sink batch_size=%d raw_only=%s", args.sink_batch_size, args.raw_only)

    cursor = start
    try:
        while cursor < end:
            chunk_end = min(cursor + chunk, end)
            t0 = time.time()
            chunk_raw = 0
            chunk_events = 0
            for chain_name, chain_adapter in adapter.adapters.items():
                logs = list(chain_adapter.ingest_raw(cursor, chunk_end))
                if logs:
                    raw_sink.write(logs)
                    chunk_raw += len(logs)
                    if event_sink is not None:
                        events = list(chain_adapter.decode_logs(logs))
                    else:
                        events = []
                    if events and event_sink is not None:
                        event_sink.write(events)
                        chunk_events += len(events)
            elapsed = time.time() - t0
            total_chunks += 1
            total_raw += chunk_raw
            total_events += chunk_events
            logger.info(
                "[%s] %s..%s  raw=%d events=%d  (%.1fs, total raw=%d events=%d)",
                f"chunk {total_chunks}",
                cursor.strftime("%Y-%m-%d %H:%M"),
                chunk_end.strftime("%H:%M"),
                chunk_raw, chunk_events, elapsed,
                total_raw, total_events,
            )
            cursor = chunk_end
    finally:
        raw_sink.close()
        if event_sink is not None:
            event_sink.close()
        adapter.close()

    logger.info(
        "DONE — %d chunks, %d raw logs, %d events",
        total_chunks, total_raw, total_events,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
