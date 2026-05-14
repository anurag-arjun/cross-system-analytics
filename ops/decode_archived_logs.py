"""Decode archived raw-log gzip files into canonical_events.

The archive produced by ``ops/archive_raw_logs.py`` is the durable raw-log
landing zone. This script is the second stage: read archived JSONL gzip files,
decode with the current registry, and write canonical_events to ClickHouse.

For BridgeExplorer v1, a useful first pass is bridge events only:

    PYTHONPATH=. python ops/decode_archived_logs.py \
      --date 2026-05-12 \
      --out-dir ~/nexus-raw-logs \
      --event-types bridge_out bridge_in
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv

from core.adapters.evm import EVMAdapter
from core.adapters.evm.registry import build_default_registry
from core.registry.protocol_contracts import PostgresProtocolContractStore, make_cached_resolver
from core.sink import ClickHouseSink, SinkConfig

load_dotenv()

logger = logging.getLogger("ops.decode_archived_logs")

CHAINS = ("base", "ethereum", "arbitrum", "optimism", "polygon")


def _parse_block_time(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("block_time")
    if isinstance(value, str) and value:
        dt = datetime.fromisoformat(value)
        row["block_time"] = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return row


def _iter_archive_rows(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield _parse_block_time(json.loads(line))


def _files_for_day(root: Path, chain: str, date: str) -> list[Path]:
    day_dir = root / chain / date
    if not day_dir.exists():
        return []
    return sorted(day_dir.glob("*.jsonl.gz"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="UTC day to decode, YYYY-MM-DD.")
    parser.add_argument("--out-dir", default="/data/nexus-raw-logs", help="Archive root.")
    parser.add_argument("--chains", nargs="+", default=list(CHAINS))
    parser.add_argument(
        "--event-types",
        nargs="+",
        default=None,
        help="Optional event types to keep after decode, e.g. bridge_out bridge_in.",
    )
    parser.add_argument("--read-batch-size", type=int, default=10_000)
    parser.add_argument("--sink-batch-size", type=int, default=10_000)
    parser.add_argument("--ch-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8124")))
    parser.add_argument("--ch-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CLICKHOUSE_PASSWORD", "nexus"))
    parser.add_argument("--ch-database", default=os.getenv("CLICKHOUSE_DB", "nexus"))
    parser.add_argument(
        "--postgres",
        default=os.environ.get(
            "PROTOCOL_CONTRACTS_DSN",
            "postgresql://nexus:nexus@localhost:5434/nexus_ops",
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    chains = [c for c in args.chains if c in CHAINS]
    unknown = sorted(set(args.chains) - set(chains))
    if unknown:
        logger.error("unknown chains: %s", unknown)
        return 2
    if not chains:
        logger.error("no chains selected")
        return 2

    keep_types = set(args.event_types) if args.event_types else None
    root = Path(args.out_dir).expanduser()
    files_by_chain = {chain: _files_for_day(root, chain, args.date) for chain in chains}
    missing = [chain for chain, files in files_by_chain.items() if not files]
    if missing:
        logger.error("no archive files for chains: %s", missing)
        return 2

    pc_store = PostgresProtocolContractStore(args.postgres)
    resolver = make_cached_resolver(pc_store) if pc_store.count() > 0 else None
    registry = build_default_registry(protocol_resolver=resolver) if resolver is not None else None
    if registry is None:
        logger.warning("protocol_contracts is empty; address-first lookup disabled")

    sink = ClickHouseSink(SinkConfig(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
        table="canonical_events",
        batch_size=args.sink_batch_size,
    ))

    total_raw = 0
    total_decoded = 0
    total_written = 0
    t_all = time.time()

    try:
        for chain in chains:
            adapter = EVMAdapter(chain=chain, registry=registry)
            chain_raw = 0
            chain_decoded = 0
            chain_written = 0
            t_chain = time.time()
            try:
                for path in files_by_chain[chain]:
                    batch: list[dict[str, Any]] = []
                    file_raw = 0
                    file_decoded = 0
                    for row in _iter_archive_rows(path):
                        batch.append(row)
                        file_raw += 1
                        if len(batch) >= args.read_batch_size:
                            events = list(adapter.decode_logs(batch))
                            if keep_types is not None:
                                events = [ev for ev in events if ev.event_type in keep_types]
                            if events:
                                sink.write(events)
                            file_decoded += len(events)
                            batch.clear()
                    if batch:
                        events = list(adapter.decode_logs(batch))
                        if keep_types is not None:
                            events = [ev for ev in events if ev.event_type in keep_types]
                        if events:
                            sink.write(events)
                        file_decoded += len(events)

                    chain_raw += file_raw
                    chain_decoded += file_decoded
                    chain_written += file_decoded
                    total_raw += file_raw
                    total_decoded += file_decoded
                    total_written += file_decoded
                    logger.info(
                        "%s %s raw=%d decoded=%d",
                        chain,
                        path.name,
                        file_raw,
                        file_decoded,
                    )
            finally:
                adapter.close()

            logger.info(
                "%s DONE raw=%d decoded=%d written=%d elapsed=%.1fm",
                chain,
                chain_raw,
                chain_decoded,
                chain_written,
                (time.time() - t_chain) / 60,
            )
    finally:
        sink.close()

    logger.info(
        "DONE date=%s raw=%d decoded=%d written=%d elapsed=%.1fm",
        args.date,
        total_raw,
        total_decoded,
        total_written,
        (time.time() - t_all) / 60,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
