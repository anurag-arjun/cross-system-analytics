"""Materialize BridgeExplorer rows for a fixed window.

This is the fast path for BridgeExplorer v1: build the expensive
canonical_events + bridge_links union once, classify every row, and serve the
frontend from ``nexus.bridge_explorer_rows``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api import queries
from core.identity.bridge_status import classify as classify_bridge_row

load_dotenv()

logger = logging.getLogger("ops.materialize_bridge_explorer")

COLUMNS = [
    "window_start", "window_end", "row_type", "link_key", "link_key_type", "bridge",
    "src_chain", "src_block_time", "src_tx_hash", "src_entity_id", "src_event_id",
    "dst_chain", "dst_block_time", "dst_tx_hash", "dst_entity_id", "dst_event_id",
    "src_token", "src_amount", "src_amount_usd",
    "dst_token", "dst_amount", "dst_amount_usd",
    "latency_seconds", "dst_chain_id_hint", "src_chain_id_hint",
    "status", "tags", "status_reason", "sort_time", "materialized_at",
]


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _lit(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ch_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def _ensure_table(client: Any) -> None:
    schema = Path(__file__).resolve().parent.parent / "core/schemas/clickhouse/bridge_explorer_rows.sql"
    client.command(schema.read_text())


def _sort_time(row: dict[str, Any], fallback: datetime) -> datetime:
    value = row.get("src_block_time") or row.get("dst_block_time") or fallback
    if isinstance(value, datetime):
        return value
    return fallback


def main(argv: list[str] | None = None) -> int:
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="UTC day, YYYY-MM-DD. Sets start/end to that day.")
    parser.add_argument("--start", help="ISO start timestamp.")
    parser.add_argument("--end", help="ISO end timestamp.")
    parser.add_argument("--limit", type=int, default=1_000_000)
    parser.add_argument("--replace", action="store_true", help="Delete existing cached rows for the window first.")
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

    if args.date:
        start = _parse_ts(f"{args.date}T00:00:00+00:00")
        end = start + timedelta(days=1)
    elif args.start and args.end:
        start = _parse_ts(args.start)
        end = _parse_ts(args.end)
    else:
        logger.error("specify --date or both --start/--end")
        return 2
    if start >= end:
        logger.error("start must be before end")
        return 2

    client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
    )
    _ensure_table(client)

    start_sql = _lit(start)
    end_sql = _lit(end)
    if args.replace:
        client.command(
            f"""
            ALTER TABLE nexus.bridge_explorer_rows
            DELETE WHERE window_start = toDateTime64('{start_sql}', 3)
              AND window_end = toDateTime64('{end_sql}', 3)
            SETTINGS mutations_sync = 1
            """
        )

    sql = queries.bridge_explorer_rows(args.limit, None, None, args.limit, start_sql, end_sql)
    result = client.query(sql)
    raw_rows = [dict(zip(result.column_names, row)) for row in result.result_rows]

    now = datetime.now(timezone.utc)
    materialized_at = now
    insert_rows = []
    for row in raw_rows:
        verdict = classify_bridge_row(row, now=now)
        insert_rows.append((
            _ch_dt(start),
            _ch_dt(end),
            row.get("row_type") or "",
            row.get("link_key") or "",
            row.get("link_key_type") or "",
            row.get("bridge") or "",
            row.get("src_chain"),
            _ch_dt(row.get("src_block_time")),
            row.get("src_tx_hash"),
            row.get("src_entity_id"),
            row.get("src_event_id") or "",
            row.get("dst_chain"),
            _ch_dt(row.get("dst_block_time")),
            row.get("dst_tx_hash"),
            row.get("dst_entity_id"),
            row.get("dst_event_id") or "",
            row.get("src_token"),
            row.get("src_amount"),
            row.get("src_amount_usd"),
            row.get("dst_token"),
            row.get("dst_amount"),
            row.get("dst_amount_usd"),
            row.get("latency_seconds"),
            row.get("dst_chain_id_hint") or "",
            row.get("src_chain_id_hint") or "",
            verdict["status"],
            verdict["tags"],
            verdict["reason"],
            _ch_dt(_sort_time(row, start)),
            _ch_dt(materialized_at),
        ))

    if insert_rows:
        client.insert("bridge_explorer_rows", insert_rows, column_names=COLUMNS)

    logger.info(
        "materialized window=%s..%s rows=%d",
        start_sql,
        end_sql,
        len(insert_rows),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
