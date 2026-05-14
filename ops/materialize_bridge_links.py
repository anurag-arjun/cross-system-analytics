"""Materialize bridge_links for a fixed time window."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("ops.materialize_bridge_links")


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _lit(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="UTC day, YYYY-MM-DD. Sets start/end to that day.")
    parser.add_argument("--start", help="ISO start timestamp.")
    parser.add_argument("--end", help="ISO end timestamp.")
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

    client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
    )

    start_sql = _lit(start)
    end_sql = _lit(end)
    sql = f"""
    INSERT INTO nexus.bridge_links (
      link_key, link_key_type,
      src_chain, src_block_time, src_tx_hash, src_entity_id, src_event_id,
      dst_chain, dst_block_time, dst_tx_hash, dst_entity_id, dst_event_id,
      token, amount, amount_usd,
      link_confidence
    )
    SELECT
      bo.link_key,
      coalesce(bo.link_key_type, bi.link_key_type) AS link_key_type,
      bo.chain        AS src_chain,
      bo.timestamp    AS src_block_time,
      bo.tx_hash      AS src_tx_hash,
      bo.entity_id    AS src_entity_id,
      bo.event_id     AS src_event_id,
      bi.chain        AS dst_chain,
      bi.timestamp    AS dst_block_time,
      bi.tx_hash      AS dst_tx_hash,
      bi.entity_id    AS dst_entity_id,
      bi.event_id     AS dst_event_id,
      coalesce(bo.token_out, bi.token_in) AS token,
      coalesce(bo.amount_out, bi.amount_in) AS amount,
      coalesce(bo.amount_out_usd, bi.amount_in_usd) AS amount_usd,
      1.0 AS link_confidence
    FROM (SELECT * FROM nexus.canonical_events FINAL WHERE event_type = 'bridge_out') AS bo
    INNER JOIN (SELECT * FROM nexus.canonical_events FINAL WHERE event_type = 'bridge_in') AS bi
        ON bo.link_key = bi.link_key
       AND bo.link_key_type = bi.link_key_type
    WHERE bo.link_key IS NOT NULL
      AND bo.timestamp >= toDateTime64('{start_sql}', 3)
      AND bo.timestamp <  toDateTime64('{end_sql}', 3)
      AND bi.timestamp >= bo.timestamp
      AND bi.timestamp <= bo.timestamp + INTERVAL 7 DAY
      AND bo.event_id NOT IN (
        SELECT src_event_id FROM nexus.bridge_links FINAL
        WHERE src_block_time >= toDateTime64('{start_sql}', 3)
          AND src_block_time <  toDateTime64('{end_sql}', 3)
      )
    """

    before = client.query("SELECT count() FROM nexus.bridge_links FINAL").result_rows[0][0]
    client.command(sql)
    after = client.query("SELECT count() FROM nexus.bridge_links FINAL").result_rows[0][0]
    logger.info("bridge_links matched=%d total=%d window=%s..%s", after - before, after, start_sql, end_sql)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
