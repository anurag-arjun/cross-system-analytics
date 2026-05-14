"""Audit BridgeExplorer correctness for a fixed window.

The goal is to make data-quality work evidence-led. This script compares the
cached explorer rows against canonical_events and bridge_links for one window,
then highlights the largest unmatched buckets to investigate first.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv()


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _lit(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _rows(result: Any) -> list[dict[str, Any]]:
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def _query(client: Any, sql: str) -> list[dict[str, Any]]:
    return _rows(client.query(sql))


def _print_table(title: str, rows: list[dict[str, Any]], limit: int | None = None) -> None:
    print(f"\n## {title}")
    rows = rows[:limit] if limit else rows
    if not rows:
        print("(none)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print(" | ".join("-" * widths[c] for c in cols))
    for row in rows:
        print(" | ".join(str(row.get(c, "")).ljust(widths[c]) for c in cols))


def _scalar(rows: list[dict[str, Any]], key: str, default: Any = 0) -> Any:
    return rows[0].get(key, default) if rows else default


def build_report(client: Any, start: datetime, end: datetime, top: int) -> dict[str, Any]:
    start_sql = _lit(start)
    end_sql = _lit(end)
    ev_window = (
        f"timestamp >= toDateTime64('{start_sql}', 3) "
        f"AND timestamp < toDateTime64('{end_sql}', 3)"
    )
    src_window = (
        f"src_block_time >= toDateTime64('{start_sql}', 3) "
        f"AND src_block_time < toDateTime64('{end_sql}', 3)"
    )
    dst_window = (
        f"dst_block_time >= toDateTime64('{start_sql}', 3) "
        f"AND dst_block_time < toDateTime64('{end_sql}', 3)"
    )
    cache_window = (
        f"window_start = toDateTime64('{start_sql}', 3) "
        f"AND window_end = toDateTime64('{end_sql}', 3)"
    )

    report: dict[str, Any] = {
        "window": {"start": start_sql, "end": end_sql},
        "source_counts": {},
        "cache_counts": {},
        "duplicates": {},
        "consistency": {},
        "anomalies": {},
        "unmatched": {},
    }

    report["source_counts"]["canonical_events_by_type"] = _query(client, f"""
        SELECT event_type, count() AS rows, uniqExact(event_id) AS uniq_event_ids
        FROM nexus.canonical_events
        WHERE {ev_window}
          AND event_type IN ('bridge_out', 'bridge_in')
        GROUP BY event_type
        ORDER BY event_type
    """)
    report["source_counts"]["canonical_events_by_chain_protocol"] = _query(client, f"""
        SELECT chain, protocol, event_type, count() AS rows, uniqExact(event_id) AS uniq_event_ids
        FROM nexus.canonical_events
        WHERE {ev_window}
          AND event_type IN ('bridge_out', 'bridge_in')
        GROUP BY chain, protocol, event_type
        ORDER BY rows DESC
        LIMIT {top}
    """)
    report["source_counts"]["link_keys_by_type"] = _query(client, f"""
        SELECT
          event_type,
          coalesce(link_key_type, '') AS link_key_type,
          count() AS rows,
          countIf(link_key IS NULL OR link_key = '') AS empty_link_key,
          uniqExact(link_key) AS uniq_link_keys
        FROM nexus.canonical_events
        WHERE {ev_window}
          AND event_type IN ('bridge_out', 'bridge_in')
        GROUP BY event_type, link_key_type
        ORDER BY rows DESC
        LIMIT {top}
    """)
    report["source_counts"]["bridge_links_by_route"] = _query(client, f"""
        SELECT src_chain, dst_chain, link_key_type, count() AS links, uniqExact(src_event_id) AS uniq_src, uniqExact(dst_event_id) AS uniq_dst
        FROM nexus.bridge_links
        WHERE {src_window}
        GROUP BY src_chain, dst_chain, link_key_type
        ORDER BY links DESC
        LIMIT {top}
    """)

    report["cache_counts"]["by_status"] = _query(client, f"""
        SELECT status, count() AS rows
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
        GROUP BY status
        ORDER BY rows DESC
    """)
    report["cache_counts"]["by_row_type"] = _query(client, f"""
        SELECT row_type, count() AS rows
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
        GROUP BY row_type
        ORDER BY rows DESC
    """)
    report["cache_counts"]["by_bridge_status"] = _query(client, f"""
        SELECT bridge, status, count() AS rows
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
        GROUP BY bridge, status
        ORDER BY rows DESC
        LIMIT {top}
    """)

    report["duplicates"]["canonical_events"] = _query(client, f"""
        SELECT event_type, count() AS rows, uniqExact(event_id) AS uniq_event_ids, rows - uniq_event_ids AS duplicate_rows
        FROM nexus.canonical_events
        WHERE {ev_window}
          AND event_type IN ('bridge_out', 'bridge_in')
        GROUP BY event_type
        ORDER BY event_type
    """)
    report["duplicates"]["bridge_links_src"] = _query(client, f"""
        SELECT count() AS rows, uniqExact(src_event_id) AS uniq_src_event_ids, rows - uniq_src_event_ids AS duplicate_src_rows
        FROM nexus.bridge_links
        WHERE {src_window}
    """)
    report["duplicates"]["bridge_links_dst"] = _query(client, f"""
        SELECT count() AS rows, uniqExact(dst_event_id) AS uniq_dst_event_ids, rows - uniq_dst_event_ids AS duplicate_dst_rows
        FROM nexus.bridge_links
        WHERE {src_window}
    """)
    report["duplicates"]["cache_keys"] = _query(client, f"""
        SELECT
          count() AS rows,
          uniqExact(row_type, src_event_id, dst_event_id, link_key, link_key_type) AS uniq_cache_keys,
          rows - uniq_cache_keys AS duplicate_cache_rows
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
    """)

    src_outs = _scalar(_query(client, f"""
        SELECT count() AS n FROM nexus.canonical_events
        WHERE {ev_window} AND event_type = 'bridge_out'
    """), "n")
    src_ins = _scalar(_query(client, f"""
        SELECT count() AS n FROM nexus.canonical_events
        WHERE {ev_window} AND event_type = 'bridge_in'
    """), "n")
    cache_outs = _scalar(_query(client, f"""
        SELECT count() AS n FROM nexus.bridge_explorer_rows
        WHERE {cache_window} AND row_type IN ('pair', 'orphan_out')
    """), "n")
    cache_ins = _scalar(_query(client, f"""
        SELECT count() AS n FROM nexus.bridge_explorer_rows
        WHERE {cache_window} AND row_type IN ('pair', 'orphan_in')
    """), "n")
    links = _scalar(_query(client, f"""
        SELECT count() AS n FROM nexus.bridge_links
        WHERE {src_window}
    """), "n")
    pairs = _scalar(_query(client, f"""
        SELECT count() AS n FROM nexus.bridge_explorer_rows
        WHERE {cache_window} AND row_type = 'pair'
    """), "n")
    report["consistency"]["source_vs_cache"] = [{
        "canonical_bridge_out": src_outs,
        "cache_src_side_rows": cache_outs,
        "src_delta": src_outs - cache_outs,
        "canonical_bridge_in": src_ins,
        "cache_dst_side_rows": cache_ins,
        "dst_delta": src_ins - cache_ins,
        "bridge_links_src_window": links,
        "cache_pairs": pairs,
        "pair_delta": links - pairs,
    }]

    report["anomalies"]["matched_latency"] = _query(client, f"""
        SELECT
          count() AS matched,
          countIf(latency_seconds < 0) AS negative_latency,
          countIf(latency_seconds > 3600) AS gt_1h,
          countIf(latency_seconds > 86400) AS gt_24h,
          quantile(0.5)(latency_seconds) AS p50_latency,
          quantile(0.95)(latency_seconds) AS p95_latency
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
          AND row_type = 'pair'
    """)
    report["anomalies"]["matched_tokens_amounts"] = _query(client, f"""
        SELECT
          count() AS matched,
          countIf(src_amount_usd IS NULL AND dst_amount_usd IS NULL) AS no_usd_value,
          countIf(src_token != '' AND dst_token != '' AND src_chain = dst_chain AND src_token != dst_token) AS same_chain_token_changed,
          countIf(src_amount != '' AND dst_amount != '' AND src_amount != dst_amount) AS raw_amount_differs
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
          AND row_type = 'pair'
    """)

    report["unmatched"]["unknown_by_bridge_row_type"] = _query(client, f"""
        SELECT bridge, row_type, count() AS rows
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
          AND status = 'UNMATCHED_UNKNOWN'
        GROUP BY bridge, row_type
        ORDER BY rows DESC
        LIMIT {top}
    """)
    report["unmatched"]["unknown_by_chain_link_key"] = _query(client, f"""
        SELECT
          bridge,
          row_type,
          coalesce(src_chain, dst_chain, '') AS chain,
          link_key_type,
          count() AS rows,
          countIf(link_key = '') AS empty_link_key,
          uniqExact(link_key) AS uniq_link_keys
        FROM nexus.bridge_explorer_rows
        WHERE {cache_window}
          AND status = 'UNMATCHED_UNKNOWN'
        GROUP BY bridge, row_type, chain, link_key_type
        ORDER BY rows DESC
        LIMIT {top}
    """)
    report["unmatched"]["unknown_opposite_side_exists_nearby"] = _query(client, f"""
        WITH unknown_outs AS (
          SELECT bridge, link_key_type, link_key
          FROM nexus.bridge_explorer_rows
          WHERE {cache_window}
            AND status = 'UNMATCHED_UNKNOWN'
            AND link_key != ''
            AND row_type = 'orphan_out'
        ),
        unknown_ins AS (
          SELECT bridge, link_key_type, link_key
          FROM nexus.bridge_explorer_rows
          WHERE {cache_window}
            AND status = 'UNMATCHED_UNKNOWN'
            AND link_key != ''
            AND row_type = 'orphan_in'
        ),
        out_probe AS (
          SELECT
            u.bridge AS bridge,
            'orphan_out' AS row_type,
            u.link_key_type AS link_key_type,
            count() AS unknowns,
            countIf(e.event_id != '') AS opposite_side_events_7d,
            uniqExact(u.link_key) AS uniq_unknown_link_keys
          FROM unknown_outs u
          LEFT JOIN nexus.canonical_events e
            ON e.link_key = u.link_key
           AND e.link_key_type = u.link_key_type
           AND e.event_type = 'bridge_in'
           AND e.timestamp >= toDateTime64('{start_sql}', 3) - INTERVAL 7 DAY
           AND e.timestamp < toDateTime64('{end_sql}', 3) + INTERVAL 7 DAY
          GROUP BY u.bridge, u.link_key_type
        ),
        in_probe AS (
          SELECT
            u.bridge AS bridge,
            'orphan_in' AS row_type,
            u.link_key_type AS link_key_type,
            count() AS unknowns,
            countIf(e.event_id != '') AS opposite_side_events_7d,
            uniqExact(u.link_key) AS uniq_unknown_link_keys
          FROM unknown_ins u
          LEFT JOIN nexus.canonical_events e
            ON e.link_key = u.link_key
           AND e.link_key_type = u.link_key_type
           AND e.event_type = 'bridge_out'
           AND e.timestamp >= toDateTime64('{start_sql}', 3) - INTERVAL 7 DAY
           AND e.timestamp < toDateTime64('{end_sql}', 3) + INTERVAL 7 DAY
          GROUP BY u.bridge, u.link_key_type
        )
        SELECT
          bridge, row_type, link_key_type, unknowns, opposite_side_events_7d, uniq_unknown_link_keys
        FROM out_probe
        UNION ALL
        SELECT
          bridge, row_type, link_key_type, unknowns, opposite_side_events_7d, uniq_unknown_link_keys
        FROM in_probe
        ORDER BY unknowns DESC
        LIMIT {top}
    """)

    return report


def print_report(report: dict[str, Any], top: int) -> None:
    window = report["window"]
    print(f"# BridgeExplorer Audit {window['start']} -> {window['end']}")
    for section, tables in report.items():
        if section == "window":
            continue
        for name, rows in tables.items():
            _print_table(f"{section}: {name}", rows, top)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="UTC day, YYYY-MM-DD. Sets start/end to that day.")
    parser.add_argument("--start", help="ISO start timestamp.")
    parser.add_argument("--end", help="ISO end timestamp.")
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--ch-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8124")))
    parser.add_argument("--ch-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CLICKHOUSE_PASSWORD", "nexus"))
    parser.add_argument("--ch-database", default=os.getenv("CLICKHOUSE_DB", "nexus"))
    args = parser.parse_args(argv)

    if args.date:
        start = _parse_ts(f"{args.date}T00:00:00+00:00")
        end = start + timedelta(days=1)
    elif args.start and args.end:
        start = _parse_ts(args.start)
        end = _parse_ts(args.end)
    else:
        parser.error("specify --date or both --start/--end")
    if start >= end:
        parser.error("start must be before end")

    client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
    )
    report = build_report(client, start, end, args.top)
    print_report(report, args.top)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, default=str, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
