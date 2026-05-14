"""Compare BridgeExplorer bridge events against Dune bridge tables.

External reference:
  - bridges_evms.deposits    -> our bridge_out events
  - bridges_evms.withdrawals -> our bridge_in events
  - bridges_evms.flows       -> our bridge_links matches

The script is intentionally aggregate-first to keep Dune credit usage low.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

load_dotenv()

DUNE_API = "https://api.dune.com/api/v1"
CHAINS = ("ethereum", "base", "arbitrum", "optimism", "polygon")

BRIDGE_NAME_MAP = {
    "across": "Across",
    "cctp": "CCTP",
    "layerzero": "LayerZero",
    "op_stack": "OP Stack",
    "base_native": "Base",
    "arbitrum_bridge": "Arbitrum",
}


def _parse_day(value: str) -> datetime:
    return datetime.fromisoformat(f"{value}T00:00:00+00:00")


def _lit(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _rows(result: Any) -> list[dict[str, Any]]:
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


class DuneClient:
    def __init__(self, api_key: str, *, timeout: int = 30, poll_seconds: float = 2.0) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.poll_seconds = poll_seconds

    def execute(self, sql: str, *, performance: str = "small", max_wait_seconds: int = 180) -> dict[str, Any]:
        req = urllib.request.Request(
            f"{DUNE_API}/sql/execute",
            data=json.dumps({"sql": sql, "performance": performance}).encode(),
            headers={"X-Dune-Api-Key": self.api_key, "Content-Type": "application/json"},
        )
        try:
            initial = json.loads(urllib.request.urlopen(req, timeout=self.timeout).read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode()
            raise RuntimeError(f"Dune execute failed: {exc.code} {body}") from exc
        execution_id = initial["execution_id"]
        deadline = time.time() + max_wait_seconds
        while time.time() < deadline:
            req = urllib.request.Request(
                f"{DUNE_API}/execution/{execution_id}/results?limit=10000",
                headers={"X-Dune-Api-Key": self.api_key},
            )
            data = json.loads(urllib.request.urlopen(req, timeout=self.timeout).read())
            if data.get("is_execution_finished"):
                if data.get("state") != "QUERY_STATE_COMPLETED":
                    raise RuntimeError(f"Dune query failed {execution_id}: {data.get('error')}")
                result = data.get("result", {})
                return {
                    "execution_id": execution_id,
                    "metadata": result.get("metadata", {}),
                    "rows": result.get("rows", []),
                }
            time.sleep(self.poll_seconds)
        raise TimeoutError(f"Dune query timed out: {execution_id}")


def _ch_query(client: Any, sql: str) -> list[dict[str, Any]]:
    return _rows(client.query(sql))


def _dune_chain_filter(column: str) -> str:
    chains = ", ".join(f"'{c}'" for c in CHAINS)
    return f"{column} IN ({chains})"


def build_report(ch: Any, dune: DuneClient, day: datetime) -> dict[str, Any]:
    start = day
    end = day + timedelta(days=1)
    date_sql = day.strftime("%Y-%m-%d")
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

    report: dict[str, Any] = {
        "date": date_sql,
        "chains": list(CHAINS),
        "ours": {},
        "dune": {},
        "comparison": {},
    }

    report["ours"]["bridge_out_by_protocol_chain"] = _ch_query(ch, f"""
        SELECT protocol, chain, count() AS rows, uniqExact(tx_hash) AS txs
        FROM nexus.canonical_events
        WHERE {ev_window}
          AND event_type = 'bridge_out'
          AND chain IN {CHAINS}
        GROUP BY protocol, chain
        ORDER BY rows DESC
    """)
    report["ours"]["bridge_in_by_protocol_chain"] = _ch_query(ch, f"""
        SELECT protocol, chain, count() AS rows, uniqExact(tx_hash) AS txs
        FROM nexus.canonical_events
        WHERE {ev_window}
          AND event_type = 'bridge_in'
          AND chain IN {CHAINS}
        GROUP BY protocol, chain
        ORDER BY rows DESC
    """)
    report["ours"]["links_by_protocol_route"] = _ch_query(ch, f"""
        SELECT
          coalesce(bo.protocol, '') AS protocol,
          bl.src_chain AS deposit_chain,
          bl.dst_chain AS withdrawal_chain,
          count() AS rows,
          uniqExact(bl.src_tx_hash) AS deposit_txs,
          uniqExact(bl.dst_tx_hash) AS withdrawal_txs
        FROM nexus.bridge_links bl
        LEFT JOIN nexus.canonical_events bo ON bo.event_id = bl.src_event_id
        WHERE {src_window}
        GROUP BY protocol, deposit_chain, withdrawal_chain
        ORDER BY rows DESC
    """)

    report["dune"]["deposits_by_bridge_chain"] = dune.execute(f"""
        SELECT
          lower(bridge_name) AS bridge_name,
          deposit_chain AS chain,
          count(*) AS rows,
          approx_distinct(tx_hash) AS txs
        FROM bridges_evms.deposits
        WHERE block_date = DATE '{date_sql}'
          AND {_dune_chain_filter('deposit_chain')}
        GROUP BY 1, 2
        ORDER BY rows DESC
    """)
    report["dune"]["withdrawals_by_bridge_chain"] = dune.execute(f"""
        SELECT
          lower(bridge_name) AS bridge_name,
          withdrawal_chain AS chain,
          count(*) AS rows,
          approx_distinct(tx_hash) AS txs
        FROM bridges_evms.withdrawals
        WHERE block_date = DATE '{date_sql}'
          AND {_dune_chain_filter('withdrawal_chain')}
        GROUP BY 1, 2
        ORDER BY rows DESC
    """)
    report["dune"]["flows_by_bridge_route"] = dune.execute(f"""
        SELECT
          lower(bridge_name) AS bridge_name,
          deposit_chain,
          withdrawal_chain,
          count(*) AS rows,
          approx_distinct(deposit_tx_hash) AS deposit_txs,
          approx_distinct(withdrawal_tx_hash) AS withdrawal_txs
        FROM bridges_evms.flows
        WHERE deposit_block_date = DATE '{date_sql}'
          AND {_dune_chain_filter('deposit_chain')}
          AND {_dune_chain_filter('withdrawal_chain')}
        GROUP BY 1, 2, 3
        ORDER BY rows DESC
    """)

    report["comparison"]["mapped_bridge_names"] = BRIDGE_NAME_MAP
    report["comparison"]["notes"] = [
        "Dune bridge_name values are external labels; compare by protocol only after mapping.",
        "Dune deposits/withdrawals cover many bridges we do not decode; unmatched totals are expected until filtered by bridge.",
        "Dune flows are matched bridge transfers, closest to nexus.bridge_links.",
    ]
    return report


def _print_rows(title: str, rows: list[dict[str, Any]], *, limit: int = 25) -> None:
    print(f"\n## {title}")
    rows = rows[:limit]
    if not rows:
        print("(none)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print(" | ".join("-" * widths[c] for c in cols))
    for row in rows:
        print(" | ".join(str(row.get(c, "")).ljust(widths[c]) for c in cols))


def print_report(report: dict[str, Any], *, top: int) -> None:
    print(f"# Dune Bridge Parity {report['date']}")
    for name, rows in report["ours"].items():
        _print_rows(f"ours: {name}", rows, limit=top)
    for name, payload in report["dune"].items():
        meta = payload.get("metadata", {})
        print(f"\nDune {name}: execution={payload.get('execution_id')} rows={meta.get('row_count')} time_ms={meta.get('execution_time_millis')}")
        _print_rows(f"dune: {name}", payload.get("rows", []), limit=top)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="UTC day, YYYY-MM-DD.")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--ch-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8124")))
    parser.add_argument("--ch-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CLICKHOUSE_PASSWORD", "nexus"))
    parser.add_argument("--ch-database", default=os.getenv("CLICKHOUSE_DB", "nexus"))
    parser.add_argument("--dune-api-key", default=os.getenv("DUNE_API_KEY"))
    args = parser.parse_args(argv)

    if not args.dune_api_key:
        parser.error("DUNE_API_KEY is required")

    ch = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
    )
    report = build_report(ch, DuneClient(args.dune_api_key), _parse_day(args.date))
    print_report(report, top=args.top)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, default=str, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
