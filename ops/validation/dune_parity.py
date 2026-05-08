"""Compare canonical_events vs Dune dex.trades and emit a per-(chain, protocol) diff.

For each (chain, protocol) we collect:
- our_count   — rows in canonical_events.event_type='swap'
- dune_count  — rows in dex.trades
- ratio       — our_count / dune_count (or null on division-by-zero)
- status      — match | gap | extra | low

Statuses:
- match: both sides have rows; ratio in [0.5, 1.5].
- low:   both sides have rows but ratio < 0.5  (we're missing a lot).
- gap:   Dune has rows, we don't (decoder missing).
- extra: We have rows, Dune doesn't (likely false positive — non-DEX swap or fork misclassified).

The check is a sanity rail, not a correctness proof. Spellbook applies its own
filters (e.g. dex.trades excludes wrapped-token mints, fee transfers, certain
wash patterns) so 1.0 parity is unrealistic. The point is to catch
*regressions* — a (chain, protocol) that was matching last week dropping into
gap/low is a signal worth investigating.

Output: ops/validation/runs/{YYYY-MM-DD}.json. Re-runs on the same date
overwrite. The previous date's file is the baseline for regression warnings.

Cost: one Dune query (group-by aggregation, tiny export). Free-tier safe —
expect ~5-10 credits per run.

Usage:
    PYTHONPATH=. python ops/validation/dune_parity.py [--days N] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import clickhouse_connect
from dotenv import load_dotenv

from core.registry.dune import DuneClient

load_dotenv()

logger = logging.getLogger("ops.validation.dune_parity")

DEFAULT_CHAINS: tuple[str, ...] = (
    "ethereum",
    "base",
    "arbitrum",
    "optimism",
    "polygon",
)


@dataclass
class ParityRow:
    chain: str
    protocol: str
    our_count: int
    dune_count: int
    ratio: float | None
    status: str  # match | low | gap | extra
    delta_vs_baseline: float | None = None  # ratio change since last run


def _slug(project: str | None, version: str | None) -> str:
    """Derive the canonical_events.protocol slug from Dune's (project, version)."""
    if not project:
        return ""
    if not version:
        return project
    return f"{project}_v{version}"


def _classify(our: int, dune: int) -> tuple[str, float | None]:
    if our == 0 and dune == 0:
        return "match", 1.0
    if our == 0:
        return "gap", 0.0
    if dune == 0:
        return "extra", None
    ratio = our / dune
    if 0.5 <= ratio <= 1.5:
        return "match", ratio
    return "low", ratio


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------


def fetch_clickhouse_counts(
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    chains: tuple[str, ...],
    start: datetime,
    end: datetime,
) -> dict[tuple[str, str], int]:
    """Group canonical_events swaps by (chain, protocol). Returns a count dict."""
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
    try:
        sql = """
            SELECT chain, protocol, count() AS n
            FROM canonical_events
            WHERE event_type = 'swap'
              AND chain IN %(chains)s
              AND block_time BETWEEN %(start)s AND %(end)s
            GROUP BY chain, protocol
        """
        result = client.query(
            sql,
            parameters={"chains": list(chains), "start": start, "end": end},
        )
        return {(row[0], row[1]): int(row[2]) for row in result.result_rows}
    finally:
        client.close()


def fetch_dune_counts(
    *,
    chains: tuple[str, ...],
    start: datetime,
    end: datetime,
) -> tuple[dict[tuple[str, str], int], int]:
    """Return ((chain, slug) -> count, bytes_scanned)."""
    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        raise RuntimeError("DUNE_API_KEY not set")
    chains_sql = ", ".join(f"'{c}'" for c in chains)
    sql = f"""
        SELECT
            blockchain,
            project,
            version,
            COUNT(*) AS n
        FROM dex.trades
        WHERE blockchain IN ({chains_sql})
          AND block_time BETWEEN
              TIMESTAMP '{start.strftime('%Y-%m-%d %H:%M:%S')}' AND
              TIMESTAMP '{end.strftime('%Y-%m-%d %H:%M:%S')}'
        GROUP BY 1, 2, 3
    """
    client = DuneClient(api_key=api_key)
    try:
        result = client.execute_sql(sql, performance="small")
    finally:
        client.close()

    counts: dict[tuple[str, str], int] = {}
    for row in result.rows:
        chain = row.get("blockchain", "")
        project = row.get("project", "")
        version = row.get("version", "") or ""
        slug = _slug(project, version)
        if not slug:
            continue
        counts[(chain, slug)] = int(row.get("n", 0))
    return counts, int(result.total_result_set_bytes or 0)


# ---------------------------------------------------------------------------
# Diff + IO
# ---------------------------------------------------------------------------


def diff(
    our: dict[tuple[str, str], int],
    dune: dict[tuple[str, str], int],
) -> list[ParityRow]:
    keys = set(our) | set(dune)
    out: list[ParityRow] = []
    for chain, protocol in sorted(keys):
        our_n = our.get((chain, protocol), 0)
        dune_n = dune.get((chain, protocol), 0)
        status, ratio = _classify(our_n, dune_n)
        out.append(
            ParityRow(
                chain=chain,
                protocol=protocol,
                our_count=our_n,
                dune_count=dune_n,
                ratio=ratio,
                status=status,
            )
        )
    return out


def load_baseline(runs_dir: Path, before: datetime) -> dict[tuple[str, str], float | None]:
    """Most-recent prior run's ratios, keyed by (chain, protocol)."""
    if not runs_dir.is_dir():
        return {}
    candidates = sorted(
        p for p in runs_dir.glob("*.json") if p.stem < before.strftime("%Y-%m-%d")
    )
    if not candidates:
        return {}
    payload = json.loads(candidates[-1].read_text())
    return {(r["chain"], r["protocol"]): r.get("ratio") for r in payload.get("rows", [])}


def annotate_with_baseline(
    rows: list[ParityRow], baseline: dict[tuple[str, str], float | None]
) -> None:
    for r in rows:
        prev = baseline.get((r.chain, r.protocol))
        if prev is not None and r.ratio is not None:
            r.delta_vs_baseline = r.ratio - prev


def warn_regressions(rows: list[ParityRow]) -> int:
    """Emit WARN logs for each (chain, protocol) that fell below 0.8 from a higher prior.
    Returns the number of regressions."""
    n = 0
    for r in rows:
        if (
            r.delta_vs_baseline is not None
            and r.ratio is not None
            and r.ratio < 0.8
            and (r.ratio - r.delta_vs_baseline) >= 0.8  # was matching previously
        ):
            logger.warning(
                "[regression] %s/%s parity dropped: prev=%.2f, now=%.2f (Δ %.2f); "
                "our=%d dune=%d",
                r.chain, r.protocol,
                r.ratio - r.delta_vs_baseline, r.ratio, r.delta_vs_baseline,
                r.our_count, r.dune_count,
            )
            n += 1
    return n


def summarise(rows: list[ParityRow]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in rows:
        counts[r.status] = counts.get(r.status, 0) + 1
    return counts


def write_run(
    rows: list[ParityRow],
    *,
    runs_dir: Path,
    window_start: datetime,
    window_end: datetime,
    chains: tuple[str, ...],
    dune_bytes_scanned: int,
) -> Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    path = runs_dir / f"{window_end.strftime('%Y-%m-%d')}.json"
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "chains": list(chains),
        "dune_bytes_scanned": dune_bytes_scanned,
        "summary": summarise(rows),
        "rows": [asdict(r) for r in rows],
    }
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=1, help="Window size (default 1d).")
    parser.add_argument(
        "--chains", nargs="+", default=list(DEFAULT_CHAINS),
        help="Chain slugs to validate.",
    )
    parser.add_argument(
        "--end", default=None,
        help="End timestamp (UTC, ISO-8601). Defaults to now.",
    )
    parser.add_argument(
        "--runs-dir",
        default=str(Path(__file__).resolve().parent / "runs"),
        help="Where to drop the per-day JSON output.",
    )
    parser.add_argument("--ch-host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.environ.get("CLICKHOUSE_PORT", "8124")))
    parser.add_argument("--ch-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument(
        "--ch-password", default=os.environ.get("CLICKHOUSE_PASSWORD", "nexus"),
    )
    parser.add_argument("--ch-database", default=os.environ.get("CLICKHOUSE_DB", "nexus"))
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Hit ClickHouse + Dune but do not write the run file.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    end = (
        datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        if args.end
        else datetime.now(timezone.utc)
    )
    start = end - timedelta(days=args.days)
    chains = tuple(args.chains)

    logger.info("window=%s..%s chains=%s", start.isoformat(), end.isoformat(), chains)

    our = fetch_clickhouse_counts(
        host=args.ch_host, port=args.ch_port, username=args.ch_user,
        password=args.ch_password, database=args.ch_database,
        chains=chains, start=start, end=end,
    )
    logger.info("clickhouse: %d (chain, protocol) groups", len(our))

    dune, dune_bytes = fetch_dune_counts(chains=chains, start=start, end=end)
    logger.info(
        "dune: %d (chain, protocol) groups, %.1f MB scanned",
        len(dune),
        dune_bytes / 1_000_000,
    )

    rows = diff(our, dune)
    runs_dir = Path(args.runs_dir)
    baseline = load_baseline(runs_dir, before=end)
    annotate_with_baseline(rows, baseline)

    n_regress = warn_regressions(rows)
    summary = summarise(rows)
    logger.info("summary: %s | regressions=%d", summary, n_regress)

    if args.dry_run:
        logger.info("[dry-run] not writing run file")
    else:
        path = write_run(
            rows, runs_dir=runs_dir,
            window_start=start, window_end=end,
            chains=chains, dune_bytes_scanned=dune_bytes,
        )
        logger.info("wrote %s", path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
