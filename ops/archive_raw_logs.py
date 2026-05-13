"""Archive raw EVM logs to compressed JSONL files.

This is the cheap landing zone for "all logs for later": pull raw logs from
HyperSync and write append-only gzip files, one chain/window per file. Decode
and ClickHouse import can run later from this archive.

Usage:
    PYTHONPATH=. python ops/archive_raw_logs.py --days 14
    PYTHONPATH=. python ops/archive_raw_logs.py --days 1 --chains base polygon
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from core.adapters.evm import EVMAdapter

load_dotenv()

logger = logging.getLogger("ops.archive_raw_logs")

CHAINS = ("base", "ethereum", "arbitrum", "optimism", "polygon")


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class ArchiveResult:
    chain: str
    start: datetime
    end: datetime
    path: Path
    rows: int
    bytes_gzip: int
    elapsed_s: float
    skipped: bool = False


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _window_path(out_dir: Path, chain: str, start: datetime) -> Path:
    return out_dir / chain / start.strftime("%Y-%m-%d") / f"{start.strftime('%H%M')}.jsonl.gz"


def _archive_window(
    *,
    chain: str,
    window: Window,
    out_dir: Path,
    overwrite: bool,
    compresslevel: int,
) -> ArchiveResult:
    path = _window_path(out_dir, chain, window.start)
    if path.exists() and path.stat().st_size > 0 and not overwrite:
        return ArchiveResult(
            chain=chain,
            start=window.start,
            end=window.end,
            path=path,
            rows=0,
            bytes_gzip=path.stat().st_size,
            elapsed_s=0.0,
            skipped=True,
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()

    adapter = EVMAdapter(chain=chain, hyper_token=os.getenv("HYPERSYNC_TOKEN"))
    t0 = time.time()
    rows = 0
    try:
        with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=compresslevel) as f:
            for row in adapter.ingest_raw(window.start, window.end):
                f.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
                rows += 1
    finally:
        adapter.close()

    tmp.replace(path)
    elapsed = time.time() - t0
    return ArchiveResult(
        chain=chain,
        start=window.start,
        end=window.end,
        path=path,
        rows=rows,
        bytes_gzip=path.stat().st_size,
        elapsed_s=elapsed,
    )


def _day_windows(day_start: datetime, day_end: datetime, chunk: timedelta) -> list[Window]:
    windows: list[Window] = []
    cursor = day_start
    while cursor < day_end:
        end = min(cursor + chunk, day_end)
        windows.append(Window(start=cursor, end=end))
        cursor = end
    return windows


def _day_ranges(end: datetime, days: int) -> list[tuple[datetime, datetime]]:
    ranges: list[tuple[datetime, datetime]] = []
    cursor_end = end
    for _ in range(days):
        cursor_start = cursor_end - timedelta(days=1)
        ranges.append((cursor_start, cursor_end))
        cursor_end = cursor_start
    return ranges


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=14, help="Number of days to archive backwards from --end.")
    parser.add_argument("--end", type=str, default=None, help="ISO end timestamp; default now UTC.")
    parser.add_argument("--chunk-minutes", type=int, default=5, help="File/window size in minutes.")
    parser.add_argument("--parallel-chains", type=int, default=2, help="How many chains to fetch concurrently.")
    parser.add_argument("--chains", nargs="+", default=list(CHAINS), help="Chains to archive.")
    parser.add_argument("--out-dir", default="/data/nexus-raw-logs", help="Archive root directory.")
    parser.add_argument("--compresslevel", type=int, default=3, choices=range(1, 10))
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing non-empty files.")
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

    end = _floor_to_minute(_parse_ts(args.end)) if args.end else _floor_to_minute(datetime.now(timezone.utc))
    chunk = timedelta(minutes=args.chunk_minutes)
    out_dir = Path(args.out_dir)

    logger.info(
        "archive start end=%s days=%d chunk=%dm parallel_chains=%d chains=%s out_dir=%s",
        end.isoformat(),
        args.days,
        args.chunk_minutes,
        args.parallel_chains,
        chains,
        out_dir,
    )

    total_rows = 0
    total_bytes = 0
    total_files = 0
    total_skipped = 0
    t_all = time.time()

    for day_index, (day_start, day_end) in enumerate(_day_ranges(end, args.days), start=1):
        day_t0 = time.time()
        day_rows = 0
        day_bytes = 0
        day_files = 0
        day_skipped = 0
        windows = _day_windows(day_start, day_end, chunk)
        logger.info(
            "day %d/%d window=%s..%s chunks=%d",
            day_index,
            args.days,
            day_start.isoformat(),
            day_end.isoformat(),
            len(windows),
        )

        for window in windows:
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel_chains) as pool:
                futures = [
                    pool.submit(
                        _archive_window,
                        chain=chain,
                        window=window,
                        out_dir=out_dir,
                        overwrite=args.overwrite,
                        compresslevel=args.compresslevel,
                    )
                    for chain in chains
                ]
                for fut in concurrent.futures.as_completed(futures):
                    res = fut.result()
                    day_rows += res.rows
                    day_bytes += res.bytes_gzip
                    total_rows += res.rows
                    total_bytes += res.bytes_gzip
                    if res.skipped:
                        day_skipped += 1
                        total_skipped += 1
                    else:
                        day_files += 1
                        total_files += 1
                    logger.info(
                        "%s %s..%s rows=%d gzip=%.2fMB elapsed=%.1fs%s",
                        res.chain,
                        res.start.strftime("%Y-%m-%d %H:%M"),
                        res.end.strftime("%H:%M"),
                        res.rows,
                        res.bytes_gzip / 1024 / 1024,
                        res.elapsed_s,
                        " skipped" if res.skipped else "",
                    )

        logger.info(
            "day %d DONE rows=%d gzip=%.2fGB files=%d skipped=%d elapsed=%.1fm",
            day_index,
            day_rows,
            day_bytes / 1024 / 1024 / 1024,
            day_files,
            day_skipped,
            (time.time() - day_t0) / 60,
        )

    logger.info(
        "DONE rows=%d gzip=%.2fGB files=%d skipped=%d elapsed=%.1fh",
        total_rows,
        total_bytes / 1024 / 1024 / 1024,
        total_files,
        total_skipped,
        (time.time() - t_all) / 3600,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
