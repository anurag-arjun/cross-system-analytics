#!/usr/bin/env python3
"""Ingest real mainnet data for demo purposes.

Usage:
    python -m core.demo.ingest_realtime --chain base --minutes 30
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from core.adapters.evm import EVMAdapter
from core.sink.clickhouse import ClickHouseSink, SinkConfig


def main():
    parser = argparse.ArgumentParser(description="Ingest real EVM data for demo")
    parser.add_argument("--chain", default="base", help="Chain to ingest (base, arbitrum, optimism)")
    parser.add_argument("--minutes", type=int, default=30, help="Minutes of data to ingest")
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=8124, help="ClickHouse port")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=args.minutes)
    end = now

    print(f"[*] Ingesting {args.minutes} minutes of {args.chain} data")
    print(f"    Time range: {start.isoformat()} -> {end.isoformat()}")

    adapter = EVMAdapter(chain=args.chain)
    sink_config = SinkConfig(host=args.host, port=args.port, batch_size=1000)
    sink = ClickHouseSink(config=sink_config)

    event_counts: dict[str, int] = {}
    total = 0

    print(f"[*] Fetching events via {'HyperSync' if adapter.hyper_token else 'JSON-RPC'}...")

    for event in adapter.ingest(start, end):
        sink.write_single(event)
        event_counts[event.event_type] = event_counts.get(event.event_type, 0) + 1
        total += 1
        if total % 100 == 0:
            print(f"    Processed {total} events...", end="\r")

    sink.close()
    adapter.close()

    print(f"\n[+] Ingested {total} events to ClickHouse")
    print("[+] Event breakdown:")
    for event_type, count in sorted(event_counts.items(), key=lambda x: -x[1]):
        print(f"    {event_type}: {count}")


if __name__ == "__main__":
    main()
