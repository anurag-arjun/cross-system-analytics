#!/usr/bin/env python3
"""Data loader for Observable Framework — fetches recent events from ClickHouse."""

import json
import sys

import clickhouse_connect


def main():
    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="nexus",
        database="default",
    )

    result = client.query("""
        SELECT 
            entity_id,
            event_type,
            protocol,
            chain,
            block_number,
            tx_hash,
            timestamp
        FROM canonical_events
        WHERE event_type IN ('swap', 'bridge_out', 'bridge_in')
        ORDER BY timestamp DESC
        LIMIT 200
    """)

    events = []
    for row in result.result_rows:
        events.append(
            {
                "entity_id": row[0][:10] + "..." + row[0][-4:] if len(row[0]) > 14 else row[0],
                "entity_id_full": row[0],
                "event_type": row[1],
                "protocol": row[2],
                "chain": row[3],
                "block_number": row[4],
                "tx_hash": row[5],
                "timestamp": str(row[6]),
            }
        )

    json.dump(events, sys.stdout)


if __name__ == "__main__":
    main()
