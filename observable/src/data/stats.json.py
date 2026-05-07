#!/usr/bin/env python3
"""Data loader for dashboard stats."""

import json
import sys

import clickhouse_connect


def main():
    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="nexus",
        database="nexus",
    )

    # Overall stats
    overall = client.query("""
        SELECT 
            count() as total_events,
            uniqExact(entity_id) as unique_wallets,
            countIf(event_type = 'swap') as total_swaps,
            countIf(event_type IN ('bridge_out', 'bridge_in')) as total_bridges,
            min(timestamp) as start_time,
            max(timestamp) as end_time
        FROM canonical_events
    """).result_rows[0]

    # By chain
    by_chain = client.query("""
        SELECT 
            chain,
            count() as events,
            uniqExact(entity_id) as wallets,
            countIf(event_type = 'swap') as swaps
        FROM canonical_events 
        GROUP BY chain
        ORDER BY events DESC
    """).result_rows

    # By event type
    by_event = client.query("""
        SELECT 
            event_type,
            count() as count
        FROM canonical_events 
        GROUP BY event_type
        ORDER BY count DESC
        LIMIT 10
    """).result_rows

    # By protocol (swaps only)
    by_protocol = client.query("""
        SELECT 
            protocol,
            count() as swaps,
            uniqExact(entity_id) as traders
        FROM canonical_events 
        WHERE event_type = 'swap' AND protocol != ''
        GROUP BY protocol
        ORDER BY swaps DESC
        LIMIT 10
    """).result_rows

    # Cross-chain wallets
    cross_chain = client.query("""
        SELECT count() as cross_chain_wallets
        FROM (
            SELECT entity_id, count(DISTINCT chain) as chains
            FROM canonical_events
            GROUP BY entity_id
            HAVING chains > 1
        )
    """).result_rows[0][0]

    # Hourly activity
    hourly = client.query("""
        SELECT 
            toStartOfHour(timestamp) as hour,
            count() as events,
            countIf(event_type = 'swap') as swaps
        FROM canonical_events 
        GROUP BY hour
        ORDER BY hour
    """).result_rows

    stats = {
        "total_events": overall[0],
        "unique_wallets": overall[1],
        "total_swaps": overall[2],
        "total_bridges": overall[3],
        "start_time": str(overall[4]),
        "end_time": str(overall[5]),
        "cross_chain_wallets": cross_chain,
        "by_chain": [{"chain": r[0], "events": r[1], "wallets": r[2], "swaps": r[3]} for r in by_chain],
        "by_event": [{"event_type": r[0], "count": r[1]} for r in by_event],
        "by_protocol": [{"protocol": r[0], "swaps": r[1], "traders": r[2]} for r in by_protocol],
        "hourly": [{"hour": str(r[0]), "events": r[1], "swaps": r[2]} for r in hourly],
    }

    json.dump(stats, sys.stdout)


if __name__ == "__main__":
    main()
