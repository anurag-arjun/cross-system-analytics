#!/usr/bin/env python3
"""Data loader for sample user trajectories."""

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

    # Find interesting wallets: those with swaps on multiple chains or multiple event types
    interesting_wallets = client.query("""
        SELECT entity_id, count() as events, count(DISTINCT chain) as chains
        FROM canonical_events
        WHERE event_type IN ('swap', 'bridge_out', 'bridge_in', 'transfer_out')
        GROUP BY entity_id
        HAVING events >= 3 AND events <= 20
        ORDER BY chains DESC, events DESC
        LIMIT 10
    """).result_rows

    trajectories = []
    for wallet_row in interesting_wallets:
        wallet = wallet_row[0]
        events = client.query(f"""
            SELECT 
                event_type,
                protocol,
                chain,
                timestamp,
                tx_hash
            FROM canonical_events
            WHERE entity_id = '{wallet}'
            ORDER BY timestamp ASC
            LIMIT 20
        """).result_rows

        trajectories.append({
            "wallet": wallet[:10] + "..." + wallet[-4:],
            "wallet_full": wallet,
            "event_count": wallet_row[1],
            "chain_count": wallet_row[2],
            "events": [
                {
                    "event_type": e[0],
                    "protocol": e[1],
                    "chain": e[2],
                    "timestamp": str(e[3]),
                    "tx_hash": e[4][:16] + "..." if e[4] else None,
                }
                for e in events
            ]
        })

    json.dump(trajectories, sys.stdout)


if __name__ == "__main__":
    main()
