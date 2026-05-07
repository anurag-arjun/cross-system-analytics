#!/usr/bin/env python3
"""Overview stats for Nexus Analytics dashboard."""

import json
import sys
from datetime import datetime

import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse", port=8123,
    username="default", password="nexus", database="nexus",
)

# Overall KPIs
overall = client.query("""
    SELECT
        count() as total_events,
        uniqExact(entity_id) as unique_wallets,
        countIf(event_type = 'swap') as total_swaps,
        countIf(event_type IN ('bridge_out', 'bridge_in')) as total_bridges,
        countIf(event_type = 'swap' AND amount_in_usd IS NOT NULL) as swaps_with_usd,
        min(timestamp) as first_event,
        max(timestamp) as last_event
    FROM canonical_events
""").result_rows[0]

total, wallets, swaps, bridges, swaps_usd, first_ev, last_ev = overall
usd_coverage = round(swaps_usd * 100.0 / max(swaps, 1), 1)

# Chain breakdown
by_chain = client.query("""
    SELECT chain, count() as events, uniqExact(entity_id) as wallets,
           countIf(event_type='swap') as swaps,
           countIf(event_type IN ('bridge_out','bridge_in')) as bridges
    FROM canonical_events
    GROUP BY chain ORDER BY events DESC
""").result_rows

# Cross-chain wallets
cross_chain = client.query("""
    SELECT
        countIf(chains > 1) as cross_chain,
        count() as total,
        round(countIf(chains > 1) * 100.0 / count(), 1) as pct
    FROM (
        SELECT entity_id, count(DISTINCT chain) as chains
        FROM canonical_events
        GROUP BY entity_id
    )
""").result_rows[0]

# Chains per wallet distribution
chain_dist = client.query("""
    SELECT chains, count() as wallets
    FROM (
        SELECT entity_id, count(DISTINCT chain) as chains
        FROM canonical_events
        GROUP BY entity_id
    )
    GROUP BY chains ORDER BY chains
""").result_rows

# Event type breakdown
by_event = client.query("""
    SELECT event_type, count() as cnt
    FROM canonical_events
    GROUP BY event_type ORDER BY cnt DESC
""").result_rows

# Top protocols (swaps only)
by_protocol = client.query("""
    SELECT protocol, count() as swaps, uniqExact(entity_id) as traders
    FROM canonical_events
    WHERE event_type = 'swap' AND protocol != ''
    GROUP BY protocol ORDER BY swaps DESC LIMIT 15
""").result_rows

# Hourly activity (last 48h)
hourly = client.query("""
    SELECT
        toStartOfHour(timestamp) as hour,
        count() as events,
        countIf(event_type='swap') as swaps,
        uniqExact(entity_id) as wallets
    FROM canonical_events
    WHERE timestamp >= now() - INTERVAL 48 HOUR
    GROUP BY hour ORDER BY hour
""").result_rows

# Event category breakdown
by_category = client.query("""
    SELECT event_category, count() as cnt
    FROM canonical_events
    GROUP BY event_category ORDER BY cnt DESC
""").result_rows

output = {
    "kpis": {
        "total_events": total,
        "unique_wallets": wallets,
        "total_swaps": swaps,
        "total_bridges": bridges,
        "swaps_with_usd": swaps_usd,
        "usd_coverage_pct": usd_coverage,
        "cross_chain_wallets": cross_chain[0],
        "cross_chain_pct": cross_chain[2],
        "first_event": str(first_ev),
        "last_event": str(last_ev),
        "chains_active": len(by_chain),
    },
    "by_chain": [
        {"chain": r[0], "events": r[1], "wallets": r[2], "swaps": r[3], "bridges": r[4]}
        for r in by_chain
    ],
    "cross_chain": {
        "total": int(cross_chain[0]),
        "pct": float(cross_chain[2]),
        "distribution": [
            {"chains": int(r[0]), "wallets": int(r[1])} for r in chain_dist
        ],
    },
    "by_event": [
        {"event_type": r[0], "count": r[1]} for r in by_event
    ],
    "by_protocol": [
        {"protocol": r[0], "swaps": r[1], "traders": r[2]} for r in by_protocol
    ],
    "by_category": [
        {"category": r[0], "count": r[1]} for r in by_category
    ],
    "hourly": [
        {"hour": str(r[0]), "events": r[1], "swaps": r[2], "wallets": r[3]}
        for r in hourly
    ],
}

json.dump(output, sys.stdout)
