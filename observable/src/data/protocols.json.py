#!/usr/bin/env python3
"""Protocol analytics data loader."""

import json
import sys

import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse", port=8123,
    username="default", password="nexus", database="nexus",
)

# Top protocols by swap volume (with USD if available)
top_protocols = client.query("""
    SELECT
        protocol, chain,
        count() as swaps,
        uniqExact(entity_id) as traders,
        sum(amount_out_usd) as volume_usd,
        countIf(amount_out_usd IS NOT NULL) as swaps_with_usd
    FROM canonical_events
    WHERE event_type = 'swap' AND protocol != ''
    GROUP BY protocol, chain
    ORDER BY swaps DESC
    LIMIT 20
""").result_rows

# Top venues (contract addresses) by activity
top_venues = client.query("""
    SELECT
        protocol, venue, chain,
        count() as events,
        uniqExact(entity_id) as wallets
    FROM canonical_events
    WHERE event_type = 'swap' AND venue != '' AND protocol != ''
    GROUP BY protocol, venue, chain
    ORDER BY events DESC
    LIMIT 25
""").result_rows

# Top tokens by swap volume
top_tokens = client.query("""
    SELECT token_out, chain, count() as swaps,
           sum(amount_out_usd) as volume_usd,
           uniqExact(entity_id) as traders
    FROM canonical_events
    WHERE event_type = 'swap' AND token_out != '' AND token_out IS NOT NULL
    GROUP BY token_out, chain
    ORDER BY swaps DESC
    LIMIT 20
""").result_rows

# Token metadata for labels
token_labels = client.query("""
    SELECT token_address, symbol, chain
    FROM token_metadata
""").result_rows

token_map = {r[0].lower(): r[1] for r in token_labels}

# Aggregator vs direct DEX breakdown
agg_stats = client.query("""
    SELECT
        countIf(aggregator != '') as agg_swaps,
        countIf(aggregator = '') as direct_swaps
    FROM canonical_events
    WHERE event_type = 'swap'
""").result_rows[0]

# Whale detection — top wallets by swap count
top_traders = client.query("""
    SELECT entity_id, count() as swaps,
           count(DISTINCT protocol) as protocols,
           count(DISTINCT chain) as chains,
           sum(amount_out_usd) as total_volume_usd
    FROM canonical_events
    WHERE event_type = 'swap'
    GROUP BY entity_id
    ORDER BY swaps DESC
    LIMIT 25
""").result_rows

output = {
    "top_protocols": [
        {
            "protocol": r[0], "chain": r[1],
            "swaps": r[2], "traders": r[3],
            "volume_usd": float(r[4]) if r[4] else 0,
            "swaps_with_usd": r[5],
        }
        for r in top_protocols
    ],
    "top_venues": [
        {
            "protocol": r[0],
            "venue": r[1][:10] + "..." if len(r[1]) > 12 else r[1],
            "venue_full": r[1],
            "chain": r[2],
            "events": r[3],
            "wallets": r[4],
        }
        for r in top_venues
    ],
    "top_tokens": [
        {
            "token": r[0],
            "symbol": token_map.get(r[0].lower(), r[0][:10] + "..."),
            "chain": r[1],
            "swaps": r[2],
            "volume_usd": float(r[3]) if r[3] else 0,
            "traders": r[4],
        }
        for r in top_tokens
    ],
    "aggregator_stats": {
        "agg_swaps": agg_stats[0] if agg_stats else 0,
        "direct_swaps": agg_stats[1] if agg_stats else 0,
    },
    "top_traders": [
        {
            "wallet": r[0][:10] + "...",
            "wallet_full": r[0],
            "swaps": r[1],
            "protocols": r[2],
            "chains": r[3],
            "total_volume_usd": float(r[4]) if r[4] else 0,
        }
        for r in top_traders
    ],
    "total_protocols": len(top_protocols),
    "total_venues": len(top_venues),
}

json.dump(output, sys.stdout)
