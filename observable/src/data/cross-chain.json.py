#!/usr/bin/env python3
"""Cross-chain analytics data loader."""

import json
import sys

import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse", port=8123,
    username="default", password="nexus", database="nexus",
)

# Cross-chain wallets by chain pair
chain_pairs = client.query("""
    WITH wallet_chains AS (
        SELECT entity_id, groupUniqArray(chain) as chains
        FROM canonical_events
        GROUP BY entity_id
        HAVING length(chains) > 1
    )
    SELECT chains, count() as wallets
    FROM wallet_chains
    GROUP BY chains ORDER BY wallets DESC
    LIMIT 20
""").result_rows

# Top cross-chain wallets (most events across multiple chains)
top_xc = client.query("""
    SELECT entity_id, count(DISTINCT chain) as chains,
           count() as events, min(timestamp) as first_seen,
           max(timestamp) as last_seen
    FROM canonical_events
    GROUP BY entity_id
    HAVING chains > 1
    ORDER BY events DESC
    LIMIT 30
""").result_rows

# Bridge events timeline
bridges = client.query("""
    SELECT
        event_type, protocol, chain, entity_id,
        timestamp, tx_hash, token_in, token_out,
        amount_in, amount_out, amount_in_usd, amount_out_usd
    FROM canonical_events
    WHERE event_type IN ('bridge_out', 'bridge_in')
    ORDER BY timestamp DESC
    LIMIT 50
""").result_rows

# Bridge links (matched pairs)
bridge_links = client.query("""
    SELECT
        link_key, link_key_type,
        src_chain, dst_chain,
        src_block_time, dst_block_time,
        src_entity_id, dst_entity_id,
        amount, amount_usd, link_confidence
    FROM bridge_links
    ORDER BY src_block_time DESC
    LIMIT 50
""").result_rows

# Chain pair activity matrix
chain_matrix = client.query("""
    WITH per_wallet AS (
        SELECT entity_id, groupUniqArray(chain) as chains
        FROM canonical_events
        GROUP BY entity_id
    )
    SELECT chains, count() as wallets
    FROM per_wallet
    GROUP BY chains
    HAVING length(chains) >= 1
    ORDER BY wallets DESC
""").result_rows

output = {
    "chain_pairs": [
        {"chains": sorted(r[0]), "wallets": r[1]} for r in chain_pairs
    ],
    "top_cross_chain": [
        {
            "wallet": r[0][:10] + "...",
            "wallet_full": r[0],
            "chains": r[1],
            "events": r[2],
            "first_seen": str(r[3]),
            "last_seen": str(r[4]),
        }
        for r in top_xc
    ],
    "bridges": [
        {
            "event_type": r[0], "protocol": r[1], "chain": r[2],
            "entity_id": r[3][:10] + "..." if r[3] else "",
            "timestamp": str(r[4]), "tx_hash": r[5][:10] + "..." if r[5] else "",
            "token_in": r[6], "token_out": r[7],
            "amount_in": str(r[8]) if r[8] else None,
            "amount_out": str(r[9]) if r[9] else None,
            "amount_in_usd": float(r[10]) if r[10] else None,
            "amount_out_usd": float(r[11]) if r[11] else None,
        }
        for r in bridges
    ],
    "bridge_links": [
        {
            "link_key": r[0], "link_key_type": r[1],
            "src_chain": r[2], "dst_chain": r[3],
            "src_time": str(r[4]), "dst_time": str(r[5]),
            "src_wallet": r[6][:10] + "..." if r[6] else "",
            "dst_wallet": r[7][:10] + "..." if r[7] else "",
            "amount": r[8], "amount_usd": float(r[9]) if r[9] else None,
            "confidence": float(r[10]) if r[10] else 0,
        }
        for r in bridge_links
    ],
    "chain_matrix": [
        {"chains": sorted(r[0]), "wallets": r[1]}
        for r in chain_matrix
    ],
    "total_cross_chain": sum(r[1] for r in chain_pairs),
}

json.dump(output, sys.stdout)
