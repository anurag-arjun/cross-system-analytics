#!/usr/bin/env python3
"""Bridge flow analytics — post-bridge user behavior."""

import json, sys
import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse", port=8123,
    username="default", password="nexus", database="nexus",
)

# 1. First action after bridge_in (by protocol + event_type)
first_action = client.query("""
    WITH bridge_ins AS (
        SELECT entity_id, chain, timestamp as bridge_ts,
               event_id as bridge_event_id, protocol as bridge_protocol
        FROM canonical_events
        WHERE event_type = 'bridge_in'
    ),
    next_events AS (
        SELECT b.entity_id, b.chain, b.bridge_ts, b.bridge_protocol,
               e.timestamp as next_ts, e.event_type as next_type,
               e.protocol as next_protocol,
               row_number() OVER (
                   PARTITION BY b.entity_id, b.chain, b.bridge_ts
                   ORDER BY e.timestamp
               ) as rn
        FROM bridge_ins b
        JOIN canonical_events e ON b.entity_id = e.entity_id
            AND b.chain = e.chain
            AND e.timestamp > b.bridge_ts
            AND e.timestamp <= b.bridge_ts + INTERVAL 1 HOUR
    )
    SELECT
        bridge_protocol, next_protocol, next_type,
        count() as cnt,
        avg(dateDiff('second', bridge_ts, next_ts)) as avg_sec,
        quantile(0.5)(dateDiff('second', bridge_ts, next_ts)) as median_sec
    FROM next_events
    WHERE rn = 1
    GROUP BY bridge_protocol, next_protocol, next_type
    ORDER BY cnt DESC
    LIMIT 20
""").result_rows

first_actions = [
    {"bridge_protocol": r[0], "next_protocol": r[1], "next_type": r[2],
     "count": r[3], "avg_sec": round(r[4], 1) if r[4] else 0,
     "median_sec": round(r[5], 1) if r[5] else 0}
    for r in first_action
]

# 2. Swap vs non-swap split after bridge
swap_split = client.query("""
    WITH bridge_ins AS (
        SELECT entity_id, chain, timestamp as bridge_ts
        FROM canonical_events WHERE event_type = 'bridge_in'
    ),
    first_acts AS (
        SELECT b.entity_id, b.chain, b.bridge_ts,
               e.event_type, e.protocol,
               row_number() OVER (
                   PARTITION BY b.entity_id, b.chain, b.bridge_ts
                   ORDER BY e.timestamp
               ) as rn
        FROM bridge_ins b
        JOIN canonical_events e ON b.entity_id = e.entity_id
            AND b.chain = e.chain
            AND e.timestamp > b.bridge_ts
            AND e.timestamp <= b.bridge_ts + INTERVAL 1 HOUR
    )
    SELECT
        multiIf(event_type = 'swap', 'swap', 'non_swap') as action_type,
        count() as cnt
    FROM first_acts WHERE rn = 1
    GROUP BY action_type
""").result_rows

swap_vs_non = {r[0]: r[1] for r in swap_split}
total_first = sum(swap_vs_non.values())
swap_pct = round(swap_vs_non.get('swap', 0) * 100.0 / max(total_first, 1), 1)

# 3. 2nd hop — what happens after first swap
second_hop = client.query("""
    WITH bridge_ins AS (
        SELECT entity_id, chain, timestamp as bridge_ts
        FROM canonical_events WHERE event_type = 'bridge_in'
    ),
    first_swap AS (
        SELECT b.entity_id, b.chain, b.bridge_ts, e.timestamp as swap_ts
        FROM bridge_ins b
        JOIN canonical_events e ON b.entity_id = e.entity_id
            AND b.chain = e.chain
            AND e.event_type = 'swap'
            AND e.timestamp > b.bridge_ts
            AND e.timestamp <= b.bridge_ts + INTERVAL 1 HOUR
        QUALIFY row_number() OVER (
            PARTITION BY b.entity_id, b.chain, b.bridge_ts
            ORDER BY e.timestamp
        ) = 1
    ),
    second_acts AS (
        SELECT f.entity_id, f.chain, f.swap_ts,
               e.event_type, e.protocol,
               row_number() OVER (
                   PARTITION BY f.entity_id, f.chain, f.swap_ts
                   ORDER BY e.timestamp
               ) as rn
        FROM first_swap f
        JOIN canonical_events e ON f.entity_id = e.entity_id
            AND f.chain = e.chain
            AND e.timestamp > f.swap_ts
            AND e.timestamp <= f.swap_ts + INTERVAL 1 HOUR
    )
    SELECT protocol, event_type, count() as cnt
    FROM second_acts WHERE rn = 1
    GROUP BY protocol, event_type
    ORDER BY cnt DESC LIMIT 15
""").result_rows

second_hops = [
    {"protocol": r[0], "event_type": r[1], "count": r[2]}
    for r in second_hop
]

# 4. 24h activity after bridge (by hour bucket)
hourly_24h = client.query("""
    WITH bridge_ins AS (
        SELECT entity_id, chain, timestamp as bridge_ts
        FROM canonical_events WHERE event_type = 'bridge_in'
    )
    SELECT
        dateDiff('hour', bridge_ts, e.timestamp) as hour_bucket,
        count() as events,
        countIf(e.event_type = 'swap') as swaps,
        uniqExact(e.entity_id) as wallets
    FROM bridge_ins b
    JOIN canonical_events e ON b.entity_id = e.entity_id
        AND b.chain = e.chain
        AND e.timestamp > b.bridge_ts
        AND e.timestamp <= b.bridge_ts + INTERVAL 24 HOUR
    WHERE dateDiff('hour', bridge_ts, e.timestamp) < 25
    GROUP BY hour_bucket ORDER BY hour_bucket
""").result_rows

hourly = [
    {"hour": int(r[0]), "events": r[1], "swaps": r[2], "wallets": r[3]}
    for r in hourly_24h
]

# 5. Bridge events breakdown
bridge_breakdown = client.query("""
    SELECT event_type, protocol, chain, count() as cnt
    FROM canonical_events
    WHERE event_type IN ('bridge_out', 'bridge_in')
    GROUP BY event_type, protocol, chain
    ORDER BY cnt DESC
""").result_rows

bridges = [
    {"type": r[0], "protocol": r[1], "chain": r[2], "count": r[3]}
    for r in bridge_breakdown
]

# 6. Top protocols used after bridge (all actions, not just first)
post_bridge_protocols = client.query("""
    WITH bridge_ins AS (
        SELECT entity_id, chain, timestamp as bridge_ts
        FROM canonical_events WHERE event_type = 'bridge_in'
    )
    SELECT e.protocol, e.event_type, count() as cnt
    FROM bridge_ins b
    JOIN canonical_events e ON b.entity_id = e.entity_id
        AND b.chain = e.chain
        AND e.timestamp > b.bridge_ts
        AND e.timestamp <= b.bridge_ts + INTERVAL 24 HOUR
    WHERE e.protocol != ''
    GROUP BY e.protocol, e.event_type
    ORDER BY cnt DESC LIMIT 20
""").result_rows

post_protocols = [
    {"protocol": r[0], "event_type": r[1], "count": r[2]}
    for r in post_bridge_protocols
]

output = {
    "first_actions": first_actions,
    "swap_split": {
        "swap": swap_vs_non.get("swap", 0),
        "non_swap": swap_vs_non.get("non_swap", 0),
        "total": total_first,
        "swap_pct": swap_pct,
    },
    "second_hops": second_hops,
    "hourly_24h": hourly,
    "bridges": bridges,
    "post_bridge_protocols": post_protocols,
    "total_bridge_ins": sum(r[3] for r in bridge_breakdown if r[0] == "bridge_in"),
    "total_bridge_outs": sum(r[3] for r in bridge_breakdown if r[0] == "bridge_out"),
}

json.dump(output, sys.stdout)
