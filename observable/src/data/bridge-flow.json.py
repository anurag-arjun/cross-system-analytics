#!/usr/bin/env python3
"""Bridge Flow Analytics data loader for Observable Framework."""

import clickhouse_connect
import json

client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password="nexus",
    database="default",
)

# 1. Path analysis summary
path_analysis = client.query("""
    SELECT path_type, count() as cnt
    FROM bridge_flow_path_analysis
    GROUP BY path_type
    ORDER BY cnt DESC
""").result_rows

path_summary = {row[0]: row[1] for row in path_analysis}
total_entries = sum(path_summary.values())
path_pct = {k: round(v * 100.0 / max(total_entries, 1), 1) for k, v in path_summary.items()}

# 2. Top immediate apps (first action after entry)
immediate_apps = client.query("""
    SELECT next_protocol, next_event_type, count() as cnt
    FROM bridge_flow_immediate_apps
    WHERE event_rank = 1
    GROUP BY next_protocol, next_event_type
    ORDER BY cnt DESC
    LIMIT 15
""").result_rows

top_apps = [{"protocol": row[0], "event_type": row[1], "count": row[2]} for row in immediate_apps]

# 3. Time-to-first-action distribution
ttfa = client.query("""
    SELECT 
        quantile(0.10)(seconds_after) as p10,
        quantile(0.25)(seconds_after) as p25,
        quantile(0.50)(seconds_after) as p50,
        quantile(0.75)(seconds_after) as p75,
        quantile(0.90)(seconds_after) as p90,
        quantile(0.95)(seconds_after) as p95,
        avg(seconds_after) as mean
    FROM bridge_flow_immediate_apps
    WHERE event_rank = 1
""").result_rows[0]

time_to_action = {
    "p10": ttfa[0], "p25": ttfa[1], "median": ttfa[2],
    "p75": ttfa[3], "p90": ttfa[4], "p95": ttfa[5], "mean": round(ttfa[6], 1)
}

# 4. Hourly activity after entry (24h window)
hourly = client.query("""
    SELECT 
        hour_bucket, 
        sum(event_count) as events, 
        sum(volume_usd) as vol,
        count(DISTINCT entity_id) as wallets
    FROM bridge_flow_24h_activity
    GROUP BY hour_bucket
    ORDER BY hour_bucket
    LIMIT 25
""").result_rows

hourly_activity = [
    {"hour": int(row[0]), "events": row[1], "volume_usd": round(row[2], 2), "wallets": row[3]}
    for row in hourly
]

# 5. 2nd hop analysis (top protocols after first post-entry swap)
hop2 = client.query("""
    SELECT hop2_protocol, hop2_event_type, count() as cnt
    FROM bridge_flow_2nd_hop
    GROUP BY hop2_protocol, hop2_event_type
    ORDER BY cnt DESC
    LIMIT 15
""").result_rows

second_hop = [{"protocol": row[0], "event_type": row[1], "count": row[2]} for row in hop2]

# 6. Entry event breakdown by protocol
entry_breakdown = client.query("""
    SELECT entry_protocol, entry_type, chain, count() as cnt
    FROM bridge_flow_entry_events
    GROUP BY entry_protocol, entry_type, chain
    ORDER BY cnt DESC
    LIMIT 10
""").result_rows

entries = [{"protocol": row[0], "type": row[1], "chain": row[2], "count": row[3]} for row in entry_breakdown]

# 7. Summary KPIs
kpis = {
    "total_entries": total_entries,
    "unique_wallets": client.query("SELECT count(DISTINCT entity_id) FROM bridge_flow_entry_events").result_rows[0][0],
    "swap_pct": path_pct.get("swap_within_1h", 0),
    "non_swap_pct": path_pct.get("non_swap_activity_within_1h", 0),
    "idle_pct": path_pct.get("idle_within_1h", 0),
    "median_time_to_action_sec": time_to_action["median"],
    "mean_time_to_action_sec": time_to_action["mean"],
}

output = {
    "kpis": kpis,
    "path_analysis": [{"type": k, "count": v, "pct": path_pct.get(k, 0)} for k, v in path_summary.items()],
    "top_immediate_apps": top_apps,
    "time_to_action": time_to_action,
    "hourly_activity": hourly_activity,
    "second_hop": second_hop,
    "entries": entries,
}

print(json.dumps(output))
