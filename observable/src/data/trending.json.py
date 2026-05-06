#!/usr/bin/env python3
"""Trending Contracts data loader for Observable Framework."""

import clickhouse_connect
import json

client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password="nexus",
    database="default",
)

# 1. Alert distribution
alert_dist = client.query("""
    SELECT alert_level, count() as cnt
    FROM trending_contract_alerts
    GROUP BY alert_level
    ORDER BY cnt DESC
""").result_rows

alerts_by_level = {row[0]: row[1] for row in alert_dist}
total_alerts = sum(alerts_by_level.values())

# 2. Top extreme + high alerts with details
top_alerts = client.query("""
    SELECT 
        contract_address,
        chain,
        protocol,
        hour,
        current_events,
        current_wallets,
        swap_count,
        round(volume_usd, 2) as volume_usd,
        round(avg_events_24h, 2) as avg_events_24h,
        round(avg_wallets_24h, 2) as avg_wallets_24h,
        round(events_spike_ratio, 2) as events_spike_ratio,
        round(wallets_spike_ratio, 2) as wallets_spike_ratio,
        alert_level
    FROM trending_contract_alerts
    WHERE alert_level IN ('extreme', 'high')
    ORDER BY wallets_spike_ratio DESC, events_spike_ratio DESC
    LIMIT 50
""").result_rows

trending = [
    {
        "contract": row[0],
        "chain": row[1],
        "protocol": row[2],
        "hour": str(row[3]),
        "events": row[4],
        "wallets": row[5],
        "swaps": row[6],
        "volume_usd": row[7],
        "avg_events": row[8],
        "avg_wallets": row[9],
        "events_spike": row[10],
        "wallets_spike": row[11],
        "alert": row[12],
    }
    for row in top_alerts
]

# 3. Protocol summary across hours
proto_summary = client.query("""
    SELECT 
        protocol, chain,
        sum(active_contracts) as contracts,
        sum(total_events) as events,
        sum(total_wallets) as wallets,
        round(sum(total_volume_usd), 2) as volume,
        round(avg(avg_spike_ratio), 2) as avg_spike,
        round(max(max_spike_ratio), 2) as max_spike
    FROM trending_protocol_summary
    GROUP BY protocol, chain
    ORDER BY wallets DESC
    LIMIT 15
""").result_rows

protocols = [
    {
        "protocol": row[0], "chain": row[1], "contracts": row[2],
        "events": row[3], "wallets": row[4], "volume": row[5],
        "avg_spike": row[6], "max_spike": row[7],
    }
    for row in proto_summary
]

# 4. Hourly activity timeline
hourly = client.query("""
    SELECT 
        hour,
        sum(current_events) as events,
        sum(current_wallets) as wallets,
        count(DISTINCT contract_address) as contracts,
        countIf(alert_level = 'extreme') as extreme_alerts,
        countIf(alert_level = 'high') as high_alerts
    FROM trending_contract_alerts
    GROUP BY hour
    ORDER BY hour
""").result_rows

timeline = [
    {
        "hour": str(row[0]), "events": row[1], "wallets": row[2],
        "contracts": row[3], "extreme": row[4], "high": row[5],
    }
    for row in hourly
]

# 5. Extreme spike deep dive — contracts with highest wallet spikes
extreme_spikes = client.query("""
    SELECT 
        contract_address,
        chain,
        protocol,
        hour,
        current_events,
        current_wallets,
        round(events_spike_ratio, 2) as events_spike,
        round(wallets_spike_ratio, 2) as wallets_spike,
        round(volume_usd, 2) as volume
    FROM trending_contract_alerts
    WHERE alert_level = 'extreme'
    ORDER BY wallets_spike_ratio DESC
    LIMIT 25
""").result_rows

extreme = [
    {
        "contract": row[0], "chain": row[1], "protocol": row[2],
        "hour": str(row[3]), "events": row[4], "wallets": row[5],
        "events_spike": row[6], "wallets_spike": row[7], "volume": row[8],
    }
    for row in extreme_spikes
]

# KPIs
kpis = {
    "total_alerts": total_alerts,
    "extreme_alerts": alerts_by_level.get("extreme", 0),
    "high_alerts": alerts_by_level.get("high", 0),
    "moderate_alerts": alerts_by_level.get("moderate", 0),
    "contracts_tracked": client.query("SELECT count(DISTINCT contract_address) FROM trending_contract_hourly").result_rows[0][0],
    "protocols_tracked": client.query("SELECT count(DISTINCT protocol) FROM trending_contract_hourly").result_rows[0][0],
}

output = {
    "kpis": kpis,
    "trending": trending,
    "protocols": protocols,
    "timeline": timeline,
    "extreme": extreme,
}

print(json.dumps(output))
