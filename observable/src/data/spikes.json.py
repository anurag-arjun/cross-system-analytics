#!/usr/bin/env python3
"""Spike detection — trending contracts with >200% wallet/event activity."""

import json, sys
import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse", port=8123,
    username="default", password="nexus", database="nexus",
)

# 1. Hourly activity per venue (last 7 days)
hourly_spikes = client.query("""
    WITH hourly AS (
        SELECT
            venue, protocol, chain,
            toStartOfHour(timestamp) as hour,
            count() as events,
            uniqExact(entity_id) as wallets
        FROM canonical_events
        WHERE venue != '' AND protocol != ''
          AND timestamp >= now() - INTERVAL 7 DAY
        GROUP BY venue, protocol, chain, hour
    ),
    rolling AS (
        SELECT venue, protocol, chain, hour, events, wallets,
            avg(events) OVER (
                PARTITION BY venue, protocol, chain
                ORDER BY hour
                ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_events,
            avg(wallets) OVER (
                PARTITION BY venue, protocol, chain
                ORDER BY hour
                ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_wallets
        FROM hourly
    )
    SELECT
        venue, protocol, chain, hour, events, wallets,
        round(events / greatest(rolling_avg_events, 1), 1) as events_ratio,
        round(wallets / greatest(rolling_avg_wallets, 1), 1) as wallets_ratio,
        multiIf(
            events_ratio >= 4 OR wallets_ratio >= 4, 'extreme',
            events_ratio >= 2 OR wallets_ratio >= 2, 'high',
            'normal'
        ) as alert
    FROM rolling
    WHERE rolling_avg_events > 0 AND events >= 3
    ORDER BY events_ratio DESC
    LIMIT 200
""").result_rows

spikes = [
    {
        "venue": r[0][:12] + "...", "venue_full": r[0],
        "protocol": r[1], "chain": r[2],
        "hour": str(r[3]),
        "events": r[4], "wallets": r[5],
        "events_ratio": float(r[6]),
        "wallets_ratio": float(r[7]),
        "alert": r[8],
    }
    for r in hourly_spikes
]

# 2. Extreme + high alerts
extreme = [s for s in spikes if s["alert"] == "extreme"]
high = [s for s in spikes if s["alert"] == "high"]

# 3. Top protocols by venue count
protocol_counts = client.query("""
    SELECT protocol, chain, count(DISTINCT venue) as venues,
           count() as total_events, uniqExact(entity_id) as wallets
    FROM canonical_events
    WHERE venue != '' AND protocol != ''
      AND timestamp >= now() - INTERVAL 7 DAY
    GROUP BY protocol, chain
    ORDER BY venues DESC LIMIT 15
""").result_rows

protocols = [
    {"protocol": r[0], "chain": r[1], "venues": r[2],
     "total_events": r[3], "wallets": r[4]}
    for r in protocol_counts
]

# 4. Hourly timeline of total events (last 7 days)
timeline = client.query("""
    SELECT toStartOfHour(timestamp) as hour,
           count() as events, uniqExact(entity_id) as wallets
    FROM canonical_events
    WHERE timestamp >= now() - INTERVAL 7 DAY
    GROUP BY hour ORDER BY hour
""").result_rows

hourly_timeline = [
    {"hour": str(r[0]), "events": r[1], "wallets": r[2]}
    for r in timeline
]

# 5. Daily activity per venue (last 30 days). Each day is compared against
#    the 7-day rolling average ending the day before. Same alert thresholds
#    as the hourly variant — gives a slower-burn signal that survives intra-
#    day noise.
daily_spikes = client.query("""
    WITH daily AS (
        SELECT
            venue, protocol, chain,
            toDate(timestamp) as day,
            count() as events,
            uniqExact(entity_id) as wallets
        FROM canonical_events
        WHERE venue != '' AND protocol != ''
          AND timestamp >= now() - INTERVAL 30 DAY
        GROUP BY venue, protocol, chain, day
    ),
    rolling AS (
        SELECT venue, protocol, chain, day, events, wallets,
            avg(events) OVER (
                PARTITION BY venue, protocol, chain
                ORDER BY day
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_events,
            avg(wallets) OVER (
                PARTITION BY venue, protocol, chain
                ORDER BY day
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_wallets,
            count() OVER (
                PARTITION BY venue, protocol, chain
                ORDER BY day
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) as prior_days
        FROM daily
    )
    SELECT
        venue, protocol, chain, day, events, wallets,
        round(events / greatest(rolling_avg_events, 1), 1) as events_ratio,
        round(wallets / greatest(rolling_avg_wallets, 1), 1) as wallets_ratio,
        multiIf(
            events_ratio >= 4 OR wallets_ratio >= 4, 'extreme',
            events_ratio >= 2 OR wallets_ratio >= 2, 'high',
            'normal'
        ) as alert
    FROM rolling
    WHERE prior_days >= 3       -- need at least 3 days of history for a sane baseline
      AND rolling_avg_events >= 5
      AND events >= 10
      AND day >= today() - INTERVAL 7 DAY
    ORDER BY events_ratio DESC
    LIMIT 200
""").result_rows

daily = [
    {
        "venue": r[0][:12] + "...", "venue_full": r[0],
        "protocol": r[1], "chain": r[2],
        "day": str(r[3]),
        "events": r[4], "wallets": r[5],
        "events_ratio": float(r[6]),
        "wallets_ratio": float(r[7]),
        "alert": r[8],
    }
    for r in daily_spikes
]

daily_extreme = [s for s in daily if s["alert"] == "extreme"]
daily_high = [s for s in daily if s["alert"] == "high"]

output = {
    "kpis": {
        "venues_tracked": len(spikes),
        "extreme_alerts": len(extreme),
        "high_alerts": len(high),
        "daily_extreme_alerts": len(daily_extreme),
        "daily_high_alerts": len(daily_high),
        "protocols_tracked": len(protocols),
    },
    "extreme": extreme[:25],
    "high": high[:25],
    "daily_extreme": daily_extreme[:25],
    "daily_high": daily_high[:25],
    "daily": daily[:50],
    "protocols": protocols,
    "timeline": hourly_timeline,
    "spikes": spikes[:50],
}

json.dump(output, sys.stdout)
