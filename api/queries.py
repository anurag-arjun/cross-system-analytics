"""SQL builders for the BD dashboard endpoints.

All queries take a ``days`` window and an optional ``chain`` filter. The
canonical_events table has ``ORDER BY (entity_id, timestamp)`` so per-entity
lookups (the bridge flow queries) are fast.

Bridge flow queries use ASOF JOIN to attach the next action to each
bridge_in event. The ASOF predicate `timestamp > b.timestamp` matches the
first event with a strictly greater timestamp; we then cap at 24h.

Spike queries are direct aggregations against canonical_events for the
hourly and daily window. Equivalent to the Observable spikes.json.py but
returned as JSON for the API consumer.
"""

from __future__ import annotations

from typing import Iterable


CHAINS = ("ethereum", "base", "arbitrum", "optimism", "polygon")


def _chain_clause(chain: str | None) -> str:
    if not chain or chain == "all":
        return ""
    if chain not in CHAINS:
        raise ValueError(f"unknown chain: {chain!r}")
    return f"AND chain = '{chain}'"


# "Meaningful DeFi action" — what we want to surface as the first action after
# a bridge. Excludes setup noise (transfer_out, approval) which dominates
# raw event-type counts. This list is the projection of the BD question
# "what apps did users interact with after bridging?" — i.e. apps, not
# token plumbing.
MEANINGFUL_DEFI_TYPES = (
    "swap", "swap_internal",
    "deposit", "withdrawal",   # wrap / unwrap, LP deposit/withdraw
    "lend_deposit", "lend_borrow", "lend_repay", "lend_withdraw",
    "stake", "stake_unstake",
    "lp_add", "lp_remove",
    "claim",
    "perp_open", "perp_close", "perp_liquidate",
    "pool_create",
)
_MEANINGFUL_LIST = ", ".join(f"'{t}'" for t in MEANINGFUL_DEFI_TYPES)


# ---------------------------------------------------------------------------
# Bridge flow
# ---------------------------------------------------------------------------


def bridge_summary(days: int, chain: str | None) -> str:
    cc = _chain_clause(chain)
    return f"""
        SELECT
            countIf(event_type = 'bridge_in')  AS bridge_ins,
            countIf(event_type = 'bridge_out') AS bridge_outs,
            countIf(event_type = 'swap')       AS swaps,
            countIf(event_type LIKE 'lend_%'
                    OR event_type LIKE 'stake%'
                    OR event_type LIKE 'lp_%'
                    OR event_type = 'claim')   AS non_swap_defi
        FROM canonical_events
        WHERE timestamp > now() - INTERVAL {days} DAY
          {cc}
    """


def bridge_breakdown(days: int, chain: str | None) -> str:
    """Bridge events broken down by protocol + chain. Powers the rightmost
    table in the BD doc's bridge flow page."""
    cc = _chain_clause(chain)
    return f"""
        SELECT
            chain,
            protocol AS bridge_protocol,
            countIf(event_type = 'bridge_in')  AS bridge_ins,
            countIf(event_type = 'bridge_out') AS bridge_outs,
            count() AS total
        FROM canonical_events
        WHERE event_type IN ('bridge_in', 'bridge_out')
          AND timestamp > now() - INTERVAL {days} DAY
          AND protocol != ''
          {cc}
        GROUP BY chain, bridge_protocol
        ORDER BY total DESC
        LIMIT 100
    """


def first_action_after_bridge(days: int, chain: str | None) -> str:
    """First *meaningful DeFi action* per (bridge_protocol, next_app,
    action_type) within 24h of bridging. Excludes setup noise like ERC20
    transfers and approvals — the BD question is "what app did the user
    use", not "did tokens move into their wallet"."""
    return f"""
        WITH
            bridges AS (
                SELECT entity_id, chain, protocol AS bridge_protocol, timestamp AS bridge_time
                FROM canonical_events
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            next_events AS (
                SELECT entity_id, chain, event_type, protocol, timestamp
                FROM canonical_events
                WHERE event_type IN ({_MEANINGFUL_LIST})
                  AND timestamp > now() - INTERVAL {days + 1} DAY
                  {_chain_clause(chain)}
            )
        SELECT
            b.bridge_protocol AS bridge_protocol,
            a.protocol        AS next_app,
            a.event_type      AS action_type,
            count()           AS n,
            quantile(0.5)(dateDiff('second', b.bridge_time, a.timestamp)) AS median_seconds
        FROM bridges AS b
        ASOF LEFT JOIN next_events AS a
          ON  b.entity_id = a.entity_id
          AND b.chain     = a.chain
          AND b.bridge_time < a.timestamp
        WHERE a.protocol != ''
          AND dateDiff('hour', b.bridge_time, a.timestamp) <= 24
        GROUP BY bridge_protocol, next_app, action_type
        ORDER BY n DESC
        LIMIT 100
    """


def swap_vs_non_swap(days: int, chain: str | None) -> str:
    """Bar-chart data: of the bridge_ins in the window, how many had a swap
    as their first DeFi action vs other DeFi vs nothing?

    "Other DeFi" only fires when non-swap decoders are wired up (Aave,
    Lido, Morpho, etc.) — until then expect mostly swap + no_action.
    """
    return f"""
        WITH
            bridges AS (
                SELECT entity_id, chain, timestamp AS bridge_time
                FROM canonical_events
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            next_events AS (
                SELECT entity_id, chain, event_type, timestamp
                FROM canonical_events
                WHERE event_type IN ({_MEANINGFUL_LIST})
                  AND timestamp > now() - INTERVAL {days + 1} DAY
                  {_chain_clause(chain)}
            ),
            joined AS (
                SELECT
                    b.entity_id,
                    b.chain,
                    if(
                        a.event_type = ''
                        OR dateDiff('hour', b.bridge_time, a.timestamp) > 24,
                        'no_action',
                        if(a.event_type IN ('swap', 'swap_internal'),
                           'swap', 'other_defi')
                    ) AS bucket
                FROM bridges AS b
                ASOF LEFT JOIN next_events AS a
                  ON  b.entity_id = a.entity_id
                  AND b.chain     = a.chain
                  AND b.bridge_time < a.timestamp
            )
        SELECT bucket, count() AS n
        FROM joined
        GROUP BY bucket
        ORDER BY n DESC
    """


def second_hop_after_swap(days: int, chain: str | None) -> str:
    """After a user swaps (first action), what's their second action?"""
    return f"""
        WITH
            bridges AS (
                SELECT entity_id, chain, timestamp AS bridge_time
                FROM canonical_events
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            swaps AS (
                SELECT entity_id, chain, protocol AS swap_protocol, timestamp AS swap_time
                FROM canonical_events
                WHERE event_type = 'swap'
                  AND timestamp > now() - INTERVAL {days + 1} DAY
                  {_chain_clause(chain)}
            ),
            next_events AS (
                SELECT entity_id, chain, event_type, protocol, timestamp
                FROM canonical_events
                WHERE event_type != 'bridge_in'
                  AND timestamp > now() - INTERVAL {days + 2} DAY
                  {_chain_clause(chain)}
            ),
            bridges_with_swap AS (
                SELECT b.entity_id, b.chain, s.swap_protocol, s.swap_time
                FROM bridges b
                ASOF JOIN swaps s
                  ON b.entity_id = s.entity_id
                  AND b.chain = s.chain
                  AND b.bridge_time < s.swap_time
                WHERE dateDiff('hour', b.bridge_time, s.swap_time) <= 24
            )
        SELECT
            bs.swap_protocol AS after_swap_on,
            a.event_type     AS next_action,
            a.protocol       AS next_protocol,
            count()          AS n
        FROM bridges_with_swap bs
        ASOF LEFT JOIN next_events a
          ON  bs.entity_id = a.entity_id
          AND bs.chain     = a.chain
          AND bs.swap_time < a.timestamp
        WHERE a.protocol != ''
          AND dateDiff('hour', bs.swap_time, a.timestamp) <= 24
        GROUP BY after_swap_on, next_action, next_protocol
        ORDER BY n DESC
        LIMIT 100
    """


def activity_after_bridge_24h(days: int, chain: str | None) -> str:
    """Hourly count of *meaningful DeFi actions* in the 24h after each
    bridge_in. Rows keyed by `hour_offset` (0-24)."""
    return f"""
        WITH
            bridges AS (
                SELECT entity_id, chain, timestamp AS bridge_time
                FROM canonical_events
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            actions AS (
                SELECT entity_id, chain, timestamp
                FROM canonical_events
                WHERE event_type IN ({_MEANINGFUL_LIST})
                  AND timestamp > now() - INTERVAL {days + 1} DAY
                  {_chain_clause(chain)}
            )
        SELECT
            dateDiff('hour', b.bridge_time, a.timestamp) AS hour_offset,
            count() AS n
        FROM bridges b
        INNER JOIN actions a
          ON  b.entity_id = a.entity_id
          AND b.chain     = a.chain
        WHERE a.timestamp > b.bridge_time
          AND dateDiff('hour', b.bridge_time, a.timestamp) BETWEEN 0 AND 24
        GROUP BY hour_offset
        ORDER BY hour_offset
    """


def top_protocols_after_bridge_24h(days: int, chain: str | None) -> str:
    """Of all *meaningful DeFi actions* in 24h-post-bridge, top protocols."""
    return f"""
        WITH
            bridges AS (
                SELECT entity_id, chain, timestamp AS bridge_time
                FROM canonical_events
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            actions AS (
                SELECT entity_id, chain, protocol, timestamp
                FROM canonical_events
                WHERE event_type IN ({_MEANINGFUL_LIST})
                  AND protocol != ''
                  AND timestamp > now() - INTERVAL {days + 1} DAY
                  {_chain_clause(chain)}
            )
        SELECT
            a.protocol AS protocol,
            count()    AS n
        FROM bridges b
        INNER JOIN actions a
          ON  b.entity_id = a.entity_id
          AND b.chain     = a.chain
        WHERE a.timestamp > b.bridge_time
          AND dateDiff('hour', b.bridge_time, a.timestamp) <= 24
        GROUP BY protocol
        ORDER BY n DESC
        LIMIT 20
    """


# ---------------------------------------------------------------------------
# Spike detection
# ---------------------------------------------------------------------------


def spike_summary(days: int) -> str:
    """KPI counts mirroring observable/src/data/spikes.json.py kpis."""
    return f"""
        WITH hourly AS (
            SELECT venue, protocol, chain,
                toStartOfHour(timestamp) AS hour,
                count() AS events,
                uniqExact(entity_id) AS wallets
            FROM canonical_events
            WHERE venue != '' AND protocol != ''
              AND timestamp > now() - INTERVAL {days} DAY
            GROUP BY venue, protocol, chain, hour
        ),
        rolling AS (
            SELECT venue, protocol, chain, hour, events, wallets,
                avg(events) OVER (
                    PARTITION BY venue, protocol, chain
                    ORDER BY hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_events,
                avg(wallets) OVER (
                    PARTITION BY venue, protocol, chain
                    ORDER BY hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_wallets
            FROM hourly
        ),
        flagged AS (
            SELECT
                events / greatest(rolling_avg_events, 1) AS er,
                wallets / greatest(rolling_avg_wallets, 1) AS wr
            FROM rolling
            WHERE rolling_avg_events > 0 AND events >= 3
        )
        SELECT
            countIf(er >= 4 OR wr >= 4) AS extreme,
            countIf((er >= 2 OR wr >= 2) AND NOT (er >= 4 OR wr >= 4)) AS high,
            count() AS venues_tracked
        FROM flagged
    """


def hourly_spikes(days: int, chain: str | None, alert: str | None, limit: int) -> str:
    cc = _chain_clause(chain)
    alert_clause = ""
    if alert in ("extreme", "high"):
        alert_clause = f"AND alert = '{alert}'"
    return f"""
        WITH hourly AS (
            SELECT venue, protocol, chain,
                toStartOfHour(timestamp) AS hour,
                count() AS events,
                uniqExact(entity_id) AS wallets
            FROM canonical_events
            WHERE venue != '' AND protocol != ''
              AND timestamp > now() - INTERVAL {days} DAY
              {cc}
            GROUP BY venue, protocol, chain, hour
        ),
        rolling AS (
            SELECT venue, protocol, chain, hour, events, wallets,
                avg(events) OVER (
                    PARTITION BY venue, protocol, chain
                    ORDER BY hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_events,
                avg(wallets) OVER (
                    PARTITION BY venue, protocol, chain
                    ORDER BY hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_wallets
            FROM hourly
        )
        SELECT
            venue, protocol, chain, hour, events, wallets,
            round(events / greatest(rolling_avg_events, 1), 1) AS events_ratio,
            round(wallets / greatest(rolling_avg_wallets, 1), 1) AS wallets_ratio,
            multiIf(
                events_ratio >= 4 OR wallets_ratio >= 4, 'extreme',
                events_ratio >= 2 OR wallets_ratio >= 2, 'high',
                'normal'
            ) AS alert
        FROM rolling
        WHERE rolling_avg_events > 0 AND events >= 3
          {alert_clause}
        ORDER BY events_ratio DESC
        LIMIT {limit}
    """


def daily_spikes(days: int, chain: str | None, alert: str | None, limit: int) -> str:
    cc = _chain_clause(chain)
    alert_clause = ""
    if alert in ("extreme", "high"):
        alert_clause = f"AND alert = '{alert}'"
    return f"""
        WITH daily AS (
            SELECT venue, protocol, chain,
                toDate(timestamp) AS day,
                count() AS events,
                uniqExact(entity_id) AS wallets
            FROM canonical_events
            WHERE venue != '' AND protocol != ''
              AND timestamp > now() - INTERVAL 30 DAY
              {cc}
            GROUP BY venue, protocol, chain, day
        ),
        rolling AS (
            SELECT venue, protocol, chain, day, events, wallets,
                avg(events) OVER (
                    PARTITION BY venue, protocol, chain
                    ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_events,
                avg(wallets) OVER (
                    PARTITION BY venue, protocol, chain
                    ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_wallets,
                count() OVER (
                    PARTITION BY venue, protocol, chain
                    ORDER BY day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ) AS prior_days
            FROM daily
        )
        SELECT
            venue, protocol, chain, day, events, wallets,
            round(events / greatest(rolling_avg_events, 1), 1) AS events_ratio,
            round(wallets / greatest(rolling_avg_wallets, 1), 1) AS wallets_ratio,
            multiIf(
                events_ratio >= 4 OR wallets_ratio >= 4, 'extreme',
                events_ratio >= 2 OR wallets_ratio >= 2, 'high',
                'normal'
            ) AS alert
        FROM rolling
        WHERE prior_days >= 3
          AND rolling_avg_events >= 5
          AND events >= 10
          AND day >= today() - INTERVAL {days} DAY
          {alert_clause}
        ORDER BY events_ratio DESC
        LIMIT {limit}
    """


def activity_timeline(days: int, chain: str | None) -> str:
    cc = _chain_clause(chain)
    return f"""
        SELECT
            toStartOfHour(timestamp) AS hour,
            count() AS events,
            uniqExact(entity_id) AS wallets
        FROM canonical_events
        WHERE timestamp > now() - INTERVAL {days} DAY
          {cc}
        GROUP BY hour
        ORDER BY hour
    """


def active_protocols(days: int, chain: str | None, limit: int) -> str:
    cc = _chain_clause(chain)
    return f"""
        SELECT
            protocol, chain,
            count(DISTINCT venue)        AS venues,
            count()                      AS events,
            uniqExact(entity_id)         AS wallets
        FROM canonical_events
        WHERE venue != '' AND protocol != ''
          AND timestamp > now() - INTERVAL {days} DAY
          {cc}
        GROUP BY protocol, chain
        ORDER BY venues DESC
        LIMIT {limit}
    """
