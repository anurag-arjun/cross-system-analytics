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
        FROM canonical_events FINAL
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
        FROM canonical_events FINAL
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
                FROM canonical_events FINAL
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            next_events AS (
                SELECT entity_id, chain, event_type, protocol, timestamp
                FROM canonical_events FINAL
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
                FROM canonical_events FINAL
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            next_events AS (
                SELECT entity_id, chain, event_type, timestamp
                FROM canonical_events FINAL
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
                FROM canonical_events FINAL
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            swaps AS (
                SELECT entity_id, chain, protocol AS swap_protocol, timestamp AS swap_time
                FROM canonical_events FINAL
                WHERE event_type = 'swap'
                  AND timestamp > now() - INTERVAL {days + 1} DAY
                  {_chain_clause(chain)}
            ),
            next_events AS (
                SELECT entity_id, chain, event_type, protocol, timestamp
                FROM canonical_events FINAL
                WHERE event_type IN ({_MEANINGFUL_LIST})
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
                FROM canonical_events FINAL
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            actions AS (
                SELECT entity_id, chain, timestamp
                FROM canonical_events FINAL
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
                FROM canonical_events FINAL
                WHERE event_type = 'bridge_in'
                  AND timestamp > now() - INTERVAL {days} DAY
                  {_chain_clause(chain)}
            ),
            actions AS (
                SELECT entity_id, chain, protocol, timestamp
                FROM canonical_events FINAL
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
            FROM canonical_events FINAL
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
            FROM canonical_events FINAL
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
            FROM canonical_events FINAL
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
        FROM canonical_events FINAL
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
        FROM canonical_events FINAL
        WHERE venue != '' AND protocol != ''
          AND timestamp > now() - INTERVAL {days} DAY
          {cc}
        GROUP BY protocol, chain
        ORDER BY venues DESC
        LIMIT {limit}
    """


# ---------------------------------------------------------------------------
# Cross-chain bridge flow (uses materialised bridge_links — bridge_out
# joined to bridge_in via link_key, populated by the bridge_links asset).
# ---------------------------------------------------------------------------


def cross_chain_matrix(days: int, chain: str | None) -> str:
    """Aggregated (src_chain, dst_chain) flow matrix for the window.

    `chain` filter, when set, restricts to either side of the bridge —
    useful for "show me everything that flowed in or out of base".
    """
    where_chain = ""
    if chain and chain != "all":
        if chain not in CHAINS:
            raise ValueError(f"unknown chain: {chain!r}")
        where_chain = f"AND (src_chain = '{chain}' OR dst_chain = '{chain}')"
    return f"""
        SELECT
            src_chain,
            dst_chain,
            count() AS bridges,
            uniqExact(src_entity_id) AS wallets,
            sum(coalesce(amount_usd, 0)) AS total_usd,
            avg(date_diff('second', src_block_time, dst_block_time)) AS avg_latency_seconds,
            quantileExact(0.5)(date_diff('second', src_block_time, dst_block_time)) AS p50_latency_seconds
        FROM bridge_links FINAL
        WHERE src_block_time > now() - INTERVAL {days} DAY
          {where_chain}
        GROUP BY src_chain, dst_chain
        ORDER BY bridges DESC
        LIMIT 100
    """


def bridge_explorer_rows(
    hours: int,
    chains: list[str] | None,
    bridges: list[str] | None,
    limit: int,
    start: str | None = None,
    end: str | None = None,
) -> str:
    """Union of three legs for the bridge explorer:

      pair       — matched bridge_out + bridge_in via bridge_links
      orphan_out — bridge_out with no row in bridge_links.src_event_id
      orphan_in  — bridge_in  with no row in bridge_links.dst_event_id

    All three legs return the same column shape so the classifier sees
    a uniform row dict. Chain filter applies to either side of the
    bridge (src OR dst); bridge filter restricts protocol.

    `hours` is the window for bridge_outs / pairs. For orphan_ins we
    widen to 7 days because a bridge_in's matching bridge_out may have
    happened up to 7 days earlier.
    """
    bridge_in_filter = ""
    bridge_out_filter = ""
    if bridges:
        slugs = ",".join(f"'{b}'" for b in bridges)
        bridge_in_filter  = f"AND bridge IN ({slugs})"
        bridge_out_filter = f"AND bridge IN ({slugs})"

    chain_pair = ""
    chain_out = ""
    chain_in = ""
    if chains:
        slugs = ",".join(f"'{c}'" for c in chains)
        chain_pair = f"AND (bl.src_chain IN ({slugs}) OR bl.dst_chain IN ({slugs}))"
        chain_out  = f"AND chain IN ({slugs})"
        chain_in   = f"AND chain IN ({slugs})"

    if start and end:
        src_window = f"src_block_time >= toDateTime64('{start}', 3) AND src_block_time < toDateTime64('{end}', 3)"
        dst_window = f"dst_block_time >= toDateTime64('{start}', 3) AND dst_block_time < toDateTime64('{end}', 3)"
        ev_window = f"timestamp >= toDateTime64('{start}', 3) AND timestamp < toDateTime64('{end}', 3)"
        pair_in_window = (
            f"timestamp >= toDateTime64('{start}', 3) "
            f"AND timestamp < toDateTime64('{end}', 3) + INTERVAL 7 DAY"
        )
        pair_window = src_window
    else:
        src_window = f"src_block_time > now() - INTERVAL {hours} HOUR"
        dst_window = f"dst_block_time > now() - INTERVAL {hours} HOUR"
        ev_window = f"timestamp > now() - INTERVAL {hours} HOUR"
        pair_in_window = ev_window
        pair_window = src_window

    return f"""
    SELECT *
    FROM (
    WITH
      -- already-linked source events (anti-join target for orphan_out)
      linked_src AS (
        SELECT src_event_id FROM bridge_links
        WHERE {src_window}
      ),
      linked_dst AS (
        SELECT dst_event_id FROM bridge_links
        WHERE {dst_window}
      )

    -- Leg 1: matched pairs
    SELECT
      'pair' AS row_type,
      bl.link_key AS link_key,
      bl.link_key_type AS link_key_type,
      bo.protocol AS bridge,
      bl.src_chain      AS src_chain,
      bl.src_block_time AS src_block_time,
      bl.src_tx_hash    AS src_tx_hash,
      bl.src_entity_id  AS src_entity_id,
      bl.src_event_id   AS src_event_id,
      bl.dst_chain      AS dst_chain,
      bl.dst_block_time AS dst_block_time,
      bl.dst_tx_hash    AS dst_tx_hash,
      bl.dst_entity_id  AS dst_entity_id,
      bl.dst_event_id   AS dst_event_id,
      bo.token_out      AS src_token,
      bo.amount_out     AS src_amount,
      bo.amount_out_usd AS src_amount_usd,
      bi.token_in       AS dst_token,
      bi.amount_in      AS dst_amount,
      bi.amount_in_usd  AS dst_amount_usd,
      toInt32(date_diff('second', bl.src_block_time, bl.dst_block_time)) AS latency_seconds,
      ''                AS dst_chain_id_hint,
      ''                AS src_chain_id_hint
    FROM (SELECT * FROM bridge_links WHERE {pair_window}) AS bl
    LEFT JOIN (SELECT * FROM canonical_events WHERE event_type='bridge_out' AND {ev_window}) AS bo ON bo.event_id = bl.src_event_id
    LEFT JOIN (SELECT * FROM canonical_events WHERE event_type='bridge_in' AND {pair_in_window}) AS bi ON bi.event_id = bl.dst_event_id
    WHERE 1=1
      {bridge_in_filter.replace('bridge', 'bo.protocol')}
      {chain_pair}

    UNION ALL

    -- Leg 2: orphan bridge_outs
    SELECT
      'orphan_out' AS row_type,
      link_key, link_key_type, protocol AS bridge,
      chain        AS src_chain,
      timestamp    AS src_block_time,
      tx_hash      AS src_tx_hash,
      entity_id    AS src_entity_id,
      event_id     AS src_event_id,
      NULL AS dst_chain, NULL AS dst_block_time, NULL AS dst_tx_hash,
      NULL AS dst_entity_id, NULL AS dst_event_id,
      token_out     AS src_token,
      amount_out    AS src_amount,
      amount_out_usd AS src_amount_usd,
      NULL AS dst_token, NULL AS dst_amount, NULL AS dst_amount_usd,
      NULL AS latency_seconds,
      extract(extra, '"destination_chain_id"\\s*:\\s*(\\d+)') AS dst_chain_id_hint,
      ''  AS src_chain_id_hint
    FROM (SELECT * FROM canonical_events WHERE event_type='bridge_out' AND {ev_window}) AS bo2
    WHERE 1=1
      AND event_id NOT IN (SELECT src_event_id FROM linked_src)
      {bridge_out_filter.replace('bridge', 'protocol')}
      {chain_out}

    UNION ALL

    -- Leg 3: orphan bridge_ins
    SELECT
      'orphan_in' AS row_type,
      link_key, link_key_type, protocol AS bridge,
      NULL AS src_chain, NULL AS src_block_time, NULL AS src_tx_hash,
      NULL AS src_entity_id, NULL AS src_event_id,
      chain      AS dst_chain,
      timestamp  AS dst_block_time,
      tx_hash    AS dst_tx_hash,
      entity_id  AS dst_entity_id,
      event_id   AS dst_event_id,
      NULL AS src_token, NULL AS src_amount, NULL AS src_amount_usd,
      token_in       AS dst_token,
      amount_in      AS dst_amount,
      amount_in_usd  AS dst_amount_usd,
      NULL AS latency_seconds,
      ''  AS dst_chain_id_hint,
      coalesce(
        nullif(JSONExtractString(extra, 'origin_chain_id'), ''),
        extract(extra, '"origin_chain_id"\\s*:\\s*(\\d+)')
      ) AS src_chain_id_hint
    FROM (SELECT * FROM canonical_events WHERE event_type='bridge_in' AND {ev_window}) AS bi2
    WHERE 1=1
      AND event_id NOT IN (SELECT dst_event_id FROM linked_dst)
      {bridge_in_filter.replace('bridge', 'protocol')}
      {chain_in}
    ) AS explorer_rows
    ORDER BY coalesce(src_block_time, dst_block_time) DESC
    LIMIT {limit}
    """


def bridge_completion(days: int, chain: str | None) -> str:
    """Of the bridge_outs in the window, how many got matched (ie a
    bridge_in was found within 7 days)? `link_rate` is the headline.
    """
    cc = _chain_clause(chain)  # matches event.chain (the src side)
    return f"""
        WITH outs AS (
            SELECT
                event_id, chain, timestamp, link_key
            FROM canonical_events FINAL
            WHERE event_type = 'bridge_out'
              AND link_key IS NOT NULL
              AND timestamp > now() - INTERVAL {days} DAY
              {cc}
        )
        SELECT
            count() AS bridge_outs,
            countIf(bl.src_event_id != '') AS matched,
            count() - countIf(bl.src_event_id != '') AS unmatched,
            round(100.0 * countIf(bl.src_event_id != '') / count(), 2) AS link_rate_pct
        FROM outs o
        LEFT JOIN (
            SELECT src_event_id FROM bridge_links FINAL
            WHERE src_block_time > now() - INTERVAL {days} DAY
        ) bl ON bl.src_event_id = o.event_id
    """
