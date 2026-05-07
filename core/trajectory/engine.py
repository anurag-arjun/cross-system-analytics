from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, List, Optional, Protocol

from core.adapters.base import CanonicalEvent
from core.identity.graph import IdentityGraph


@dataclass
class Filter:
    field: str
    op: str
    value: any


class ClickHouseClientLike(Protocol):
    def query(self, query: str, parameters: Optional[dict[str, Any]] = None) -> Any: ...


class TrajectoryEngine:
    def __init__(
        self,
        clickhouse_client: ClickHouseClientLike | None = None,
        identity_graph: IdentityGraph | None = None,
    ) -> None:
        self.client = clickhouse_client
        self.graph = identity_graph

    def query(
        self,
        entity_id: str,
        anchor_event: str,
        window_before: timedelta,
        window_after: timedelta,
        entity_type: str = "wallet",
        filters: Optional[List[Filter]] = None,
        include_anchor: bool = True,
    ) -> List[CanonicalEvent]:
        if self.client is None:
            return []

        resolved_id = self._resolve_to_wallet(entity_id, entity_type)
        if resolved_id is None:
            return []

        anchor_ts = self._find_anchor_timestamp(resolved_id, anchor_event)
        if anchor_ts is None:
            return []

        rows = self._query_window(
            resolved_id,
            anchor_ts - window_before,
            anchor_ts + window_after,
            anchor_event if not include_anchor else None,
            filters,
        )
        return [_row_to_event(row) for row in rows]

    def query_omnichain(
        self,
        entity_id: str,
        anchor_event: str,
        window_before: timedelta,
        window_after: timedelta,
        entity_type: str = "wallet",
        anchor_protocol: str | None = None,
        first_interaction: bool = False,
        filters: Optional[List[Filter]] = None,
        include_anchor: bool = True,
    ) -> List[CanonicalEvent]:
        """Fetch all events for a wallet across ALL chains in a time window
        around an anchor event. Does not require bridge linking.

        Args:
            entity_id: Wallet address or other entity identifier.
            anchor_event: Event type to anchor on (e.g. 'swap', 'bridge_out').
            window_before: How far before the anchor to include.
            window_after: How far after the anchor to include.
            entity_type: Type of entity_id (default 'wallet').
            anchor_protocol: If set, filter the anchor to this protocol
                (protocol-aware anchor).
            first_interaction: If True, use the FIRST occurrence (MIN timestamp)
                instead of the most recent (MAX).
            filters: Optional list of Filter objects applied to window events.
            include_anchor: Include the anchor event itself in results.

        Returns:
            Events from all chains merged and sorted by timestamp globally.
        """
        if self.client is None:
            return []

        resolved_id = self._resolve_to_wallet(entity_id, entity_type)
        if resolved_id is None:
            return []

        anchor_ts = self._find_anchor_timestamp(
            resolved_id,
            anchor_event,
            anchor_protocol=anchor_protocol,
            first_interaction=first_interaction,
        )
        if anchor_ts is None:
            return []

        rows = self._query_window(
            resolved_id,
            anchor_ts - window_before,
            anchor_ts + window_after,
            anchor_event if not include_anchor else None,
            filters,
        )
        return [_row_to_event(row) for row in rows]

    def query_bridge_following(
        self,
        entity_id: str,
        window_before: timedelta,
        window_after: timedelta,
        entity_type: str = "wallet",
        bridge_protocol: str | None = None,
        filters: Optional[List[Filter]] = None,
    ) -> List[CanonicalEvent]:
        """Follow a bridge_out to the destination chain for a contiguous
        cross-chain timeline.  The anchor is always the most recent bridge_out.

        Returns events in temporal order:
          source-chain pre-bridge → bridge_out → bridge_in → dest-chain post-bridge

        Args:
            entity_id: Wallet address or other entity identifier.
            window_before: How far before the bridge_out to include on source chain.
            window_after: How far after the bridge_out to include on
                destination chain (measured from bridge_out time, not bridge_in).
            entity_type: Type of entity_id (default 'wallet').
            bridge_protocol: If set, anchor on bridge_out from this specific
                protocol (e.g. 'across', 'stargate').
            filters: Optional list of Filter objects (applied to source chain
                events only).

        Returns:
            Contiguous cross-chain timeline, or source-only events if the bridge
            link is not yet matched.
        """
        if self.client is None:
            return []

        resolved_id = self._resolve_to_wallet(entity_id, entity_type)
        if resolved_id is None:
            return []

        # Find the most recent bridge_out anchor
        anchor_ts = self._find_anchor_timestamp(
            resolved_id, "bridge_out",
            anchor_protocol=bridge_protocol,
        )
        if anchor_ts is None:
            return []

        # Get source chain events: pre-bridge window up to (and including) bridge_out
        source_rows = self._query_window(
            resolved_id,
            anchor_ts - window_before,
            anchor_ts,
            exclude_event=None,
            filters=filters,
        )
        source_events = [_row_to_event(r) for r in source_rows]

        if not source_events:
            return []

        # Extract the bridge_out event (last in the window by timestamp)
        bridge_out = None
        for ev in reversed(source_events):
            if ev.event_type == "bridge_out" and ev.link_key:
                bridge_out = ev
                break

        if bridge_out is None:
            return source_events

        # Resolve the bridge link to find bridge_in on destination chain
        link_sql = """
            SELECT dst_chain, dst_entity_id, dst_block_time, dst_event_id
            FROM bridge_links
            WHERE link_key = {link_key:String}
              AND link_key_type = {link_key_type:String}
              AND src_entity_id = {entity_id:String}
            LIMIT 1
        """
        link_result = self.client.query(
            link_sql,
            parameters={
                "link_key": bridge_out.link_key,
                "link_key_type": bridge_out.link_key_type,
                "entity_id": resolved_id,
            },
        )

        if not link_result.result_rows:
            return source_events

        dst_chain, dst_entity_id, bridge_in_ts, dst_event_id = link_result.result_rows[0]

        if not dst_entity_id:
            return source_events

        # Get destination chain events from bridge_in time onward
        dest_rows = self._query_dst_chain_events(
            dst_entity_id,
            dst_chain,
            bridge_in_ts,
            anchor_ts + window_after,
        )
        dest_events = [_row_to_event(r) for r in dest_rows]

        # Merge: source events + dest events, deduplicate, sort by timestamp
        all_events = list(source_events)
        seen = {e.event_id for e in all_events}
        for ev in dest_events:
            if ev.event_id not in seen:
                all_events.append(ev)
                seen.add(ev.event_id)

        all_events.sort(key=lambda e: e.timestamp)
        return all_events

    def query_cross_chain(
        self,
        entity_id: str,
        anchor_event: str,
        window_before: timedelta,
        window_after: timedelta,
        entity_type: str = "wallet",
        filters: Optional[List[Filter]] = None,
        include_anchor: bool = True,
    ) -> List[CanonicalEvent]:
        if self.client is None:
            return []

        resolved_id = self._resolve_to_wallet(entity_id, entity_type)
        if resolved_id is None:
            return []

        same_chain = self.query(
            resolved_id,
            anchor_event,
            window_before,
            window_after,
            entity_type="wallet",
            filters=filters,
            include_anchor=include_anchor,
        )
        if not same_chain:
            return same_chain

        start_ts = min(e.timestamp for e in same_chain)
        end_ts = max(e.timestamp for e in same_chain)

        bridge_outs = [e for e in same_chain if e.event_type == "bridge_out"]
        if not bridge_outs:
            return same_chain

        cross_chain_events = self._resolve_bridge_links(resolved_id, bridge_outs, start_ts, end_ts)

        all_events = list(same_chain)
        seen = {e.event_id for e in all_events}
        for ev in cross_chain_events:
            if ev.event_id not in seen:
                all_events.append(ev)
                seen.add(ev.event_id)

        all_events.sort(key=lambda e: e.timestamp)
        return all_events

    def _find_anchor_timestamp(
        self,
        entity_id: str,
        anchor_event: str,
        anchor_protocol: str | None = None,
        first_interaction: bool = False,
    ) -> Any:
        order_clause = "ASC" if first_interaction else "DESC"
        sql = (
            "SELECT timestamp FROM canonical_events"
            " WHERE entity_id = {entity_id:String}"
            " AND event_type = {event_type:String}"
        )
        params: dict[str, Any] = {
            "entity_id": entity_id,
            "event_type": anchor_event,
        }
        if anchor_protocol:
            sql += " AND protocol = {protocol:String}"
            params["protocol"] = anchor_protocol
        sql += f" ORDER BY timestamp {order_clause} LIMIT 1"

        result = self.client.query(sql, parameters=params)
        if not result.result_rows:
            return None
        return result.result_rows[0][0]

    def _query_window(
        self,
        entity_id: str,
        start: Any,
        end: Any,
        exclude_event: str | None,
        filters: Optional[List[Filter]],
    ) -> list[tuple]:
        sql = _build_window_query(exclude_event, filters)
        params: dict[str, Any] = {
            "entity_id": entity_id,
            "start": start,
            "end": end,
        }
        if exclude_event:
            params["exclude_event"] = exclude_event
        if filters:
            for i, f in enumerate(filters):
                params[f"filter_{i}"] = f.value

        result = self.client.query(sql, parameters=params)
        return result.result_rows

    def _resolve_bridge_links(
        self,
        entity_id: str,
        bridge_outs: list[CanonicalEvent],
        start_ts: Any,
        end_ts: Any,
    ) -> list[CanonicalEvent]:
        link_keys = [e.link_key for e in bridge_outs if e.link_key]
        if not link_keys:
            return []

        links_sql = """
            SELECT link_key, dst_chain, dst_entity_id, dst_block_time
            FROM bridge_links
            WHERE link_key IN {link_keys:Array(String)}
              AND src_entity_id = {entity_id:String}
        """
        links_result = self.client.query(
            links_sql,
            parameters={"link_keys": link_keys, "entity_id": entity_id},
        )
        if not links_result.result_rows:
            return []

        cross_chain: list[CanonicalEvent] = []
        for row in links_result.result_rows:
            _, dst_chain, dst_entity_id, _ = row
            if not dst_entity_id:
                continue
            dst_rows = self._query_dst_chain_events(dst_entity_id, dst_chain, start_ts, end_ts)
            cross_chain.extend(_row_to_event(r) for r in dst_rows)

        return cross_chain

    def _query_dst_chain_events(
        self, entity_id: str, chain: str, start: Any, end: Any
    ) -> list[tuple]:
        sql = """
            SELECT
                entity_id, entity_type, event_id, event_type, event_category,
                timestamp, source_system, source_event_id, chain, block_number,
                block_time, tx_hash, log_index, protocol, venue, token_in,
                token_out, amount_in, amount_out, amount_in_usd,
                amount_out_usd, counterparty, aggregator, link_key,
                link_key_type, extra
            FROM canonical_events
            WHERE entity_id = {entity_id:String}
              AND chain = {chain:String}
              AND timestamp >= {start:DateTime64(3)}
              AND timestamp <= {end:DateTime64(3)}
            ORDER BY timestamp ASC
        """
        result = self.client.query(
            sql, parameters={"entity_id": entity_id, "chain": chain, "start": start, "end": end}
        )
        return result.result_rows

    def _resolve_to_wallet(self, entity_id: str, entity_type: str) -> str | None:
        if entity_type == "wallet":
            return entity_id
        if self.graph is None:
            return entity_id
        results = self.graph.resolve(
            entity_id, entity_type=entity_type, target_type="wallet", max_depth=3
        )
        if results:
            return results[0].entity_id
        return entity_id


_ROW_COLUMNS = [
    "entity_id",
    "entity_type",
    "event_id",
    "event_type",
    "event_category",
    "timestamp",
    "source_system",
    "source_event_id",
    "chain",
    "block_number",
    "block_time",
    "tx_hash",
    "log_index",
    "protocol",
    "venue",
    "token_in",
    "token_out",
    "amount_in",
    "amount_out",
    "amount_in_usd",
    "amount_out_usd",
    "counterparty",
    "aggregator",
    "link_key",
    "link_key_type",
    "extra",
]

_VALID_FIELDS = set(_ROW_COLUMNS)
_VALID_OPS = {"=", "!=", ">", "<", ">=", "<=", "LIKE", "NOT LIKE", "IN", "NOT IN"}


def _build_window_query(exclude_event: str | None, filters: Optional[List[Filter]]) -> str:
    sql = """
        SELECT
            entity_id, entity_type, event_id, event_type, event_category,
            timestamp, source_system, source_event_id, chain, block_number,
            block_time, tx_hash, log_index, protocol, venue, token_in,
            token_out, amount_in, amount_out, amount_in_usd,
            amount_out_usd, counterparty, aggregator, link_key,
            link_key_type, extra
        FROM canonical_events
        WHERE entity_id = {entity_id:String}
          AND timestamp >= {start:DateTime64(3)}
          AND timestamp <= {end:DateTime64(3)}
    """
    if exclude_event:
        sql += "\n          AND event_type != {exclude_event:String}"
    if filters:
        for i, f in enumerate(filters):
            field = _validate_field(f.field)
            op = _validate_op(f.op)
            sql += f"\n          AND {field} {op} {{filter_{i}}}"
    sql += "\n        ORDER BY timestamp ASC"
    return sql


def _validate_field(field: str) -> str:
    if field not in _VALID_FIELDS:
        raise ValueError(f"Invalid filter field: {field}")
    return field


def _validate_op(op: str) -> str:
    op_upper = op.upper()
    if op_upper not in _VALID_OPS:
        raise ValueError(f"Invalid filter operator: {op}")
    return op_upper


def _row_to_event(row: tuple) -> CanonicalEvent:
    (
        entity_id,
        entity_type,
        event_id,
        event_type,
        event_category,
        timestamp,
        source_system,
        source_event_id,
        chain,
        block_number,
        block_time,
        tx_hash,
        log_index,
        protocol,
        venue,
        token_in,
        token_out,
        amount_in,
        amount_out,
        amount_in_usd,
        amount_out_usd,
        counterparty,
        aggregator,
        link_key,
        link_key_type,
        extra_json,
    ) = row

    extra = None
    if extra_json:
        try:
            extra = json.loads(extra_json)
        except (json.JSONDecodeError, TypeError):
            extra = None

    def _maybe_decimal(v):
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))

    return CanonicalEvent(
        entity_id=entity_id,
        entity_type=entity_type,
        event_id=event_id,
        event_type=event_type,
        event_category=event_category,
        timestamp=timestamp,
        source_system=source_system,
        source_event_id=source_event_id,
        chain=chain or None,
        block_number=block_number,
        block_time=block_time,
        tx_hash=tx_hash,
        log_index=log_index,
        protocol=protocol or None,
        venue=venue or None,
        token_in=token_in,
        token_out=token_out,
        amount_in=_maybe_decimal(amount_in),
        amount_out=_maybe_decimal(amount_out),
        amount_in_usd=_maybe_decimal(amount_in_usd),
        amount_out_usd=_maybe_decimal(amount_out_usd),
        counterparty=counterparty,
        aggregator=aggregator or None,
        link_key=link_key,
        link_key_type=link_key_type,
        extra=extra,
    )
