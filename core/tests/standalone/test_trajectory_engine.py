import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from core.adapters.base import CanonicalEvent
from core.identity.graph import IdentityGraph
from core.trajectory.engine import (
    Filter,
    TrajectoryEngine,
    _row_to_event,
    _validate_field,
    _validate_op,
)


class MockQueryResult:
    def __init__(self, rows: list[tuple]):
        self.result_rows = rows


class MockClickHouseClient:
    def __init__(self, events: list[CanonicalEvent] = None, bridge_links: list[tuple] = None):
        self.events = events or []
        self.bridge_links = bridge_links or []
        self.queries: list[tuple[str, dict]] = []

    def query(self, sql: str, parameters: dict = None):
        self.queries.append((sql, parameters or {}))

        if "FROM canonical_events" in sql:
            return self._query_events(sql, parameters or {})
        elif "FROM bridge_links" in sql:
            return self._query_bridge_links(sql, parameters or {})
        return MockQueryResult([])

    def _query_events(self, sql: str, params: dict) -> MockQueryResult:
        entity_id = params.get("entity_id")
        start = params.get("start")
        end = params.get("end")
        chain = params.get("chain")
        exclude_event = params.get("exclude_event")
        event_type = params.get("event_type")
        single_col = "SELECT timestamp" in sql

        rows = []
        for ev in self.events:
            if entity_id and ev.entity_id != entity_id:
                continue
            if start and ev.timestamp < start:
                continue
            if end and ev.timestamp > end:
                continue
            if chain and ev.chain != chain:
                continue
            if exclude_event and ev.event_type == exclude_event:
                continue
            if event_type and ev.event_type != event_type:
                continue
            if not self._passes_filters(ev, sql, params):
                continue
            if single_col:
                rows.append((ev.timestamp,))
            else:
                rows.append(_event_to_row(ev))

        if single_col:
            rows.sort(key=lambda r: r[0], reverse=True)
        else:
            rows.sort(key=lambda r: r[5])
        return MockQueryResult(rows)

    def _passes_filters(self, ev: CanonicalEvent, sql: str, params: dict) -> bool:
        filter_fields = self._parse_filter_fields(sql)
        for key, value in params.items():
            if not key.startswith("filter_"):
                continue
            field = filter_fields.get(key)
            if field is None:
                continue
            actual = getattr(ev, field, None)
            if actual != value:
                return False
        return True

    def _parse_filter_fields(self, sql: str) -> dict[str, str]:
        fields = {}
        for line in sql.split("\n"):
            line = line.strip()
            if not line.startswith("AND "):
                continue
            parts = line.split()
            if len(parts) < 4:
                continue
            field = parts[1]
            param = parts[3]
            if param.startswith("{") and param.endswith("}"):
                param = param[1:-1]
                fields[param] = field
        return fields

    def _query_bridge_links(self, sql: str, params: dict) -> MockQueryResult:
        link_keys = set(params.get("link_keys", []))
        entity_id = params.get("entity_id")
        rows = [row[:4] for row in self.bridge_links if row[0] in link_keys and row[4] == entity_id]
        return MockQueryResult(rows)


def _event_to_row(ev: CanonicalEvent) -> tuple:
    import json

    return (
        ev.entity_id,
        ev.entity_type,
        ev.event_id,
        ev.event_type,
        ev.event_category,
        ev.timestamp,
        ev.source_system,
        ev.source_event_id,
        ev.chain or "",
        ev.block_number,
        ev.block_time,
        ev.tx_hash,
        ev.log_index,
        ev.protocol or "",
        ev.venue or "",
        ev.token_in,
        ev.token_out,
        ev.amount_in,
        ev.amount_out,
        ev.amount_in_usd,
        ev.amount_out_usd,
        ev.counterparty,
        ev.aggregator or "",
        ev.link_key,
        ev.link_key_type,
        json.dumps(ev.extra) if ev.extra else "{}",
    )


def _make_event(**kwargs) -> CanonicalEvent:
    defaults = dict(
        entity_id="0xaaa",
        entity_type="wallet",
        event_id="ev1",
        event_type="swap",
        event_category="transaction",
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        source_system="evm_base",
        source_event_id="tx:0",
        chain="base",
        block_number=100,
        block_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        tx_hash="0xdead",
        log_index=0,
        protocol="uniswap_v3",
        venue="0xpool",
        token_in="0xt1",
        token_out="0xt2",
        amount_in=Decimal("1.5"),
        amount_out=Decimal("2.0"),
        extra={"foo": "bar"},
    )
    defaults.update(kwargs)
    return CanonicalEvent(**defaults)


class TestTrajectoryEngineNoClient:
    def test_no_client_returns_empty(self):
        engine = TrajectoryEngine(clickhouse_client=None)
        result = engine.query(
            entity_id="0xaaa",
            anchor_event="bridge_out",
            window_before=timedelta(days=7),
            window_after=timedelta(days=7),
        )
        assert result == []


class TestTrajectoryEngineQuery:
    def test_basic_trajectory(self):
        anchor = _make_event(
            event_id="anchor",
            event_type="bridge_out",
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        )
        before = _make_event(
            event_id="before",
            event_type="swap",
            timestamp=datetime(2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc),
        )
        after = _make_event(
            event_id="after",
            event_type="swap",
            timestamp=datetime(2024, 1, 16, 12, 0, 0, tzinfo=timezone.utc),
        )
        outside = _make_event(
            event_id="outside",
            event_type="swap",
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        client = MockClickHouseClient(events=[anchor, before, after, outside])
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query(
            entity_id="0xaaa",
            anchor_event="bridge_out",
            window_before=timedelta(days=2),
            window_after=timedelta(days=2),
        )

        assert len(result) == 3
        assert result[0].event_id == "before"
        assert result[1].event_id == "anchor"
        assert result[2].event_id == "after"

    def test_exclude_anchor(self):
        anchor = _make_event(
            event_id="anchor",
            event_type="bridge_out",
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        )
        other = _make_event(
            event_id="other",
            event_type="swap",
            timestamp=datetime(2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc),
        )

        client = MockClickHouseClient(events=[anchor, other])
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query(
            entity_id="0xaaa",
            anchor_event="bridge_out",
            window_before=timedelta(days=2),
            window_after=timedelta(days=2),
            include_anchor=False,
        )

        assert len(result) == 1
        assert result[0].event_id == "other"

    def test_no_anchor_returns_empty(self):
        client = MockClickHouseClient(events=[])
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query(
            entity_id="0xaaa",
            anchor_event="bridge_out",
            window_before=timedelta(days=7),
            window_after=timedelta(days=7),
        )
        assert result == []

    def test_filter_by_event_type(self):
        anchor = _make_event(
            event_id="anchor",
            event_type="bridge_out",
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        )
        swap1 = _make_event(
            event_id="swap1",
            event_type="swap",
            timestamp=datetime(2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc),
        )
        transfer = _make_event(
            event_id="transfer",
            event_type="transfer_out",
            timestamp=datetime(2024, 1, 14, 13, 0, 0, tzinfo=timezone.utc),
        )

        client = MockClickHouseClient(events=[anchor, swap1, transfer])
        engine = TrajectoryEngine(clickhouse_client=client)

        result = engine.query(
            entity_id="0xaaa",
            anchor_event="bridge_out",
            window_before=timedelta(days=2),
            window_after=timedelta(days=2),
            filters=[Filter(field="event_type", op="=", value="swap")],
        )

        assert len(result) == 1
        assert result[0].event_id == "swap1"

    def test_identity_graph_resolution(self):
        graph = IdentityGraph()
        graph.add_relationship(
            from_entity="vitalik.eth",
            from_type="ens",
            to_entity="0xabc",
            to_type="wallet",
            relationship_type="resolved_to",
            confidence=0.95,
            source="ens",
        )

        anchor = _make_event(
            entity_id="0xabc",
            event_id="anchor",
            event_type="bridge_out",
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        )
        other = _make_event(
            entity_id="0xabc",
            event_id="other",
            event_type="swap",
            timestamp=datetime(2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc),
        )

        client = MockClickHouseClient(events=[anchor, other])
        engine = TrajectoryEngine(clickhouse_client=client, identity_graph=graph)
        result = engine.query(
            entity_id="vitalik.eth",
            anchor_event="bridge_out",
            window_before=timedelta(days=2),
            window_after=timedelta(days=2),
            entity_type="ens",
        )

        assert len(result) == 2
        assert {e.event_id for e in result} == {"anchor", "other"}

    def test_identity_graph_no_match_returns_empty(self):
        graph = IdentityGraph()
        client = MockClickHouseClient(events=[])
        engine = TrajectoryEngine(clickhouse_client=client, identity_graph=graph)
        result = engine.query(
            entity_id="unknown.eth",
            anchor_event="bridge_out",
            window_before=timedelta(days=7),
            window_after=timedelta(days=7),
            entity_type="ens",
        )
        assert result == []


class TestTrajectoryEngineCrossChain:
    def test_cross_chain_includes_dst_events(self):
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        anchor = _make_event(
            entity_id="0xaaa",
            event_id="anchor",
            event_type="bridge_out",
            timestamp=base_time,
            chain="base",
            link_key="dep_123",
            link_key_type="across_deposit_id",
        )
        before = _make_event(
            entity_id="0xaaa",
            event_id="before",
            event_type="swap",
            timestamp=base_time - timedelta(days=1),
            chain="base",
        )

        dst_swap = _make_event(
            entity_id="0xbbb",
            event_id="dst_swap",
            event_type="swap",
            timestamp=base_time - timedelta(hours=1),
            chain="ethereum",
        )

        bridge_link = (
            "dep_123",
            "ethereum",
            "0xbbb",
            base_time + timedelta(hours=1),
            "0xaaa",
        )

        client = MockClickHouseClient(
            events=[anchor, before, dst_swap],
            bridge_links=[bridge_link],
        )
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query_cross_chain(
            entity_id="0xaaa",
            anchor_event="bridge_out",
            window_before=timedelta(days=2),
            window_after=timedelta(days=2),
        )

        assert len(result) == 3
        event_ids = [e.event_id for e in result]
        assert event_ids == ["before", "dst_swap", "anchor"]

    def test_cross_chain_no_bridge_out_returns_same_chain(self):
        anchor = _make_event(
            event_id="anchor",
            event_type="swap",
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        )
        client = MockClickHouseClient(events=[anchor])
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query_cross_chain(
            entity_id="0xaaa",
            anchor_event="swap",
            window_before=timedelta(days=2),
            window_after=timedelta(days=2),
        )
        assert len(result) == 1
        assert result[0].event_id == "anchor"

    def test_cross_chain_deduplicates_events(self):
        base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        anchor = _make_event(
            entity_id="0xaaa",
            event_id="anchor",
            event_type="bridge_out",
            timestamp=base_time,
            chain="base",
            link_key="dep_456",
        )

        bridge_link = (
            "dep_456",
            "ethereum",
            "0xaaa",
            base_time + timedelta(hours=1),
            "0xaaa",
        )

        client = MockClickHouseClient(
            events=[anchor],
            bridge_links=[bridge_link],
        )
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query_cross_chain(
            entity_id="0xaaa",
            anchor_event="bridge_out",
            window_before=timedelta(days=2),
            window_after=timedelta(days=2),
        )

        assert len(result) == 1


class TestRowToEvent:
    def test_basic_conversion(self):
        row = (
            "0xaaa",
            "wallet",
            "ev1",
            "swap",
            "transaction",
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "evm_base",
            "tx:0",
            "base",
            100,
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "0xdead",
            0,
            "uniswap_v3",
            "0xpool",
            "0xt1",
            "0xt2",
            Decimal("1.5"),
            Decimal("2.0"),
            None,
            None,
            None,
            "",
            None,
            None,
            '{"foo": "bar"}',
        )
        ev = _row_to_event(row)
        assert ev.entity_id == "0xaaa"
        assert ev.event_type == "swap"
        assert ev.extra == {"foo": "bar"}

    def test_null_extra(self):
        row = (
            "0xaaa",
            "wallet",
            "ev1",
            "swap",
            "transaction",
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "evm_base",
            "tx:0",
            "base",
            100,
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "0xdead",
            0,
            "",
            "",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "",
            None,
            None,
            None,
        )
        ev = _row_to_event(row)
        assert ev.extra is None


class TestValidation:
    def test_valid_field(self):
        assert _validate_field("event_type") == "event_type"

    def test_invalid_field_raises(self):
        with pytest.raises(ValueError, match="Invalid filter field"):
            _validate_field("hacked")

    def test_valid_op(self):
        assert _validate_op("=") == "="
        assert _validate_op(">=") == ">="
        assert _validate_op("like") == "LIKE"

    def test_invalid_op_raises(self):
        with pytest.raises(ValueError, match="Invalid filter operator"):
            _validate_op("; DROP TABLE")
