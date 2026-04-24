"""Tests for ClickHouse sink — works without real ClickHouse (uses InMemorySink)."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from core.adapters.base import CanonicalEvent
from core.sink.clickhouse import ClickHouseSink, InMemorySink, SinkConfig, _event_to_row


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


class TestInMemorySink:
    def test_write_single_event(self):
        sink = InMemorySink()
        ev = _make_event()
        sink.write([ev])
        assert len(sink.events) == 1
        assert sink.events[0].event_id == "ev1"

    def test_write_batch(self):
        sink = InMemorySink()
        events = [_make_event(event_id=f"ev{i}") for i in range(5)]
        sink.write(events)
        assert len(sink.events) == 5
        assert sink.events[2].event_id == "ev2"

    def test_flush_is_noop(self):
        sink = InMemorySink()
        sink.write([_make_event()])
        assert sink.flush() == 0
        assert len(sink.events) == 1


class MockClient:
    def __init__(self):
        self.inserts: list[tuple[str, list]] = []

    def insert(self, table: str, data: list) -> None:
        self.inserts.append((table, data))

    def close(self) -> None:
        pass


class TestClickHouseSinkBuffering:
    def test_buffer_events_until_flush(self):
        sink = ClickHouseSink(SinkConfig(batch_size=100))
        events = [_make_event(event_id=f"ev{i}") for i in range(10)]
        written = sink.write(events)
        assert written == 0
        assert len(sink.buffered) == 10

    def test_auto_flush_when_batch_size_reached(self):
        mock = MockClient()
        sink = ClickHouseSink(SinkConfig(batch_size=5), client=mock)
        events = [_make_event(event_id=f"ev{i}") for i in range(12)]
        written = sink.write(events)
        assert written == 12
        assert len(sink.buffered) == 0
        assert len(mock.inserts) == 1

    def test_write_single(self):
        sink = ClickHouseSink(SinkConfig(batch_size=10))
        sink.write_single(_make_event())
        assert len(sink.buffered) == 1

    def test_flush_empty_buffer(self):
        sink = ClickHouseSink(SinkConfig(batch_size=10))
        assert sink.flush() == 0

    def test_flush_with_mock_client(self):
        mock = MockClient()
        sink = ClickHouseSink(SinkConfig(batch_size=10), client=mock)
        sink.write([_make_event(event_id="ev1")])
        assert sink.flush() == 1
        assert len(mock.inserts) == 1
        assert mock.inserts[0][0] == "canonical_events"


class TestEventToRow:
    def test_row_structure(self):
        ev = _make_event()
        row = _event_to_row(ev)
        assert row[0] == "0xaaa"
        assert row[1] == "wallet"
        assert row[2] == "ev1"
        assert row[3] == "swap"
        assert row[4] == "transaction"
        assert row[25] == '{"foo": "bar"}'

    def test_none_extra(self):
        ev = _make_event(extra=None)
        row = _event_to_row(ev)
        assert row[25] == "{}"

    def test_decimal_fields(self):
        ev = _make_event(amount_in=Decimal("1.5"), amount_out=Decimal("2.0"))
        row = _event_to_row(ev)
        assert row[17] == Decimal("1.5")
        assert row[18] == Decimal("2.0")
