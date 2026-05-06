"""Tests for Dagster pipeline using mock resources."""

from unittest.mock import patch

from dagster import build_asset_context

from nexus_pipeline.assets import bridge_links, decoded_events, raw_logs
from nexus_pipeline.resources import (
    ClickHouseResource,
    EVMIngestionResource,
    PostgresResource,
)


class MockSink:
    def __init__(self):
        self.written = []

    def write(self, items):
        self.written.extend(items)

    def close(self):
        pass


class MockClickHouse(ClickHouseResource):
    def get_raw_log_sink(self, batch_size=1000):
        return MockSink()

    def get_event_sink(self, batch_size=1000):
        return MockSink()

    def get_bridge_link_sink(self, batch_size=100):
        return MockSink()


class MockPostgres(PostgresResource):
    enabled: bool = False


class MockEVM(EVMIngestionResource):
    lookback_minutes: int = 1


def test_raw_logs_asset():
    context = build_asset_context()
    clickhouse = MockClickHouse()
    evm = MockEVM()
    postgres = MockPostgres()

    with patch("nexus_pipeline.assets.CHAINS", []):
        result = raw_logs(context, clickhouse, evm, postgres)

    assert "raw_logs_ingested" in result
    assert result["raw_logs_ingested"] == 0


def test_decoded_events_asset():
    context = build_asset_context()
    clickhouse = MockClickHouse()
    evm = MockEVM()
    postgres = MockPostgres()

    with patch("nexus_pipeline.assets.CHAINS", []):
        result = decoded_events(context, clickhouse, evm, postgres)

    assert "decoded_events" in result
    assert "bridge_outs" in result
    assert result["decoded_events"] == 0


def test_bridge_links_asset():
    context = build_asset_context()
    clickhouse = MockClickHouse()
    evm = MockEVM()
    postgres = MockPostgres()

    decoded = {"decoded_events": 0, "bridge_outs": []}
    with patch("nexus_pipeline.assets.CHAINS", []):
        result = bridge_links(context, clickhouse, evm, postgres, decoded)
    assert "matched" in result
    assert "pending" in result
    assert result["matched"] == 0
    assert result["pending"] == 0
