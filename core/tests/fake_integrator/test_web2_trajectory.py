import pytest
from datetime import datetime, timedelta, timezone

from core.adapters.dummy import DummyWeb2Adapter
from core.trajectory.engine import TrajectoryEngine


class TestFakeIntegrator:
    def test_web2_events_ingest(self):
        adapter = DummyWeb2Adapter(num_clients=3, sessions_per_client=2)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        session_starts = [e for e in events if e.event_type == "session_start"]
        pageviews = [e for e in events if e.event_type == "pageview"]

        assert len(session_starts) > 0
        assert len(pageviews) > 0
        assert all(e.entity_type == "device_id" for e in events)
        assert all(e.source_system == "dummy" for e in events)

    def test_web2_trajectory_ordered(self):
        adapter = DummyWeb2Adapter(num_clients=2, sessions_per_client=2)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)

    def test_entity_id_is_device_id_not_wallet(self):
        adapter = DummyWeb2Adapter(num_clients=2)
        events = list(
            adapter.ingest(
                datetime.now(timezone.utc) - timedelta(hours=1),
                datetime.now(timezone.utc),
            )
        )
        assert all(e.entity_type == "device_id" for e in events)
        assert all(not e.entity_id.startswith("0x") for e in events)

    def test_multiple_clients(self):
        adapter = DummyWeb2Adapter(num_clients=5, sessions_per_client=2)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        client_ids = {e.entity_id for e in events}
        assert len(client_ids) <= 5
        assert len(client_ids) > 1

    def test_multiple_sessions_per_client(self):
        adapter = DummyWeb2Adapter(num_clients=2, sessions_per_client=4)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        session_starts = [e for e in events if e.event_type == "session_start"]
        assert len(session_starts) >= 2

    def test_session_has_multiple_pageviews(self):
        adapter = DummyWeb2Adapter(
            num_clients=1,
            sessions_per_client=1,
            pageviews_per_session=(3, 5),
        )
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        pageviews = [e for e in events if e.event_type == "pageview"]
        assert len(pageviews) >= 3

    def test_pageviews_have_urls(self):
        adapter = DummyWeb2Adapter(num_clients=1, sessions_per_client=1)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        pageviews = [e for e in events if e.event_type == "pageview"]
        assert all("url" in (e.extra or {}) for e in pageviews)
        assert all(e.extra["url"].startswith("/") for e in pageviews)

    def test_some_sessions_have_utm(self):
        adapter = DummyWeb2Adapter(num_clients=10, sessions_per_client=5, seed=42)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        sessions_with_utm = [
            e for e in events if e.event_type == "session_start" and "utm_source" in (e.extra or {})
        ]
        assert len(sessions_with_utm) > 0

    def test_trajectory_query_on_device_id(self):
        from core.tests.standalone.test_trajectory_engine import MockClickHouseClient

        adapter = DummyWeb2Adapter(num_clients=1, sessions_per_client=1)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        client_id = events[0].entity_id
        client = MockClickHouseClient(events=events)
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query(
            entity_id=client_id,
            anchor_event="session_start",
            window_before=timedelta(hours=12),
            window_after=timedelta(hours=12),
            entity_type="device_id",
        )

        assert len(result) > 0
        assert all(e.entity_id == client_id for e in result)

    def test_cross_system_trajectory_simulation(self):
        from core.tests.standalone.test_trajectory_engine import MockClickHouseClient
        from core.adapters.base import CanonicalEvent

        adapter = DummyWeb2Adapter(num_clients=1, sessions_per_client=1)
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc)
        events = list(adapter.ingest(start, end))

        client_id = events[0].entity_id

        bridge_event = CanonicalEvent(
            entity_id=client_id,
            entity_type="device_id",
            event_id="bridge_1",
            event_type="bridge_out",
            event_category="transaction",
            timestamp=events[0].timestamp + timedelta(minutes=30),
            source_system="evm_base",
            source_event_id="tx:1",
            chain="base",
            protocol="across",
            link_key="dep_123",
            link_key_type="across_deposit_id",
            extra={"destination_chain": "ethereum"},
        )

        all_events = events + [bridge_event]
        client = MockClickHouseClient(events=all_events)
        engine = TrajectoryEngine(clickhouse_client=client)
        result = engine.query(
            entity_id=client_id,
            anchor_event="session_start",
            window_before=timedelta(hours=12),
            window_after=timedelta(hours=12),
            entity_type="device_id",
        )

        event_types = [e.event_type for e in result]
        assert "session_start" in event_types
        assert "pageview" in event_types
        assert "bridge_out" in event_types
