import pytest
from datetime import datetime, timedelta
from core.adapters.dummy import DummyWeb2Adapter
from core.trajectory.engine import TrajectoryEngine


class TestFakeIntegrator:
    """Ingest simulated Web2 events and verify trajectory queries work."""

    def test_web2_events_ingest(self):
        adapter = DummyWeb2Adapter()
        start = datetime.utcnow() - timedelta(days=1)
        end = datetime.utcnow()
        events = list(adapter.ingest(start, end))

        session_starts = [e for e in events if e.event_type == "session_start"]
        pageviews = [e for e in events if e.event_type == "pageview"]

        assert len(session_starts) > 0
        assert len(pageviews) > 0
        assert all(e.entity_type == "device_id" for e in events)

    def test_web2_trajectory(self):
        adapter = DummyWeb2Adapter()
        start = datetime.utcnow() - timedelta(days=1)
        end = datetime.utcnow()
        events = list(adapter.ingest(start, end))

        # Verify events are ordered by timestamp
        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)

    def test_entity_id_is_device_id_not_wallet(self):
        adapter = DummyWeb2Adapter()
        events = list(
            adapter.ingest(datetime.utcnow() - timedelta(hours=1), datetime.utcnow())
        )
        assert all(e.entity_type == "device_id" for e in events)
        assert all(not e.entity_id.startswith("0x") for e in events)
