import pytest
from datetime import datetime, timedelta
from core.adapters.dummy import DummyWeb2Adapter
from core.trajectory.engine import TrajectoryEngine


class TestStandalone:
    """Test that /core runs without /avail dependencies."""

    def test_core_imports_no_avail(self):
        # Should not import anything from /avail
        import core

        assert hasattr(core, "adapters")
        assert hasattr(core, "trajectory")
        assert hasattr(core, "identity")

    def test_dummy_adapter_runs(self):
        adapter = DummyWeb2Adapter()
        events = list(
            adapter.ingest(datetime.utcnow() - timedelta(days=1), datetime.utcnow())
        )
        assert len(events) > 0
        assert all(e.source_system == "dummy" for e in events)

    def test_trajectory_query_runs(self):
        # With a mock ClickHouse client
        engine = TrajectoryEngine(None)
        result = engine.query(
            entity_id="test_123",
            anchor_event="bridge_out",
            window_before=timedelta(days=7),
            window_after=timedelta(days=7),
        )
        assert isinstance(result, list)
