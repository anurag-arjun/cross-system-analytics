import pytest


def test_core_imports_without_avail():
    """/core modules must import without /avail present."""
    from core.adapters.base import Adapter, CanonicalEvent
    from core.identity.graph import IdentityGraph
    from core.trajectory.engine import TrajectoryEngine
    from core.schemas.validator import validate_event

    assert Adapter is not None


def test_trajectory_engine_exists():
    """Trajectory engine must be instantiable with a mock client."""
    from core.trajectory.engine import TrajectoryEngine

    engine = TrajectoryEngine(None)
    assert engine is not None
