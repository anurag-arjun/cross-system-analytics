"""Dummy Web2 adapter for testing cross-system trajectories."""

from datetime import datetime, timedelta
from typing import Iterator
import uuid

from core.adapters.base import Adapter, CanonicalEvent


class DummyWeb2Adapter(Adapter):
    @property
    def source_system(self) -> str:
        return "dummy"

    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]:
        """Generate simulated pageviews and sessions for testing."""
        current = start
        session_id = str(uuid.uuid4())
        client_id = f"dummy_client_{uuid.uuid4().hex[:8]}"

        while current < end:
            yield CanonicalEvent(
                entity_id=client_id,
                entity_type="device_id",
                event_id=f"dummy_{uuid.uuid4().hex}",
                event_type="session_start",
                event_category="acquisition",
                timestamp=current,
                source_system="dummy",
                source_event_id=session_id,
                extra={"client_id": client_id, "session_id": session_id},
            )

            yield CanonicalEvent(
                entity_id=client_id,
                entity_type="device_id",
                event_id=f"dummy_{uuid.uuid4().hex}",
                event_type="pageview",
                event_category="acquisition",
                timestamp=current + timedelta(minutes=1),
                source_system="dummy",
                source_event_id=f"pv_{uuid.uuid4().hex}",
                extra={"url": "/landing", "session_id": session_id},
            )

            current += timedelta(hours=1)
