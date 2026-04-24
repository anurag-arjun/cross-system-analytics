from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from typing import Iterator

from core.adapters.base import Adapter, CanonicalEvent


class DummyWeb2Adapter(Adapter):
    @property
    def source_system(self) -> str:
        return "dummy"

    def __init__(
        self,
        num_clients: int = 5,
        sessions_per_client: int = 3,
        pageviews_per_session: tuple[int, int] = (2, 6),
        seed: int | None = 42,
    ) -> None:
        self.num_clients = num_clients
        self.sessions_per_client = sessions_per_client
        self.pageviews_per_session = pageviews_per_session
        self.rng = random.Random(seed)

    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]:
        duration = end - start
        for client_idx in range(self.num_clients):
            client_id = f"dummy_client_{self.rng.randint(1000, 9999)}"
            for session_idx in range(self.sessions_per_client):
                session_offset = self.rng.random() * duration
                session_start = start + session_offset
                if session_start >= end:
                    continue
                session_id = f"sess_{client_idx}_{session_idx}_{uuid.uuid4().hex[:6]}"
                yield self._session_start(client_id, session_id, session_start)

                num_pvs = self.rng.randint(*self.pageviews_per_session)
                urls = self._pick_urls(num_pvs)
                for pv_idx, url in enumerate(urls):
                    pv_time = session_start + timedelta(minutes=1 + pv_idx * 3)
                    if pv_time >= end:
                        break
                    yield self._pageview(
                        client_id,
                        session_id,
                        pv_time,
                        url,
                        pv_idx,
                    )

    def _session_start(
        self,
        client_id: str,
        session_id: str,
        timestamp: datetime,
    ) -> CanonicalEvent:
        has_utm = self.rng.random() < 0.4
        extra: dict[str, str] = {
            "client_id": client_id,
            "session_id": session_id,
        }
        if has_utm:
            sources = ["google", "twitter", "newsletter", "direct"]
            mediums = ["organic", "cpc", "social", "email"]
            extra["utm_source"] = self.rng.choice(sources)
            extra["utm_medium"] = self.rng.choice(mediums)

        return CanonicalEvent(
            entity_id=client_id,
            entity_type="device_id",
            event_id=f"dummy_{uuid.uuid4().hex}",
            event_type="session_start",
            event_category="acquisition",
            timestamp=timestamp,
            source_system="dummy",
            source_event_id=session_id,
            extra=extra,
        )

    def _pageview(
        self,
        client_id: str,
        session_id: str,
        timestamp: datetime,
        url: str,
        pv_index: int,
    ) -> CanonicalEvent:
        return CanonicalEvent(
            entity_id=client_id,
            entity_type="device_id",
            event_id=f"dummy_{uuid.uuid4().hex}",
            event_type="pageview",
            event_category="acquisition",
            timestamp=timestamp,
            source_system="dummy",
            source_event_id=f"pv_{session_id}_{pv_index}",
            extra={"url": url, "session_id": session_id},
        )

    def _pick_urls(self, count: int) -> list[str]:
        pages = [
            "/",
            "/landing",
            "/pricing",
            "/docs",
            "/docs/getting-started",
            "/docs/api",
            "/blog",
            "/about",
            "/contact",
            "/signup",
            "/login",
        ]
        if count <= len(pages):
            return self.rng.sample(pages, count)
        return self.rng.choices(pages, k=count)
