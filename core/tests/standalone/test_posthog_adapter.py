import json
from datetime import datetime, timezone

import pytest

from core.adapters.posthog import (
    PostHogAdapter,
    _extract_cursor,
    _map_event_type,
    _parse_timestamp,
)


class MockClient:
    def __init__(self, responses: list[dict]):
        self.responses = responses
        self.call_index = 0
        self.requests: list[dict] = []

    def get(self, url: str, params: dict = None):
        self.requests.append({"url": url, "params": params})
        resp_data = self.responses[self.call_index]
        self.call_index += 1
        return MockResponse(resp_data)


class MockResponse:
    def __init__(self, data: dict):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class TestPostHogAdapter:
    def test_source_system(self):
        adapter = PostHogAdapter(api_key="test", project_id="123")
        assert adapter.source_system == "posthog"

    def test_ingest_pageview(self, monkeypatch):
        adapter = PostHogAdapter(api_key="test", project_id="123")

        resp = {
            "results": [
                {
                    "distinct_id": "user_123",
                    "event": "$pageview",
                    "timestamp": "2024-01-15T12:00:00.000000Z",
                    "properties": {
                        "$current_url": "https://example.com/landing",
                        "$referrer": "https://google.com",
                        "$session_id": "sess_abc",
                        "utm_source": "google",
                        "utm_medium": "organic",
                    },
                }
            ],
            "next": None,
        }

        mock_client = MockClient([resp])
        monkeypatch.setattr(adapter, "_client", mock_client)

        events = list(
            adapter.ingest(
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 20, 0, 0, 0, tzinfo=timezone.utc),
            )
        )

        assert len(events) == 1
        ev = events[0]
        assert ev.entity_id == "user_123"
        assert ev.entity_type == "device_id"
        assert ev.event_type == "pageview"
        assert ev.event_category == "acquisition"
        assert ev.source_system == "posthog"
        assert ev.timestamp == datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert ev.extra["session_id"] == "sess_abc"
        assert ev.extra["url"] == "https://example.com/landing"
        assert ev.extra["referrer"] == "https://google.com"
        assert ev.extra["utm_source"] == "google"
        assert ev.extra["utm_medium"] == "organic"

    def test_ingest_session_start(self, monkeypatch):
        adapter = PostHogAdapter(api_key="test", project_id="123")

        resp = {
            "results": [
                {
                    "distinct_id": "user_123",
                    "event": "$session_start",
                    "timestamp": "2024-01-15T12:00:00.000000Z",
                    "properties": {"$session_id": "sess_abc"},
                }
            ],
            "next": None,
        }

        mock_client = MockClient([resp])
        monkeypatch.setattr(adapter, "_client", mock_client)

        events = list(
            adapter.ingest(
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 20, 0, 0, 0, tzinfo=timezone.utc),
            )
        )

        assert len(events) == 1
        assert events[0].event_type == "session_start"

    def test_ingest_filters_unknown_events(self, monkeypatch):
        adapter = PostHogAdapter(api_key="test", project_id="123")

        resp = {
            "results": [
                {
                    "distinct_id": "user_123",
                    "event": "$pageleave",
                    "timestamp": "2024-01-15T12:00:00.000000Z",
                    "properties": {},
                }
            ],
            "next": None,
        }

        mock_client = MockClient([resp])
        monkeypatch.setattr(adapter, "_client", mock_client)

        events = list(
            adapter.ingest(
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 20, 0, 0, 0, tzinfo=timezone.utc),
            )
        )

        assert len(events) == 0

    def test_ingest_pagination(self, monkeypatch):
        adapter = PostHogAdapter(api_key="test", project_id="123")

        resp1 = {
            "results": [
                {
                    "distinct_id": "user_1",
                    "event": "$pageview",
                    "timestamp": "2024-01-15T12:00:00.000000Z",
                    "properties": {},
                }
            ],
            "next": "https://app.posthog.com/api/projects/123/events?after=cursor_abc",
        }

        resp2 = {
            "results": [
                {
                    "distinct_id": "user_2",
                    "event": "$pageview",
                    "timestamp": "2024-01-15T13:00:00.000000Z",
                    "properties": {},
                }
            ],
            "next": None,
        }

        mock_client = MockClient([resp1, resp2])
        monkeypatch.setattr(adapter, "_client", mock_client)

        events = list(
            adapter.ingest(
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 20, 0, 0, 0, tzinfo=timezone.utc),
            )
        )

        assert len(events) == 2
        assert events[0].entity_id == "user_1"
        assert events[1].entity_id == "user_2"

        assert len(mock_client.requests) == 2
        assert mock_client.requests[1]["params"]["after"] == "cursor_abc"

    def test_request_structure(self, monkeypatch):
        adapter = PostHogAdapter(api_key="test", project_id="123")

        resp = {"results": [], "next": None}
        mock_client = MockClient([resp])
        monkeypatch.setattr(adapter, "_client", mock_client)

        list(
            adapter.ingest(
                datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 6, 5, 0, 0, 0, tzinfo=timezone.utc),
            )
        )

        req = mock_client.requests[0]
        assert "projects/123/events" in req["url"]
        assert req["params"]["limit"] == 100
        assert "2024-06-01" in req["params"]["after"]
        assert "2024-06-05" in req["params"]["before"]

    def test_missing_distinct_id_skipped(self, monkeypatch):
        adapter = PostHogAdapter(api_key="test", project_id="123")

        resp = {
            "results": [
                {
                    "distinct_id": "",
                    "event": "$pageview",
                    "timestamp": "2024-01-15T12:00:00.000000Z",
                    "properties": {},
                }
            ],
            "next": None,
        }

        mock_client = MockClient([resp])
        monkeypatch.setattr(adapter, "_client", mock_client)

        events = list(
            adapter.ingest(
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 20, 0, 0, 0, tzinfo=timezone.utc),
            )
        )

        assert len(events) == 0


class TestHelpers:
    def test_map_event_type_pageview(self):
        assert _map_event_type("$pageview") == "pageview"

    def test_map_event_type_session_start(self):
        assert _map_event_type("$session_start") == "session_start"

    def test_map_event_type_unknown(self):
        assert _map_event_type("$pageleave") is None
        assert _map_event_type("custom_event") is None

    def test_parse_timestamp_iso(self):
        dt = _parse_timestamp("2024-01-15T12:30:00.000000Z")
        assert dt == datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)

    def test_parse_timestamp_offset(self):
        dt = _parse_timestamp("2024-01-15T12:30:00+00:00")
        assert dt == datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)

    def test_parse_timestamp_invalid(self):
        assert _parse_timestamp("") is None
        assert _parse_timestamp("invalid") is None

    def test_extract_cursor(self):
        url = "https://app.posthog.com/api/projects/123/events?after=cursor_123"
        assert _extract_cursor(url) == "cursor_123"

    def test_extract_cursor_no_cursor(self):
        url = "https://app.posthog.com/api/projects/123/events"
        assert _extract_cursor(url) is None
