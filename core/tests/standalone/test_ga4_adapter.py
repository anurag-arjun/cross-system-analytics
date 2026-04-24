import json
import os
import tempfile
from datetime import datetime, timezone

import pytest

from core.adapters.ga4 import GA4Adapter, _map_event_type, _parse_date_hour_minute


class FakeCredentials:
    def __init__(self, token="fake_token"):
        self.token = token
        self.expired = False

    def refresh(self, request):
        pass


class MockClient:
    def __init__(self, responses: list[dict]):
        self.responses = responses
        self.call_index = 0
        self.requests: list[dict] = []

    def post(self, url: str, headers: dict = None, json: dict = None):
        self.requests.append({"url": url, "headers": headers, "json": json})
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


def _make_credentials_file() -> str:
    creds = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "abc123",
        "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBALRiMLAH\n-----END RSA PRIVATE KEY-----",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "client_id": "123456",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(creds, f)
    return path


class TestGA4Adapter:
    def test_source_system(self):
        path = _make_credentials_file()
        try:
            adapter = GA4Adapter(property_id="123", credentials_path=path)
            assert adapter.source_system == "ga4"
        finally:
            os.remove(path)

    def test_ingest_pageview(self, monkeypatch):
        path = _make_credentials_file()
        try:
            adapter = GA4Adapter(property_id="123", credentials_path=path)

            fake_creds = FakeCredentials()
            monkeypatch.setattr(adapter, "_credentials", fake_creds)

            resp = {
                "dimensionHeaders": [
                    {"name": "dateHourMinute"},
                    {"name": "eventName"},
                    {"name": "sessionId"},
                    {"name": "clientId"},
                    {"name": "pageLocation"},
                    {"name": "pageReferrer"},
                    {"name": "sessionSource"},
                    {"name": "sessionMedium"},
                ],
                "metricHeaders": [{"name": "eventCount"}],
                "rows": [
                    {
                        "dimensionValues": [
                            {"value": "202401011200"},
                            {"value": "page_view"},
                            {"value": "sess_abc"},
                            {"value": "client_123"},
                            {"value": "https://example.com/landing"},
                            {"value": "https://google.com"},
                            {"value": "google"},
                            {"value": "organic"},
                        ],
                        "metricValues": [{"value": "1"}],
                    }
                ],
            }

            mock_client = MockClient([resp])
            monkeypatch.setattr(adapter, "_client", mock_client)

            events = list(
                adapter.ingest(
                    datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                )
            )

            assert len(events) == 1
            ev = events[0]
            assert ev.entity_id == "client_123"
            assert ev.entity_type == "device_id"
            assert ev.event_type == "pageview"
            assert ev.event_category == "acquisition"
            assert ev.source_system == "ga4"
            assert ev.timestamp == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            assert ev.extra["session_id"] == "sess_abc"
            assert ev.extra["url"] == "https://example.com/landing"
            assert ev.extra["referrer"] == "https://google.com"
            assert ev.extra["utm_source"] == "google"
            assert ev.extra["utm_medium"] == "organic"
        finally:
            os.remove(path)

    def test_ingest_session_start(self, monkeypatch):
        path = _make_credentials_file()
        try:
            adapter = GA4Adapter(property_id="123", credentials_path=path)
            fake_creds = FakeCredentials()
            monkeypatch.setattr(adapter, "_credentials", fake_creds)

            resp = {
                "dimensionHeaders": [
                    {"name": "dateHourMinute"},
                    {"name": "eventName"},
                    {"name": "sessionId"},
                    {"name": "clientId"},
                ],
                "rows": [
                    {
                        "dimensionValues": [
                            {"value": "202401011200"},
                            {"value": "session_start"},
                            {"value": "sess_abc"},
                            {"value": "client_123"},
                        ],
                    }
                ],
            }

            mock_client = MockClient([resp])
            monkeypatch.setattr(adapter, "_client", mock_client)

            events = list(
                adapter.ingest(
                    datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                )
            )

            assert len(events) == 1
            assert events[0].event_type == "session_start"
        finally:
            os.remove(path)

    def test_ingest_filters_unknown_events(self, monkeypatch):
        path = _make_credentials_file()
        try:
            adapter = GA4Adapter(property_id="123", credentials_path=path)
            fake_creds = FakeCredentials()
            monkeypatch.setattr(adapter, "_credentials", fake_creds)

            resp = {
                "dimensionHeaders": [
                    {"name": "dateHourMinute"},
                    {"name": "eventName"},
                    {"name": "sessionId"},
                    {"name": "clientId"},
                ],
                "rows": [
                    {
                        "dimensionValues": [
                            {"value": "202401011200"},
                            {"value": "scroll"},
                            {"value": "sess_abc"},
                            {"value": "client_123"},
                        ],
                    }
                ],
            }

            mock_client = MockClient([resp])
            monkeypatch.setattr(adapter, "_client", mock_client)

            events = list(
                adapter.ingest(
                    datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                )
            )

            assert len(events) == 0
        finally:
            os.remove(path)

    def test_ingest_pagination(self, monkeypatch):
        path = _make_credentials_file()
        try:
            adapter = GA4Adapter(property_id="123", credentials_path=path)
            fake_creds = FakeCredentials()
            monkeypatch.setattr(adapter, "_credentials", fake_creds)

            resp1 = {
                "dimensionHeaders": [
                    {"name": "dateHourMinute"},
                    {"name": "eventName"},
                    {"name": "sessionId"},
                    {"name": "clientId"},
                ],
                "rowCount": "2",
                "rows": [
                    {
                        "dimensionValues": [
                            {"value": "202401011200"},
                            {"value": "page_view"},
                            {"value": "sess_1"},
                            {"value": "client_1"},
                        ],
                    }
                ],
            }

            resp2 = {
                "dimensionHeaders": [
                    {"name": "dateHourMinute"},
                    {"name": "eventName"},
                    {"name": "sessionId"},
                    {"name": "clientId"},
                ],
                "rowCount": "2",
                "rows": [
                    {
                        "dimensionValues": [
                            {"value": "202401011300"},
                            {"value": "page_view"},
                            {"value": "sess_2"},
                            {"value": "client_2"},
                        ],
                    }
                ],
            }

            mock_client = MockClient([resp1, resp2])
            monkeypatch.setattr(adapter, "_client", mock_client)

            events = list(
                adapter.ingest(
                    datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                )
            )

            assert len(events) == 2
            assert events[0].entity_id == "client_1"
            assert events[1].entity_id == "client_2"

            assert len(mock_client.requests) == 2
            assert mock_client.requests[1]["json"]["offset"] == "100000"
        finally:
            os.remove(path)

    def test_request_body_structure(self, monkeypatch):
        path = _make_credentials_file()
        try:
            adapter = GA4Adapter(property_id="123", credentials_path=path)
            fake_creds = FakeCredentials()
            monkeypatch.setattr(adapter, "_credentials", fake_creds)

            resp = {
                "dimensionHeaders": [{"name": "eventName"}, {"name": "clientId"}],
                "rows": [],
            }

            mock_client = MockClient([resp])
            monkeypatch.setattr(adapter, "_client", mock_client)

            list(
                adapter.ingest(
                    datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
                    datetime(2024, 6, 5, 0, 0, 0, tzinfo=timezone.utc),
                )
            )

            req = mock_client.requests[0]
            assert req["json"]["dateRanges"][0]["startDate"] == "2024-06-01"
            assert req["json"]["dateRanges"][0]["endDate"] == "2024-06-05"
            assert req["headers"]["Authorization"] == "Bearer fake_token"
        finally:
            os.remove(path)


class TestHelpers:
    def test_map_event_type_pageview(self):
        assert _map_event_type("page_view") == "pageview"
        assert _map_event_type("pageview") == "pageview"

    def test_map_event_type_session_start(self):
        assert _map_event_type("session_start") == "session_start"

    def test_map_event_type_unknown(self):
        assert _map_event_type("scroll") is None
        assert _map_event_type("click") is None

    def test_parse_date_hour_minute_valid(self):
        dt = _parse_date_hour_minute("202401011230")
        assert dt == datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)

    def test_parse_date_hour_minute_invalid(self):
        assert _parse_date_hour_minute("") is None
        assert _parse_date_hour_minute("short") is None
        assert _parse_date_hour_minute("202413011230") is None
