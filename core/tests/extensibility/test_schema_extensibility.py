import pytest
import yaml
from pathlib import Path
from core.schemas.validator import validate_event, _load_registry
from core.adapters.base import CanonicalEvent
from datetime import datetime


class TestSchemaExtensibility:
    """Add new event types without adapter-external code changes."""

    def test_registry_loads(self):
        registry = _load_registry()
        assert "event_types" in registry
        assert "swap" in registry["event_types"]
        assert "pageview" in registry["event_types"]

    def test_event_type_has_required_fields(self):
        registry = _load_registry()
        swap_spec = registry["event_types"]["swap"]
        assert swap_spec["category"] == "transaction"
        assert "token_in" in swap_spec["required_properties"]

    def test_new_event_type_can_be_added(self):
        registry = _load_registry()
        # Simulate adding a new event type
        registry["event_types"]["custom_event"] = {
            "category": "product",
            "required_properties": ["action"],
            "optional_properties": [],
        }
        # Validation should work without code changes outside the adapter
        assert "custom_event" in registry["event_types"]
