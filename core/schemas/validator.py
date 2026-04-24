"""Schema validation for canonical events against registry.yaml."""

import yaml
from pathlib import Path
from core.adapters.base import CanonicalEvent


_REGISTRY = None


def _load_registry() -> dict:
    global _REGISTRY
    if _REGISTRY is None:
        registry_path = Path(__file__).parent.parent / "schemas" / "registry.yaml"
        with open(registry_path) as f:
            _REGISTRY = yaml.safe_load(f)
    return _REGISTRY


def validate_event(event: CanonicalEvent) -> bool:
    """Validate a canonical event against the schema registry."""
    registry = _load_registry()
    event_spec = registry.get("event_types", {}).get(event.event_type)
    if event_spec is None:
        return False
    # TODO: Validate required properties in event.extra
    return True
