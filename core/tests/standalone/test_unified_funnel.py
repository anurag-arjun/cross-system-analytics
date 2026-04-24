import pytest
from datetime import datetime, timedelta, timezone

from core.adapters.base import CanonicalEvent
from core.demo.unified_funnel import UnifiedFunnelResult, build_unified_funnel
from core.identity.graph import IdentityGraph
from core.tests.standalone.test_trajectory_engine import MockClickHouseClient


class TestUnifiedFunnel:
    def test_funnel_with_identity_graph(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        assert result is not None
        assert result.converted is True
        assert len(result.steps) == 5

        event_types = [e.event_type for e in result.steps]
        assert event_types == [
            "session_start",
            "pageview",
            "pageview",
            "bridge_out",
            "swap",
        ]

    def test_funnel_without_identity_graph(self):
        result = build_unified_funnel(identity_graph=None)

        assert result is not None
        assert result.converted is True
        assert len(result.steps) == 5

        event_types = [e.event_type for e in result.steps]
        assert "session_start" in event_types
        assert "bridge_out" in event_types
        assert "swap" in event_types

    def test_time_to_bridge_calculated(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        assert result.time_to_bridge is not None
        assert result.time_to_bridge == timedelta(minutes=15)

    def test_time_to_swap_calculated(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        assert result.time_to_swap is not None
        assert result.time_to_swap == timedelta(minutes=10)

    def test_entity_id_resolved(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        assert result.entity_id == "0xaaa"

    def test_ga4_events_present(self):
        graph = IdentityGraph()
        result = build_unified_funnel(identity_graph=graph)

        ga4_events = [e for e in result.steps if e.source_system == "ga4"]
        assert len(ga4_events) == 3
        assert all(e.entity_type == "wallet" for e in ga4_events)

    def test_evm_events_present(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        evm_events = [e for e in result.steps if e.source_system.startswith("evm_")]
        assert len(evm_events) == 2
        assert all(e.entity_type == "wallet" for e in evm_events)

    def test_bridge_link_cross_chain(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        chains = {e.chain for e in result.steps if e.chain}
        assert "base" in chains
        assert "arbitrum" in chains

    def test_utms_preserved(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        session = next(e for e in result.steps if e.event_type == "session_start")
        assert session.extra["utm_source"] == "google"

    def test_swap_amounts_present(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        swap = next(e for e in result.steps if e.event_type == "swap")
        assert swap.amount_in == 1.5
        assert swap.amount_out == 3000

    def test_event_ordering(self):
        graph = IdentityGraph()
        result = build_unified_funnel(None, identity_graph=graph)

        timestamps = [e.timestamp for e in result.steps]
        assert timestamps == sorted(timestamps)

    def test_result_dataclass(self):
        from decimal import Decimal

        steps = [
            CanonicalEvent(
                entity_id="test",
                entity_type="wallet",
                event_id="e1",
                event_type="swap",
                event_category="transaction",
                timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                source_system="evm_base",
                source_event_id="tx:1",
            )
        ]
        result = UnifiedFunnelResult(
            entity_id="test",
            steps=steps,
            time_to_bridge=timedelta(minutes=5),
            time_to_swap=timedelta(minutes=10),
            converted=True,
        )

        assert result.entity_id == "test"
        assert result.converted is True
        assert result.time_to_bridge == timedelta(minutes=5)
        assert result.time_to_swap == timedelta(minutes=10)
