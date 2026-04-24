from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from core.adapters.base import CanonicalEvent
from core.identity.graph import IdentityGraph
from core.trajectory.engine import TrajectoryEngine


@dataclass
class UnifiedFunnelResult:
    entity_id: str
    steps: list[CanonicalEvent]
    time_to_bridge: timedelta | None
    time_to_swap: timedelta | None
    converted: bool


def build_unified_funnel(
    clickhouse_client=None,
    identity_graph: IdentityGraph | None = None,
) -> UnifiedFunnelResult | None:
    from core.tests.standalone.test_trajectory_engine import MockClickHouseClient

    base_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    client_id = "ga4_client_abc123"
    wallet = "0xaaa"

    if identity_graph is not None:
        identity_graph.add_relationship(
            from_entity=client_id,
            from_type="device_id",
            to_entity=wallet,
            to_type="wallet",
            relationship_type="linked_via_signup",
            confidence=0.95,
            source="fastbridge",
        )

    if identity_graph is not None:
        query_entity = wallet
        event_entity = wallet
    else:
        query_entity = wallet
        event_entity = wallet

    events = [
        CanonicalEvent(
            entity_id=event_entity,
            entity_type="wallet",
            event_id="sess_1",
            event_type="session_start",
            event_category="acquisition",
            timestamp=base_time,
            source_system="ga4",
            source_event_id="ga4:sess_1",
            extra={"session_id": "sess_1", "utm_source": "google", "original_client_id": client_id},
        ),
        CanonicalEvent(
            entity_id=event_entity,
            entity_type="wallet",
            event_id="pv_1",
            event_type="pageview",
            event_category="acquisition",
            timestamp=base_time + timedelta(minutes=2),
            source_system="ga4",
            source_event_id="ga4:pv_1",
            extra={"url": "/landing", "session_id": "sess_1"},
        ),
        CanonicalEvent(
            entity_id=event_entity,
            entity_type="wallet",
            event_id="pv_2",
            event_type="pageview",
            event_category="acquisition",
            timestamp=base_time + timedelta(minutes=5),
            source_system="ga4",
            source_event_id="ga4:pv_2",
            extra={"url": "/bridge", "session_id": "sess_1"},
        ),
        CanonicalEvent(
            entity_id=wallet,
            entity_type="wallet",
            event_id="bridge_1",
            event_type="bridge_out",
            event_category="transaction",
            timestamp=base_time + timedelta(minutes=15),
            source_system="evm_base",
            source_event_id="tx:bridge1",
            chain="base",
            protocol="across",
            token_out="0xeth",
            amount_out=Decimal("1.5"),
            link_key="dep_123",
            link_key_type="across_deposit_id",
            extra={"destination_chain": "arbitrum"},
        ),
        CanonicalEvent(
            entity_id=wallet,
            entity_type="wallet",
            event_id="swap_1",
            event_type="swap",
            event_category="transaction",
            timestamp=base_time + timedelta(minutes=25),
            source_system="evm_arbitrum",
            source_event_id="tx:swap1",
            chain="arbitrum",
            protocol="uniswap_v3",
            venue="0xpool",
            token_in="0xeth",
            token_out="0xusdc",
            amount_in=Decimal("1.5"),
            amount_out=Decimal("3000"),
            extra={"slippage": "0.5%"},
        ),
    ]

    bridge_links = [
        (
            "dep_123",
            "arbitrum",
            wallet,
            base_time + timedelta(minutes=20),
            wallet,
        ),
    ]

    client = MockClickHouseClient(events=events, bridge_links=bridge_links)
    engine = TrajectoryEngine(clickhouse_client=client, identity_graph=identity_graph)

    result = engine.query_cross_chain(
        entity_id=query_entity,
        anchor_event="session_start",
        window_before=timedelta(minutes=10),
        window_after=timedelta(minutes=30),
        entity_type="wallet",
    )

    if not result:
        return None

    session_event = next((e for e in result if e.event_type == "session_start"), None)
    bridge_event = next((e for e in result if e.event_type == "bridge_out"), None)
    swap_event = next((e for e in result if e.event_type == "swap"), None)

    time_to_bridge = None
    time_to_swap = None
    if session_event and bridge_event:
        time_to_bridge = bridge_event.timestamp - session_event.timestamp
    if bridge_event and swap_event:
        time_to_swap = swap_event.timestamp - bridge_event.timestamp

    return UnifiedFunnelResult(
        entity_id=wallet,
        steps=result,
        time_to_bridge=time_to_bridge,
        time_to_swap=time_to_swap,
        converted=swap_event is not None,
    )
