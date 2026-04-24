"""Dagster assets for Nexus Analytics pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dagster import asset

from core.adapters.evm.multi import ChainConfig
from core.identity.bridge_links import BridgeLinkEngine

from .resources import ClickHouseResource, EVMIngestionResource


CHAINS = [
    ChainConfig("base", rpc_url="https://mainnet.base.org", page_size=200),
    ChainConfig(
        "ethereum", rpc_url="https://ethereum-rpc.publicnode.com", page_size=200
    ),
    ChainConfig(
        "arbitrum", rpc_url="https://arbitrum-one-rpc.publicnode.com", page_size=200
    ),
    ChainConfig(
        "optimism", rpc_url="https://optimism-rpc.publicnode.com", page_size=200
    ),
]


@asset
def raw_logs(
    context,
    clickhouse: ClickHouseResource,
    evm: EVMIngestionResource,
) -> dict:
    """Fetch raw EVM logs from all chains and store in canonical_logs."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=evm.lookback_minutes)

    adapter = evm.get_adapter(CHAINS)
    sink = clickhouse.get_raw_log_sink(batch_size=1000)

    total = 0
    try:
        for chain_name, chain_adapter in adapter.adapters.items():
            logs = list(chain_adapter.ingest_raw(start, end))
            if logs:
                sink.write(logs)
                total += len(logs)
                context.log.info(f"Fetched {len(logs)} raw logs from {chain_name}")
    finally:
        adapter.close()
        sink.close()

    return {"raw_logs_ingested": total}


@asset(deps=[raw_logs])
def decoded_events(
    context,
    clickhouse: ClickHouseResource,
    evm: EVMIngestionResource,
) -> dict:
    """Decode raw logs into canonical_events."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=evm.lookback_minutes)

    adapter = evm.get_adapter(CHAINS)
    sink = clickhouse.get_event_sink(batch_size=1000)
    bridge_engine = evm.get_bridge_engine()

    total_decoded = 0
    bridge_outs = []

    try:
        for chain_name, chain_adapter in adapter.adapters.items():
            raw_logs = list(chain_adapter.ingest_raw(start, end))
            events = list(chain_adapter.decode_logs(raw_logs))

            if events:
                sink.write(events)
                total_decoded += len(events)
                context.log.info(f"Decoded {len(events)} events from {chain_name}")

            for ev in events:
                if ev.event_type == "bridge_out":
                    bridge_outs.append(
                        {
                            "event_type": ev.event_type,
                            "link_key": ev.link_key,
                            "link_key_type": ev.link_key_type,
                            "chain": ev.chain,
                            "timestamp": ev.timestamp,
                            "tx_hash": ev.tx_hash,
                            "entity_id": ev.entity_id,
                            "event_id": ev.event_id,
                            "token_out": ev.token_out,
                            "amount_out": ev.amount_out,
                        }
                    )
    finally:
        adapter.close()
        sink.close()

    # Store bridge_outs for next step
    context.add_output_metadata({"bridge_outs": len(bridge_outs)})
    return {"decoded_events": total_decoded, "bridge_outs": bridge_outs}


@asset(deps=[decoded_events])
def bridge_links(
    context,
    clickhouse: ClickHouseResource,
    evm: EVMIngestionResource,
    decoded_events: dict,
) -> dict:
    """Match bridge_out events with bridge_in events across chains."""
    bridge_outs = decoded_events.get("bridge_outs", [])
    if not bridge_outs:
        return {"matched": 0, "pending": 0}

    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=evm.lookback_minutes * 2)

    adapter = evm.get_adapter(CHAINS)
    bridge_engine = BridgeLinkEngine()
    sink = clickhouse.get_bridge_link_sink(batch_size=100)

    matched = 0
    try:
        for ev_out in bridge_outs:
            bridge_engine.add_bridge_out(ev_out)

        for chain_name, chain_adapter in adapter.adapters.items():
            raw_logs = list(chain_adapter.ingest_raw(start, end))
            events = list(chain_adapter.decode_logs(raw_logs))

            for ev in events:
                if ev.event_type == "bridge_in":
                    link = bridge_engine.add_bridge_in(
                        {
                            "event_type": ev.event_type,
                            "link_key": ev.link_key,
                            "link_key_type": ev.link_key_type,
                            "chain": ev.chain,
                            "timestamp": ev.timestamp,
                            "tx_hash": ev.tx_hash,
                            "entity_id": ev.entity_id,
                            "event_id": ev.event_id,
                        }
                    )
                    if link:
                        sink.write([link])
                        matched += 1

        sink.close()
    finally:
        adapter.close()

    pending = len(bridge_engine.get_pending())
    context.log.info(f"Matched {matched} bridge links, {pending} pending")
    return {"matched": matched, "pending": pending}
