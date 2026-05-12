"""Dagster assets for Nexus Analytics pipeline."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from dagster import asset

from core.adapters.evm.multi import ChainConfig
from core.enrichment.prices import PriceFetcher
from core.identity.bridge_links import BridgeLinkEngine

from .resources import ClickHouseResource, EVMIngestionResource, PostgresResource


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
    ChainConfig(
        "polygon", rpc_url="https://polygon-bor-rpc.publicnode.com", page_size=200
    ),
]


@asset
def raw_logs(
    context,
    clickhouse: ClickHouseResource,
    evm: EVMIngestionResource,
    postgres: PostgresResource,
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


@asset
def decoded_events(
    context,
    clickhouse: ClickHouseResource,
    evm: EVMIngestionResource,
    postgres: PostgresResource,
) -> dict:
    """Decode raw logs into canonical_events."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=evm.lookback_minutes)

    adapter = evm.get_adapter(CHAINS)
    sink = clickhouse.get_event_sink(batch_size=1000)
    bridge_engine = evm.get_bridge_engine()

    total_decoded = 0
    bridge_outs = []

    pending_store = postgres.get_pending_bridge_store()

    try:
        for chain_name, chain_adapter in adapter.adapters.items():
            # `ingest()` pushes the topic0 filter to HyperSync's edge so we
            # only receive logs that have a registered decoder, then decodes
            # them inline. Replaces the old ingest_raw + decode_logs pair,
            # which re-fetched the whole window unfiltered (~70% of logs had
            # no decoder anyway).
            events = list(chain_adapter.ingest(start, end))

            if events:
                sink.write(events)
                total_decoded += len(events)
                context.log.info(f"Decoded {len(events)} events from {chain_name}")

            for ev in events:
                if ev.event_type == "bridge_out":
                    ev_dict = {
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
                    bridge_outs.append(ev_dict)
                    pending_store.add_pending(ev_dict)
    finally:
        adapter.close()
        sink.close()

    context.add_output_metadata({"bridge_outs": len(bridge_outs)})
    return {"decoded_events": total_decoded, "bridge_outs": bridge_outs}


@asset(deps=[decoded_events])
def bridge_links(
    context,
    clickhouse: ClickHouseResource,
    evm: EVMIngestionResource,
    postgres: PostgresResource,
    decoded_events: dict,
) -> dict:
    """Match bridge_out events with bridge_in events across chains."""
    pending_store = postgres.get_pending_bridge_store()

    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=evm.lookback_minutes * 2)

    adapter = evm.get_adapter(CHAINS)
    bridge_engine = evm.get_bridge_engine(store=pending_store)
    sink = clickhouse.get_bridge_link_sink(batch_size=100)

    matched = 0
    try:
        # Re-ingest and decode recent logs to find bridge_in events.
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
                            "amount": ev.amount_in or ev.amount_out,
                            "token": ev.token_in or ev.token_out,
                        }
                    )
                    if link:
                        sink.write([link])
                        matched += 1

        sink.close()
    finally:
        adapter.close()

    # Also clean up expired pending rows.
    deleted = pending_store.delete_expired(end - timedelta(days=30))
    if deleted:
        context.log.info(f"Deleted {deleted} expired pending bridge rows")

    stats = bridge_engine.stats()
    context.log.info(f"Matched {matched} bridge links, {stats['pending']} pending")
    return {"matched": matched, "pending": stats["pending"], "expired_deleted": deleted}


@asset(
    description="Hourly CoinGecko price refresh for all tracked tokens",
)
def token_prices(context, clickhouse: ClickHouseResource) -> dict:
    """Fetch current USD prices for all tokens in token_metadata from CoinGecko.

    Reads token_metadata to discover tracked tokens, groups them by chain,
    fetches prices via CoinGecko (with DexScreener fallback), and stores
    results in token_prices.
    """
    client = clickhouse.get_client()

    # Discover all tracked tokens from metadata
    result = client.query("""
        SELECT lower(token_address) AS addr, chain
        FROM token_metadata
        ORDER BY chain, addr
    """)

    if not result.result_rows:
        context.log.info("No tokens in token_metadata — skipping price refresh")
        return {"prices_fetched": 0, "chains": []}

    tokens_by_chain: dict[str, list[str]] = {}
    for row in result.result_rows:
        addr, chain = row
        tokens_by_chain.setdefault(chain, []).append(addr)

    context.log.info(
        f"Refreshing prices for {sum(len(v) for v in tokens_by_chain.values())} "
        f"tokens across {len(tokens_by_chain)} chains: "
        f"{', '.join(f'{c}({len(v)})' for c, v in tokens_by_chain.items())}"
    )

    # Fetch and store prices (CoinGecko primary, DexScreener fallback)
    fetcher = PriceFetcher(client=client)
    total = 0
    for chain, addresses in tokens_by_chain.items():
        count = fetcher.update_prices(chain, addresses)
        total += count
        context.log.info(f"  {chain}: fetched {count}/{len(addresses)} prices")
        if chain != list(tokens_by_chain.keys())[-1]:
            time.sleep(1.0)  # Gentle rate-limit courtesy for free tier

    return {"prices_fetched": total, "chains": list(tokens_by_chain.keys())}


@asset(
    description="Weekly parity check vs Dune dex.trades — surfaces decoder gaps and drift.",
)
def dune_parity(context, clickhouse: ClickHouseResource) -> dict:
    """Run the Dune parity validator for the last 24h across in-scope chains.

    Wraps `ops.validation.dune_parity.main()`. Output goes to
    `ops/validation/runs/{YYYY-MM-DD}.json`. Logs the per-status summary
    and counts of regression warnings.
    """
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    from ops.validation.dune_parity import (
        DEFAULT_CHAINS,
        annotate_with_baseline,
        diff,
        fetch_clickhouse_counts,
        fetch_dune_counts,
        load_baseline,
        summarise,
        warn_regressions,
        write_run,
    )

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=1)

    our = fetch_clickhouse_counts(
        host=clickhouse.host,
        port=clickhouse.port,
        username=clickhouse.username,
        password=clickhouse.password,
        database=clickhouse.database,
        chains=DEFAULT_CHAINS,
        start=start,
        end=end,
    )
    dune, dune_bytes = fetch_dune_counts(chains=DEFAULT_CHAINS, start=start, end=end)

    rows = diff(our, dune)
    runs_dir = Path(__file__).resolve().parents[2] / "validation" / "runs"
    baseline = load_baseline(runs_dir, before=end)
    annotate_with_baseline(rows, baseline)

    n_regress = warn_regressions(rows)
    summary = summarise(rows)
    path = write_run(
        rows,
        runs_dir=runs_dir,
        window_start=start,
        window_end=end,
        chains=DEFAULT_CHAINS,
        dune_bytes_scanned=dune_bytes,
    )

    context.log.info(
        f"parity summary: {summary} | regressions={n_regress} | "
        f"dune_bytes={dune_bytes:,} | wrote {path.name}"
    )
    return {
        "summary": summary,
        "regressions": n_regress,
        "dune_bytes_scanned": dune_bytes,
        "run_file": str(path),
    }
