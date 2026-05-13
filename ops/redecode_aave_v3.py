"""Re-decode the Aave V3 logs already sitting in canonical_logs.

The unfiltered raw_logs ingest captured every log in its run windows,
including Aave V3 Supply/Withdraw/Borrow/Repay events at the Pool
contract addresses. Now that the aave_v3 YAML mapping is registered,
this script re-decodes those existing raw rows into canonical events —
no HyperSync re-fetch needed.

Selection scope: only logs whose (chain, address) is one of the five
hardcoded Aave V3 main-market Pool addresses AND whose topic0 is one of
the four Aave V3 event signatures. This is deliberately narrow — a
broader topic0-only scan would catch shape-collision events at unrelated
contracts (the `Withdraw(address,address,address,uint256)` signature
isn't unique to Aave).

Idempotent: canonical_events is ReplacingMergeTree on
(entity_id, timestamp, event_id) so re-running collapses duplicates at
merge time.

Usage:
    PYTHONPATH=. python ops/redecode_aave_v3.py
    PYTHONPATH=. python ops/redecode_aave_v3.py --chain base --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import clickhouse_connect  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from core.adapters.base import CanonicalEvent  # noqa: E402
from core.adapters.evm.decoders.base import DecodedEvent  # noqa: E402
from core.adapters.evm.decoders.generic import (  # noqa: E402
    GenericABIDecoder,
    load_mapping_dir,
)
from core.sink.clickhouse import ClickHouseSink, SinkConfig  # noqa: E402

load_dotenv()

MAPPINGS_DIR = (
    Path(__file__).resolve().parent.parent
    / "core/adapters/evm/decoders/mappings"
)

# Aave V3 main-market Pool addresses (mirror of ops/seed_aave_v3_contracts.py).
AAVE_V3_POOLS = {
    "ethereum": "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
    "base":     "0xa238dd80c259a72e81d7e4664a9801593f98d1c5",
    "arbitrum": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
    "optimism": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
    "polygon":  "0x794a61358d6845594f94dc1db02a252b5b4814ad",
}

logger = logging.getLogger("ops.redecode_aave_v3")


def build_aave_decoders() -> dict[str, GenericABIDecoder]:
    """Map topic0 -> decoder for the four Aave V3 event signatures."""
    for mapping in load_mapping_dir(MAPPINGS_DIR):
        if mapping.protocol != "aave_v3":
            continue
        out: dict[str, GenericABIDecoder] = {}
        for ev in mapping.events:
            d = GenericABIDecoder(mapping, ev)
            out[d.topic0] = d
        return out
    raise RuntimeError("aave_v3 mapping not found under " + str(MAPPINGS_DIR))


def _s(v) -> str | None:
    """clickhouse-connect returns FixedString columns as `bytes` and
    nullable variants as `bytes | None`. Decode to str for the decoder."""
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode("ascii")
    return v


def reconstruct_log(row: dict) -> dict:
    """Build a HyperSync-style log dict from a canonical_logs row.

    The GenericABIDecoder.decode() method consumes the same shape that
    the live ingest pipeline produces, so re-decoding requires only that
    we re-pack the stored columns back into that form.
    """
    topics = [
        _s(t) for t in (row["topic0"], row["topic1"], row["topic2"], row["topic3"])
        if t is not None
    ]
    return {
        "address": _s(row["address"]),
        "topics": topics,
        "data": _s(row["data"]),
        "blockNumber": hex(row["block_number"]),
        "transactionHash": _s(row["tx_hash"]),
        "logIndex": hex(row["log_index"]),
    }


def to_canonical(
    decoded: DecodedEvent, chain: str, source_system: str
) -> CanonicalEvent:
    """Mirror of EVMAdapter._to_canonical without the live-ingest deps."""
    source_event_id = f"{decoded.tx_hash}:{decoded.log_index}"
    event_id = hashlib.sha256(
        f"{source_system}:{source_event_id}".encode()
    ).hexdigest()
    return CanonicalEvent(
        entity_id=decoded.entity_id,
        entity_type="wallet",
        event_id=event_id,
        event_type=decoded.event_type,
        event_category="transaction",
        timestamp=decoded.timestamp,
        source_system=source_system,
        source_event_id=source_event_id,
        chain=chain,
        block_number=decoded.block_number,
        block_time=decoded.timestamp,
        tx_hash=decoded.tx_hash,
        log_index=decoded.log_index,
        protocol=decoded.protocol or None,
        venue=decoded.venue or None,
        token_in=decoded.token_in,
        token_out=decoded.token_out,
        amount_in=decoded.amount_in,
        amount_out=decoded.amount_out,
        amount_in_usd=None,  # USD enrichment skipped — backfill, not ingest
        amount_out_usd=None,
        counterparty=decoded.counterparty,
        aggregator=decoded.aggregator or None,
        link_key=decoded.link_key,
        link_key_type=decoded.link_key_type,
        extra=decoded.extra,
    )


def redecode_chain(
    client, chain: str, pool_address: str,
    decoders: dict[str, GenericABIDecoder],
    sink: ClickHouseSink | None,
) -> tuple[int, int]:
    """Returns (rows_decoded, rows_written)."""
    topics_in = "(" + ",".join(f"'{t}'" for t in decoders.keys()) + ")"
    sql = f"""
        SELECT
            chain, source_system, address,
            topic0, topic1, topic2, topic3,
            data, block_number, block_time, tx_hash, log_index
        FROM canonical_logs
        WHERE chain = '{chain}'
          AND lower(address) = '{pool_address}'
          AND topic0 IN {topics_in}
        ORDER BY block_number, log_index
    """
    result = client.query(sql)

    events: list[CanonicalEvent] = []
    skipped = 0
    for row in result.named_results():
        topic0 = _s(row["topic0"])
        decoder = decoders.get(topic0) if topic0 else None
        if decoder is None:
            skipped += 1
            continue
        ts = row["block_time"] or datetime.now(timezone.utc)
        decoded = decoder.decode(reconstruct_log(row), ts)
        if decoded is None:
            skipped += 1
            continue
        events.append(to_canonical(decoded, chain, row["source_system"]))

    logger.info(
        "  %s: %d rows pulled, %d decoded, %d skipped",
        chain, len(result.result_rows), len(events), skipped,
    )

    if sink is not None and events:
        sink.write(events)
        wrote = sink.flush()
        return len(events), wrote
    return len(events), 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--chain", default=None,
        help="Limit to one chain (default: all 5)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ch-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument(
        "--ch-port", type=int,
        default=int(os.getenv("CLICKHOUSE_PORT", "8124")),
    )
    parser.add_argument("--ch-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CLICKHOUSE_PASSWORD", "nexus"))
    parser.add_argument("--ch-database", default=os.getenv("CLICKHOUSE_DB", "nexus"))
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    decoders = build_aave_decoders()
    logger.info(
        "loaded %d aave_v3 decoders: %s",
        len(decoders), sorted(decoders.keys()),
    )

    client = clickhouse_connect.get_client(
        host=args.ch_host, port=args.ch_port,
        username=args.ch_user, password=args.ch_password,
        database=args.ch_database,
    )

    chains = [args.chain] if args.chain else list(AAVE_V3_POOLS.keys())

    sink: ClickHouseSink | None = None
    if not args.dry_run:
        sink = ClickHouseSink(SinkConfig(
            host=args.ch_host, port=args.ch_port,
            username=args.ch_user, password=args.ch_password,
            database=args.ch_database, batch_size=10000,
            table="canonical_events",
        ))

    total_decoded = 0
    total_written = 0
    try:
        for chain in chains:
            pool_address = AAVE_V3_POOLS[chain].lower()
            decoded, written = redecode_chain(
                client, chain, pool_address, decoders, sink,
            )
            total_decoded += decoded
            total_written += written
    finally:
        if sink is not None:
            sink.close()

    logger.info(
        "DONE: total_decoded=%d total_written=%d dry_run=%s",
        total_decoded, total_written, args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
