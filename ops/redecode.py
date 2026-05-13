"""Re-decode existing canonical_logs rows through a YAML decoder.

The unfiltered raw_logs ingest captured every log on each chain it ran
on. Once a new decoder is added — YAML mapping + addresses seeded in
``protocol_contracts`` — this script re-decodes those existing raw rows
into canonical events without re-fetching from HyperSync.

Scope per run is the address set registered for a given protocol slug in
``protocol_contracts``. The decoder applies its full topic0 set against
that address set; only matching logs are decoded.

Idempotent: ``canonical_events`` is ReplacingMergeTree on
(entity_id, timestamp, event_id) so re-running collapses duplicates at
merge time.

Usage:
    PYTHONPATH=. python ops/redecode.py --protocol aave_v3
    PYTHONPATH=. python ops/redecode.py --protocol lido --chain ethereum --dry-run
    PYTHONPATH=. python ops/redecode.py --protocol compound_v3
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
from core.registry.protocol_contracts import (  # noqa: E402
    PostgresProtocolContractStore,
)
from core.sink.clickhouse import ClickHouseSink, SinkConfig  # noqa: E402

load_dotenv()

MAPPINGS_DIR = (
    Path(__file__).resolve().parent.parent
    / "core/adapters/evm/decoders/mappings"
)

logger = logging.getLogger("ops.redecode")


def _s(v) -> str | None:
    """clickhouse-connect returns FixedString columns as `bytes`.
    Decode at the row boundary so downstream code sees `str`."""
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode("ascii")
    return v


def build_decoders_for_protocol(slug: str) -> dict[str, GenericABIDecoder]:
    """Return topic0 -> decoder for every event in the protocol's YAML.

    The slug matches the YAML's ``protocol:`` field (e.g. ``aave_v3``,
    ``lido``, ``compound_v3``).
    """
    for mapping in load_mapping_dir(MAPPINGS_DIR):
        if mapping.protocol != slug:
            continue
        return {
            (d := GenericABIDecoder(mapping, ev)).topic0: d
            for ev in mapping.events
        }
    raise RuntimeError(
        f"No YAML mapping with protocol: '{slug}' under {MAPPINGS_DIR}"
    )


def addresses_for_protocol(
    pg_dsn: str, slug: str, chain_filter: str | None = None
) -> dict[str, list[str]]:
    """Pull (chain -> [address, ...]) from protocol_contracts for the slug.

    Aggregates across all sources (manual / dune / spellbook). Lowercased.
    """
    import psycopg2

    sql = """
        SELECT chain, address
        FROM protocol_contracts
        WHERE protocol = %s
    """
    params: list = [slug]
    if chain_filter is not None:
        sql += " AND chain = %s"
        params.append(chain_filter)

    out: dict[str, list[str]] = {}
    with psycopg2.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for chain, address in cur.fetchall():
                out.setdefault(chain.lower(), []).append(address.lower())
    return out


def reconstruct_log(row: dict) -> dict:
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
    """Mirror of EVMAdapter._to_canonical without the live-ingest deps.
    USD enrichment is skipped — this script backfills, doesn't price."""
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
        amount_in_usd=None,
        amount_out_usd=None,
        counterparty=decoded.counterparty,
        aggregator=decoded.aggregator or None,
        link_key=decoded.link_key,
        link_key_type=decoded.link_key_type,
        extra=decoded.extra,
    )


def redecode_chain(
    client, chain: str, addresses: list[str],
    decoders: dict[str, GenericABIDecoder],
    sink: ClickHouseSink | None,
) -> tuple[int, int]:
    """Returns (rows_decoded, rows_written)."""
    topics_in = "(" + ",".join(f"'{t}'" for t in decoders.keys()) + ")"
    addrs_in = "(" + ",".join(f"'{a}'" for a in addresses) + ")"
    sql = f"""
        SELECT
            chain, source_system, address,
            topic0, topic1, topic2, topic3,
            data, block_number, block_time, tx_hash, log_index
        FROM canonical_logs
        WHERE chain = '{chain}'
          AND lower(address) IN {addrs_in}
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
        "  %s (%d addrs): %d pulled, %d decoded, %d skipped",
        chain, len(addresses),
        len(result.result_rows), len(events), skipped,
    )

    if sink is not None and events:
        sink.write(events)
        wrote = sink.flush()
        return len(events), wrote
    return len(events), 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--protocol", required=True,
        help="Protocol slug — matches the YAML mapping's protocol: field",
    )
    parser.add_argument(
        "--chain", default=None,
        help="Limit to one chain (default: every chain with addresses for this protocol)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--postgres",
        default=os.environ.get(
            "PROTOCOL_CONTRACTS_DSN",
            "postgresql://nexus:nexus@localhost:5434/nexus_ops",
        ),
    )
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

    decoders = build_decoders_for_protocol(args.protocol)
    logger.info(
        "loaded %d decoders for %s: %s",
        len(decoders), args.protocol, sorted(decoders.keys()),
    )

    by_chain = addresses_for_protocol(args.postgres, args.protocol, args.chain)
    if not by_chain:
        logger.error(
            "No addresses found in protocol_contracts for protocol=%r%s. "
            "Seed addresses first (see ops/seed_*.py).",
            args.protocol,
            f" chain={args.chain!r}" if args.chain else "",
        )
        return 2
    logger.info(
        "address coverage: %s",
        {c: len(a) for c, a in sorted(by_chain.items())},
    )

    client = clickhouse_connect.get_client(
        host=args.ch_host, port=args.ch_port,
        username=args.ch_user, password=args.ch_password,
        database=args.ch_database,
    )

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
        for chain in sorted(by_chain.keys()):
            decoded, written = redecode_chain(
                client, chain, by_chain[chain], decoders, sink,
            )
            total_decoded += decoded
            total_written += written
    finally:
        if sink is not None:
            sink.close()

    logger.info(
        "DONE protocol=%s total_decoded=%d total_written=%d dry_run=%s",
        args.protocol, total_decoded, total_written, args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
