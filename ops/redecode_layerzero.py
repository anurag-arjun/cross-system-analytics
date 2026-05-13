"""Re-decode LayerZero V2 PacketSent + PacketDelivered events from
canonical_logs into canonical_events.

Why this exists:
  - PacketSent had no decoder before — ~1,169 events sat in canonical_logs.
  - PacketDelivered was decoded but `link_key` only carried src_eid (a
    per-chain id, useless for matching).

After updating bridge.py to:
  - emit `nonce` in `PacketDelivered.extra`
  - add `LayerZeroPacketSentDecoder` that parses the encoded payload
and the EVMAdapter's `_composite_link_key` to weave src_eid + sender +
nonce + dst_eid into a packet-unique link_key, we need to re-decode the
raw logs to backfill both sides.

Idempotent via ReplacingMergeTree on (entity_id, timestamp, event_id).
Re-inserts the same event_id with the new link_key/extra; FINAL on
canonical_events resolves to the newer row.

Usage:  PYTHONPATH=. python ops/redecode_layerzero.py
"""

from __future__ import annotations

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
from core.adapters.evm import _composite_link_key  # noqa: E402
from core.adapters.evm.decoders.bridge import (  # noqa: E402
    LayerZeroPacketDeliveredDecoder,
    LayerZeroPacketSentDecoder,
)
from core.sink.clickhouse import ClickHouseSink, SinkConfig  # noqa: E402

load_dotenv()

DECODERS = {
    d.topic0: d
    for d in (LayerZeroPacketSentDecoder(), LayerZeroPacketDeliveredDecoder())
}

CHAINS = ["ethereum", "base", "arbitrum", "optimism", "polygon"]

logger = logging.getLogger("ops.redecode_lz")


def _s(v):
    if v is None:
        return None
    if isinstance(v, bytes):
        return v.decode("ascii")
    return v


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


def to_canonical(decoded, chain: str, source_system: str) -> CanonicalEvent:
    source_event_id = f"{decoded.tx_hash}:{decoded.log_index}"
    event_id = hashlib.sha256(
        f"{source_system}:{source_event_id}".encode()
    ).hexdigest()
    link_key = _composite_link_key(decoded, chain)
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
        link_key=link_key,
        link_key_type=decoded.link_key_type,
        extra=decoded.extra,
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    ch_kwargs = dict(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8124")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "nexus"),
        database=os.getenv("CLICKHOUSE_DB", "nexus"),
    )
    client = clickhouse_connect.get_client(**ch_kwargs)
    sink = ClickHouseSink(SinkConfig(
        **ch_kwargs, batch_size=10000, table="canonical_events",
    ))

    topics_in = "(" + ",".join(f"'{t}'" for t in DECODERS.keys()) + ")"

    total_decoded = 0
    total_written = 0
    try:
        for chain in CHAINS:
            sql = f"""
                SELECT chain, source_system, address,
                       topic0, topic1, topic2, topic3,
                       data, block_number, block_time, tx_hash, log_index
                FROM canonical_logs
                WHERE chain = '{chain}'
                  AND topic0 IN {topics_in}
                ORDER BY block_number, log_index
            """
            result = client.query(sql)

            events = []
            skipped = 0
            for row in result.named_results():
                t0 = _s(row["topic0"])
                d = DECODERS.get(t0)
                if d is None:
                    skipped += 1
                    continue
                ts = row["block_time"] or datetime.now(timezone.utc)
                try:
                    decoded = d.decode(reconstruct_log(row), ts)
                except Exception:
                    logger.exception("decode failed tx=%s log=%s", row["tx_hash"], row["log_index"])
                    skipped += 1
                    continue
                if decoded is None:
                    skipped += 1
                    continue
                events.append(to_canonical(decoded, chain, row["source_system"]))

            logger.info(
                "%s: pulled=%d decoded=%d skipped=%d",
                chain, len(result.result_rows), len(events), skipped,
            )
            if events:
                sink.write(events)
                total_decoded += len(events)
                total_written += sink.flush()
    finally:
        sink.close()

    logger.info("DONE total_decoded=%d total_written=%d", total_decoded, total_written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
