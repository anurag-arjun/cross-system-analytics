"""Backfill the UniV4 pool registry from PoolManager Initialize events.

Walks PoolManager logs from a known deployment block forward, decodes every
Initialize event, and upserts into the `uniswap_v4_pools` Postgres table.
HyperSync filters by `(address=PoolManager, topic0=Initialize_topic0)` so a
~3.2M-block range comes back in seconds with only the relevant logs.

Run from the project root:

    PYTHONPATH=. python ops/backfill_uniswap_v4_pools.py
"""

from __future__ import annotations

import argparse
import os
import sys
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

import hypersync
from dotenv import load_dotenv
from eth_abi import decode as eth_abi_decode
from eth_utils import keccak

from core.registry.uniswap_v4_pools import UniV4Pool, UniV4PoolStore

load_dotenv()


# Canonical PoolManager addresses per chain. Hand-curated — not in
# protocol_contracts because Dune's bootstrap missed some.
POOL_MANAGERS: dict[str, str] = {
    "ethereum": "0x000000000004444c5dc75cb358380d2e3de08a90",
}

# Approximate deployment blocks per chain. Used as the backfill lower bound.
DEPLOY_BLOCKS: dict[str, int] = {
    "ethereum": 21_688_329,
}

# UniV4 PoolManager URLs (HyperSync slugs).
HYPERSYNC_URLS: dict[str, str] = {
    "ethereum": "https://eth.hypersync.xyz",
    "base": "https://base.hypersync.xyz",
    "arbitrum": "https://arbitrum.hypersync.xyz",
    "optimism": "https://optimism.hypersync.xyz",
    "polygon": "https://polygon.hypersync.xyz",
}

INITIALIZE_SIG = (
    "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
)
INITIALIZE_TOPIC0 = "0x" + keccak(text=INITIALIZE_SIG).hex()


@dataclass
class Stats:
    initialized: int = 0
    upserted: int = 0
    blocks_scanned: int = 0


def _addr_from_topic(topic: str) -> str:
    return "0x" + topic[-40:]


async def _backfill_one_chain(
    chain: str,
    store: UniV4PoolStore,
    hyper_token: str | None,
    from_block: int | None,
    end_block: int | None,
    chunk_size: int,
) -> Stats:
    pool_manager = POOL_MANAGERS[chain]
    if from_block is None:
        from_block = DEPLOY_BLOCKS[chain]
    cfg = hypersync.ClientConfig(
        url=HYPERSYNC_URLS[chain],
        api_token=hyper_token,
        proactive_rate_limit_sleep=True,
    )
    client = hypersync.HypersyncClient(cfg)
    head = await client.get_height() if end_block is None else end_block

    print(
        f"[{chain}] backfilling pool_manager={pool_manager} "
        f"blocks {from_block:,}..{head:,} ({head - from_block:,} blocks)"
    )

    log_selection = hypersync.LogSelection(
        address=[pool_manager],
        topics=[[INITIALIZE_TOPIC0]],
    )
    field = hypersync.FieldSelection(
        log=[
            "address",
            "topic0",
            "topic1",
            "topic2",
            "topic3",
            "data",
            "block_number",
            "transaction_hash",
            "log_index",
        ],
        block=["number", "timestamp"],
    )

    stats = Stats()
    current = from_block
    while current < head:
        chunk_end = min(current + chunk_size, head)
        await client.wait_for_rate_limit()
        query = hypersync.Query(
            from_block=current,
            to_block=chunk_end,
            max_num_blocks=chunk_size,
            max_num_logs=50_000,
            logs=[log_selection],
            field_selection=field,
        )
        resp = await client.get(query)

        # Build a block_number -> timestamp map for this batch.
        block_ts: dict[int, datetime] = {}
        for blk in resp.data.blocks:
            ts = blk.timestamp
            if isinstance(ts, str):
                ts = int(ts, 16)
            block_ts[blk.number] = datetime.fromtimestamp(int(ts), tz=timezone.utc)

        pools: list[UniV4Pool] = []
        for log in resp.data.logs:
            topics = log.topics
            if len(topics) < 4:
                continue
            pool_id = topics[1]
            currency0 = _addr_from_topic(topics[2])
            currency1 = _addr_from_topic(topics[3])
            data = log.data
            if not data or data == "0x":
                continue
            try:
                fee, tick_spacing, hooks_addr, _sqrt, _tick = eth_abi_decode(
                    ["uint24", "int24", "address", "uint160", "int24"],
                    bytes.fromhex(data[2:]),
                )
            except Exception:
                continue

            pools.append(
                UniV4Pool(
                    chain=chain,
                    pool_id=pool_id,
                    pool_manager=pool_manager,
                    currency0=currency0,
                    currency1=currency1,
                    fee=int(fee),
                    tick_spacing=int(tick_spacing),
                    hooks=hooks_addr,
                    init_block=log.block_number,
                    init_block_time=block_ts.get(log.block_number, datetime.now(timezone.utc)),
                    init_tx_hash=log.transaction_hash,
                )
            )

        stats.initialized += len(pools)
        if pools:
            stats.upserted += store.upsert_many(pools)

        next_cur = resp.next_block if resp.next_block else chunk_end
        stats.blocks_scanned += next_cur - current
        current = next_cur if next_cur > current else chunk_end

    try:
        await client.close()
    except Exception:
        pass
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--chains",
        nargs="+",
        default=["ethereum"],
        choices=sorted(POOL_MANAGERS.keys()),
    )
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--end-block", type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=500_000)
    parser.add_argument(
        "--postgres",
        default=os.environ.get(
            "PROTOCOL_CONTRACTS_DSN",
            "postgresql://nexus:nexus@localhost:5434/nexus_ops",
        ),
    )
    args = parser.parse_args(argv)

    store = UniV4PoolStore(args.postgres)
    hyper_token = os.getenv("HYPERSYNC_TOKEN")

    async def run() -> None:
        before = store.count()
        for chain in args.chains:
            stats = await _backfill_one_chain(
                chain=chain,
                store=store,
                hyper_token=hyper_token,
                from_block=args.from_block,
                end_block=args.end_block,
                chunk_size=args.chunk_size,
            )
            print(
                f"[{chain}] initialized={stats.initialized} "
                f"upserted={stats.upserted} blocks_scanned={stats.blocks_scanned:,}"
            )
        after = store.count()
        print(f"\ntotal pools in registry: {before} -> {after}")

    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
