#!/usr/bin/env python3
"""Build pool registry by fetching pool creation events.

Usage:
    python -m core.demo.build_pool_registry --chain base
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timezone

import clickhouse_connect
import hypersync
from dotenv import load_dotenv
from eth_abi import decode

load_dotenv()

# Uniswap V3 PoolCreated event
# PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)
V3_POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

# Uniswap V2 PairCreated event  
# PairCreated(address indexed token0, address indexed token1, address pair, uint)
V2_PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# Factory addresses
FACTORIES = {
    "base": {
        "uniswap_v3": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
        "uniswap_v2": "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6",
        "aerodrome": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
    },
    "arbitrum": {
        "uniswap_v3": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "uniswap_v2": "0xf1D7CC64Fb4452F05c498126312eBE29f30Fbcf9",
        "sushiswap": "0xc35DADB65012eC5796536bD9864eD8773aBc74C4",
    },
}

CHAIN_TO_HYPERSYNC = {
    "base": "https://base.hypersync.xyz",
    "arbitrum": "https://arbitrum.hypersync.xyz",
}


async def fetch_pool_creations(chain: str, factory: str, topic0: str, is_v3: bool) -> list[dict]:
    """Fetch pool creation events from a factory."""
    url = CHAIN_TO_HYPERSYNC.get(chain)
    if not url:
        return []
    
    token = os.getenv("HYPERSYNC_TOKEN")
    cfg = hypersync.ClientConfig(url=url, bearer_token=token)
    client = hypersync.HypersyncClient(cfg)
    
    # Get last 30 days of pools (enough for demo)
    head = await client.get_height()
    blocks_per_day = 43200 if chain in ("base", "arbitrum") else 7200
    from_block = max(0, head - (blocks_per_day * 30))
    
    query = hypersync.Query(
        from_block=from_block,
        logs=[
            hypersync.LogSelection(
                address=[factory],
                topics=[[topic0]],
            )
        ],
        field_selection=hypersync.FieldSelection(
            log=["address", "topic0", "topic1", "topic2", "topic3", "data", "block_number"],
        ),
    )
    
    pools = []
    while True:
        resp = await client.get(query)
        for log in resp.data.logs:
            try:
                if is_v3:
                    # V3: token0, token1 in topics, pool in data
                    token0 = "0x" + log.topics[1][-40:]
                    token1 = "0x" + log.topics[2][-40:]
                    fee = int(log.topics[3], 16)
                    # Decode data: (int24 tickSpacing, address pool)
                    data_bytes = bytes.fromhex(log.data[2:]) if log.data else b""
                    if len(data_bytes) >= 64:
                        decoded = decode(["int24", "address"], data_bytes)
                        pool = decoded[1]
                    else:
                        continue
                else:
                    # V2: token0, token1 in topics, pair in data
                    token0 = "0x" + log.topics[1][-40:]
                    token1 = "0x" + log.topics[2][-40:]
                    fee = None
                    data_bytes = bytes.fromhex(log.data[2:]) if log.data else b""
                    if len(data_bytes) >= 64:
                        decoded = decode(["address", "uint256"], data_bytes)
                        pool = decoded[0]
                    else:
                        continue
                
                pools.append({
                    "pool": pool.lower() if isinstance(pool, str) else "0x" + pool.hex(),
                    "token0": token0.lower(),
                    "token1": token1.lower(),
                    "fee": fee,
                })
            except Exception as e:
                continue
        
        if resp.next_block >= head:
            break
        query.from_block = resp.next_block
    
    return pools


def main():
    parser = argparse.ArgumentParser(description="Build pool registry")
    parser.add_argument("--chain", default="base", help="Chain to index")
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=8124, help="ClickHouse port")
    args = parser.parse_args()

    client = clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username="default",
        password="nexus",
        database="nexus",
    )

    factories = FACTORIES.get(args.chain, {})
    if not factories:
        print(f"No factories configured for {args.chain}")
        return

    total = 0
    now = datetime.now(timezone.utc)

    for protocol, factory in factories.items():
        is_v3 = "v3" in protocol
        topic = V3_POOL_CREATED_TOPIC if is_v3 else V2_PAIR_CREATED_TOPIC
        
        print(f"[*] Fetching {protocol} pools from {factory[:10]}...")
        pools = asyncio.run(fetch_pool_creations(args.chain, factory, topic, is_v3))
        
        if pools:
            rows = [
                [p["pool"], args.chain, protocol, p["token0"], p["token1"], p["fee"], now]
                for p in pools
            ]
            client.insert("pool_registry", rows)
            print(f"    Inserted {len(pools)} pools")
            total += len(pools)

    print(f"[+] Total pools indexed: {total}")

    # Update the enriched view to use pool registry
    print("[*] Creating enriched_swaps_v2 view with pool tokens...")
    client.command("""
        CREATE OR REPLACE VIEW enriched_swaps_v2 AS
        SELECT 
            ce.entity_id,
            ce.event_type,
            ce.protocol,
            ce.chain,
            ce.timestamp,
            ce.tx_hash,
            ce.venue as pool_address,
            pr.token0,
            pr.token1,
            ce.amount_in as amount0,
            ce.amount_out as amount1,
            tm0.symbol as token0_symbol,
            tm1.symbol as token1_symbol,
            tm0.decimals as token0_decimals,
            tm1.decimals as token1_decimals,
            -- Human readable amounts
            CASE WHEN tm0.decimals IS NOT NULL THEN toFloat64(ce.amount_in) / pow(10, tm0.decimals) ELSE NULL END as amount0_human,
            CASE WHEN tm1.decimals IS NOT NULL THEN toFloat64(ce.amount_out) / pow(10, tm1.decimals) ELSE NULL END as amount1_human,
            -- USD values
            tp0.price_usd as token0_price,
            tp1.price_usd as token1_price,
            CASE 
                WHEN tm0.decimals IS NOT NULL AND tp0.price_usd IS NOT NULL 
                THEN (toFloat64(ce.amount_in) / pow(10, tm0.decimals)) * tp0.price_usd 
                ELSE NULL 
            END as amount0_usd,
            CASE 
                WHEN tm1.decimals IS NOT NULL AND tp1.price_usd IS NOT NULL 
                THEN (toFloat64(ce.amount_out) / pow(10, tm1.decimals)) * tp1.price_usd 
                ELSE NULL 
            END as amount1_usd
        FROM canonical_events ce
        LEFT JOIN pool_registry pr ON lower(ce.venue) = pr.pool_address AND ce.chain = pr.chain
        LEFT JOIN token_metadata tm0 ON pr.token0 = tm0.token_address AND ce.chain = tm0.chain
        LEFT JOIN token_metadata tm1 ON pr.token1 = tm1.token_address AND ce.chain = tm1.chain
        LEFT JOIN token_prices tp0 ON pr.token0 = tp0.token_address AND ce.chain = tp0.chain
        LEFT JOIN token_prices tp1 ON pr.token1 = tp1.token_address AND ce.chain = tp1.chain
        WHERE ce.event_type = 'swap'
    """)
    print("    Created enriched_swaps_v2 view")

    # Test
    test = client.query("""
        SELECT 
            count() as total,
            countIf(pool_address IS NOT NULL AND token0 IS NOT NULL) as with_pool,
            countIf(token0_symbol IS NOT NULL) as with_symbols,
            countIf(amount0_usd IS NOT NULL) as with_usd,
            sum(coalesce(amount0_usd, 0)) as volume_usd
        FROM enriched_swaps_v2
    """).result_rows[0]
    
    print(f"\n[*] Enrichment stats:")
    print(f"    Total swaps: {test[0]}")
    print(f"    With pool tokens: {test[1]}")
    print(f"    With symbols: {test[2]}")
    print(f"    With USD: {test[3]}")
    print(f"    Volume (USD): ${test[4]:,.2f}" if test[4] else "    Volume: $0")

    client.close()


if __name__ == "__main__":
    main()
