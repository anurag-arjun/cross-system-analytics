#!/usr/bin/env python3
"""Enrich token metadata and prices for demo data.

Usage:
    python -m core.demo.enrich_tokens
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect

# Common tokens on Base and Arbitrum with their CoinGecko IDs
KNOWN_TOKENS = {
    "base": {
        "0x4200000000000000000000000000000000000006": ("WETH", 18, "Wrapped Ether", "weth"),
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": ("USDC", 6, "USD Coin", "usd-coin"),
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": ("DAI", 18, "Dai Stablecoin", "dai"),
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": ("USDbC", 6, "USD Base Coin", "bridged-usd-coin-base"),
        "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22": ("cbETH", 18, "Coinbase Wrapped Staked ETH", "coinbase-wrapped-staked-eth"),
        "0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452": ("wstETH", 18, "Wrapped stETH", "wrapped-steth"),
        "0x0000000000000000000000000000000000000000": ("ETH", 18, "Ether", "ethereum"),
    },
    "arbitrum": {
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": ("WETH", 18, "Wrapped Ether", "weth"),
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831": ("USDC", 6, "USD Coin", "usd-coin"),
        "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": ("USDC.e", 6, "Bridged USDC", "usd-coin"),
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": ("USDT", 6, "Tether", "tether"),
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": ("DAI", 18, "Dai Stablecoin", "dai"),
        "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": ("WBTC", 8, "Wrapped BTC", "wrapped-bitcoin"),
        "0x912ce59144191c1204e64559fe8253a0e49e6548": ("ARB", 18, "Arbitrum", "arbitrum"),
        "0x5979d7b546e38e414f7e9822514be443a4800529": ("wstETH", 18, "Wrapped stETH", "wrapped-steth"),
        "0x0000000000000000000000000000000000000000": ("ETH", 18, "Ether", "ethereum"),
    },
}

# Current approximate prices (fallback if CoinGecko rate limited)
FALLBACK_PRICES = {
    "weth": 3200.0,
    "ethereum": 3200.0,
    "usd-coin": 1.0,
    "tether": 1.0,
    "dai": 1.0,
    "bridged-usd-coin-base": 1.0,
    "wrapped-bitcoin": 95000.0,
    "arbitrum": 0.85,
    "coinbase-wrapped-staked-eth": 3400.0,
    "wrapped-steth": 3600.0,
}


def main():
    parser = argparse.ArgumentParser(description="Enrich token metadata and prices")
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

    # 1. Insert token metadata
    print("[*] Inserting token metadata...")
    metadata_rows = []
    for chain, tokens in KNOWN_TOKENS.items():
        for addr, (symbol, decimals, name, cg_id) in tokens.items():
            metadata_rows.append([addr.lower(), chain, symbol, decimals, name, cg_id, datetime.now(timezone.utc)])
    
    client.insert("token_metadata", metadata_rows)
    print(f"    Inserted {len(metadata_rows)} token metadata records")

    # 2. Discover tokens from canonical_events that we don't have metadata for
    print("[*] Discovering tokens from events...")
    unknown = client.query("""
        SELECT DISTINCT 
            lower(token_in) as token, chain
        FROM canonical_events 
        WHERE token_in IS NOT NULL AND token_in != ''
        UNION DISTINCT
        SELECT DISTINCT 
            lower(token_out) as token, chain
        FROM canonical_events 
        WHERE token_out IS NOT NULL AND token_out != ''
    """).result_rows
    
    known_set = set()
    for chain, tokens in KNOWN_TOKENS.items():
        for addr in tokens:
            known_set.add((addr.lower(), chain))
    
    unknown_tokens = [(t, c) for t, c in unknown if (t, c) not in known_set and t and len(t) == 42]
    print(f"    Found {len(unknown_tokens)} unknown tokens")

    # 3. Insert current prices
    print("[*] Inserting token prices...")
    now = datetime.now(timezone.utc)
    price_rows = []
    for chain, tokens in KNOWN_TOKENS.items():
        for addr, (symbol, decimals, name, cg_id) in tokens.items():
            price = FALLBACK_PRICES.get(cg_id, 0.0)
            if price > 0:
                price_rows.append([addr.lower(), chain, now, price, "fallback", None, now])
    
    client.insert("token_prices", price_rows)
    print(f"    Inserted {len(price_rows)} price records")

    # 4. Create enriched view for swaps with USD values
    print("[*] Creating enriched swaps view...")
    client.command("""
        CREATE OR REPLACE VIEW enriched_swaps AS
        SELECT 
            ce.entity_id,
            ce.event_type,
            ce.protocol,
            ce.chain,
            ce.timestamp,
            ce.tx_hash,
            ce.venue,
            ce.token_in,
            ce.token_out,
            ce.amount_in,
            ce.amount_out,
            tm_in.symbol as token_in_symbol,
            tm_out.symbol as token_out_symbol,
            tm_in.decimals as token_in_decimals,
            tm_out.decimals as token_out_decimals,
            CASE 
                WHEN tm_in.decimals IS NOT NULL AND ce.amount_in IS NOT NULL 
                THEN toFloat64(ce.amount_in) / pow(10, tm_in.decimals)
                ELSE NULL 
            END as amount_in_human,
            CASE 
                WHEN tm_out.decimals IS NOT NULL AND ce.amount_out IS NOT NULL 
                THEN toFloat64(ce.amount_out) / pow(10, tm_out.decimals)
                ELSE NULL 
            END as amount_out_human,
            tp_in.price_usd as price_in_usd,
            tp_out.price_usd as price_out_usd,
            CASE 
                WHEN tm_in.decimals IS NOT NULL AND ce.amount_in IS NOT NULL AND tp_in.price_usd IS NOT NULL
                THEN (toFloat64(ce.amount_in) / pow(10, tm_in.decimals)) * tp_in.price_usd
                ELSE NULL 
            END as amount_in_usd,
            CASE 
                WHEN tm_out.decimals IS NOT NULL AND ce.amount_out IS NOT NULL AND tp_out.price_usd IS NOT NULL
                THEN (toFloat64(ce.amount_out) / pow(10, tm_out.decimals)) * tp_out.price_usd
                ELSE NULL 
            END as amount_out_usd
        FROM canonical_events ce
        LEFT JOIN token_metadata tm_in ON lower(ce.token_in) = tm_in.token_address AND ce.chain = tm_in.chain
        LEFT JOIN token_metadata tm_out ON lower(ce.token_out) = tm_out.token_address AND ce.chain = tm_out.chain
        LEFT JOIN token_prices tp_in ON lower(ce.token_in) = tp_in.token_address AND ce.chain = tp_in.chain
        LEFT JOIN token_prices tp_out ON lower(ce.token_out) = tp_out.token_address AND ce.chain = tp_out.chain
        WHERE ce.event_type = 'swap'
    """)
    print("    Created enriched_swaps view")

    # 5. Test the enrichment
    print("[*] Testing enrichment...")
    test = client.query("""
        SELECT 
            count() as total_swaps,
            countIf(token_in_symbol IS NOT NULL) as with_token_in,
            countIf(amount_in_usd IS NOT NULL) as with_usd_in,
            sum(amount_in_usd) as total_volume_usd
        FROM enriched_swaps
    """).result_rows[0]
    
    print(f"    Total swaps: {test[0]}")
    print(f"    With token_in symbol: {test[1]}")
    print(f"    With USD value: {test[2]}")
    print(f"    Total volume (USD): ${test[3]:,.2f}" if test[3] else "    Total volume (USD): $0")

    client.close()
    print("[+] Done!")


if __name__ == "__main__":
    main()
