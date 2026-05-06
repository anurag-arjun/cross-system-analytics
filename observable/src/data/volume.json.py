#!/usr/bin/env python3
"""Data loader for volume statistics with USD values."""

import json
import sys

import clickhouse_connect


def main():
    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="default",
        password="nexus",
        database="default",
    )

    # Volume by protocol with USD
    by_protocol = client.query("""
        SELECT 
            protocol,
            count() as swaps,
            sum(CASE 
                WHEN token0_symbol IN ('WETH', 'USDC', 'USDbC', 'DAI') AND amount0_usd > 0 AND amount0_usd < 1000000 THEN amount0_usd
                WHEN token1_symbol IN ('WETH', 'USDC', 'USDbC', 'DAI') AND amount1_usd > 0 AND amount1_usd < 1000000 THEN amount1_usd
                ELSE 0 
            END) as volume_usd
        FROM enriched_swaps_v2
        GROUP BY protocol
        ORDER BY volume_usd DESC
    """).result_rows

    # Top tokens by volume
    top_tokens = client.query("""
        SELECT 
            token0_symbol as symbol,
            count() as swaps,
            sum(CASE WHEN amount0_usd > 0 AND amount0_usd < 1000000 THEN amount0_usd ELSE 0 END) as volume_usd
        FROM enriched_swaps_v2
        WHERE token0_symbol IS NOT NULL AND token0_symbol != ''
        GROUP BY token0_symbol
        ORDER BY volume_usd DESC
        LIMIT 10
    """).result_rows

    # Recent large swaps
    large_swaps = client.query("""
        SELECT 
            entity_id,
            protocol,
            chain,
            token0_symbol,
            token1_symbol,
            amount0_human,
            amount1_human,
            CASE 
                WHEN token0_symbol IN ('WETH', 'USDC') AND amount0_usd < 1000000 THEN amount0_usd
                WHEN token1_symbol IN ('WETH', 'USDC') AND amount1_usd < 1000000 THEN amount1_usd
                ELSE NULL
            END as volume_usd,
            timestamp
        FROM enriched_swaps_v2
        WHERE (token0_symbol IN ('WETH', 'USDC') OR token1_symbol IN ('WETH', 'USDC'))
        ORDER BY volume_usd DESC NULLS LAST
        LIMIT 20
    """).result_rows

    # Total volume
    totals = client.query("""
        SELECT 
            sum(CASE 
                WHEN token0_symbol IN ('WETH', 'USDC', 'USDbC', 'DAI') AND amount0_usd > 0 AND amount0_usd < 1000000 THEN amount0_usd
                WHEN token1_symbol IN ('WETH', 'USDC', 'USDbC', 'DAI') AND amount1_usd > 0 AND amount1_usd < 1000000 THEN amount1_usd
                ELSE 0 
            END) as total_volume_usd,
            countIf(token0_symbol IS NOT NULL OR token1_symbol IS NOT NULL) as enriched_swaps
        FROM enriched_swaps_v2
    """).result_rows[0]

    data = {
        "total_volume_usd": totals[0] or 0,
        "enriched_swaps": totals[1],
        "by_protocol": [
            {"protocol": r[0], "swaps": r[1], "volume_usd": r[2] or 0}
            for r in by_protocol
        ],
        "top_tokens": [
            {"symbol": r[0], "swaps": r[1], "volume_usd": r[2] or 0}
            for r in top_tokens
        ],
        "large_swaps": [
            {
                "wallet": r[0][:10] + "..." + r[0][-4:] if r[0] and len(r[0]) > 14 else r[0],
                "protocol": r[1],
                "chain": r[2],
                "token0": r[3],
                "token1": r[4],
                "amount0": r[5],
                "amount1": r[6],
                "volume_usd": r[7],
                "timestamp": str(r[8]),
            }
            for r in large_swaps
            if r[7] is not None
        ],
    }

    json.dump(data, sys.stdout)


if __name__ == "__main__":
    main()
