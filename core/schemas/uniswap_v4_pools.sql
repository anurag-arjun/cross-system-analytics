-- UniV4 pool registry. Populated from PoolManager Initialize events.
-- Per-pool tokens + fee + tickSpacing + hooks resolved at decode time so the
-- Swap event can look up token0/token1 via the indexed PoolId.

CREATE TABLE IF NOT EXISTS uniswap_v4_pools (
    pool_id          TEXT NOT NULL,
    chain            TEXT NOT NULL,
    pool_manager     TEXT NOT NULL,
    currency0        TEXT NOT NULL,
    currency1        TEXT NOT NULL,
    fee              INTEGER NOT NULL,
    tick_spacing     INTEGER NOT NULL,
    hooks            TEXT NOT NULL,
    init_block       BIGINT NOT NULL,
    init_block_time  TIMESTAMPTZ NOT NULL,
    init_tx_hash     TEXT NOT NULL,
    added_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, pool_id)
);

CREATE INDEX IF NOT EXISTS uniswap_v4_pools_currencies_idx
    ON uniswap_v4_pools (chain, currency0, currency1);
