-- /core/schemas/protocol_abis.sql
-- Postgres DDL for cached event/function ABIs, content-addressed by the
-- keccak256 hash of the deployed bytecode. Many factory-deployed contracts
-- (every UniV3 pool, every Aave V3 reserve) share an implementation ABI —
-- keying by code_hash means we fetch each unique implementation exactly once.

CREATE TABLE IF NOT EXISTS protocol_abis (
    code_hash  TEXT        NOT NULL PRIMARY KEY,
    abi_json   TEXT        NOT NULL,           -- JSON-encoded ABI (string of array)
    source     TEXT        NOT NULL,           -- 'etherscan' | 'sourcify' | 'manual'
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
