-- /core/schemas/contract_bytecodes.sql
-- Postgres DDL mapping (chain, address) to its bytecode hash. The hash is
-- the join key into protocol_abis. EIP-1967 proxies have an additional
-- implementation_address; for those, abi lookup uses the implementation's
-- code_hash, not the proxy's.

CREATE TABLE IF NOT EXISTS contract_bytecodes (
    chain                  TEXT        NOT NULL,
    address                TEXT        NOT NULL,
    code_hash              TEXT        NOT NULL,        -- keccak256 of deployed bytecode
    is_proxy               BOOLEAN     NOT NULL DEFAULT FALSE,
    implementation_address TEXT,                        -- EIP-1967 impl, if proxy
    fetched_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, address)
);

CREATE INDEX IF NOT EXISTS contract_bytecodes_code_hash_idx
    ON contract_bytecodes (code_hash);
