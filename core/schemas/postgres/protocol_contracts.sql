-- /core/schemas/protocol_contracts.sql
-- Postgres DDL for the contract -> protocol registry consumed by the
-- generic ABI decoder framework (na-8490). The decoder asks
-- "what protocol does this (chain, address) belong to?" and dispatches
-- to the matching YAML mapping.
--
-- Composite key (chain, address, source) lets entries from different
-- sources (spellbook seeds, Dune dex.trades / labels.addresses, manual)
-- coexist for the same address. The application is responsible for
-- deciding which source to trust if they disagree on protocol/version.

CREATE TABLE IF NOT EXISTS protocol_contracts (
    chain         TEXT        NOT NULL,
    address       TEXT        NOT NULL,
    protocol      TEXT        NOT NULL,
    version       TEXT,
    contract_type TEXT,
    source        TEXT        NOT NULL,
    added_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, address, source)
);

CREATE INDEX IF NOT EXISTS protocol_contracts_chain_address_idx
    ON protocol_contracts (chain, address);

CREATE INDEX IF NOT EXISTS protocol_contracts_protocol_idx
    ON protocol_contracts (protocol);
