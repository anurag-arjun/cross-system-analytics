-- /core/schemas/contract_labels.sql
-- Postgres DDL for human-readable address labels (DEX, lending, CEX, etc.).
-- Populated by the Dune bootstrap export (na-7p8s) via JOIN against the
-- known address universe in protocol_contracts.
--
-- Composite key (chain, address, source) lets dune + manual labels coexist.

CREATE TABLE IF NOT EXISTS contract_labels (
    chain      TEXT        NOT NULL,
    address    TEXT        NOT NULL,
    label      TEXT        NOT NULL,
    category   TEXT,
    source     TEXT        NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, address, source)
);

CREATE INDEX IF NOT EXISTS contract_labels_chain_address_idx
    ON contract_labels (chain, address);
