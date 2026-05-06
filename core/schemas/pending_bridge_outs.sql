-- /core/schemas/pending_bridge_outs.sql
-- Postgres table for unmatched bridge_out events.
-- Supports bridge-family-specific retry cadences and 30-day expiry.
-- Cross-chain depositId collisions are prevented by including src_chain in lookups.

CREATE TABLE IF NOT EXISTS pending_bridge_outs (
    id SERIAL PRIMARY KEY,

    -- Canonical event identity
    event_id VARCHAR(64) NOT NULL UNIQUE,
    entity_id VARCHAR(64) NOT NULL,
    entity_type VARCHAR(32) NOT NULL DEFAULT 'wallet',

    -- Bridge matching key (composite with src_chain prevents collisions)
    link_key VARCHAR(256) NOT NULL,
    link_key_type VARCHAR(64) NOT NULL,
    src_chain VARCHAR(32) NOT NULL,

    -- Source event metadata
    src_block_time TIMESTAMP WITH TIME ZONE NOT NULL,
    src_tx_hash VARCHAR(66) NOT NULL,
    src_event_id VARCHAR(64) NOT NULL,
    token VARCHAR(42),
    amount VARCHAR(78),

    -- Retry scheduling
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Lifecycle timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    matched_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Optimise the three main query patterns:
-- 1. Retry scheduler: find pending rows ready for retry
CREATE INDEX idx_pending_bridge_outs_status_retry
    ON pending_bridge_outs(status, next_retry_at);

-- 2. Bridge matching: look up candidates by link_key + link_key_type
CREATE INDEX idx_pending_bridge_outs_link_key
    ON pending_bridge_outs(link_key, link_key_type, status)
    WHERE status = 'pending';

-- 3. Expiry cleanup: find expired pending rows
CREATE INDEX idx_pending_bridge_outs_expires
    ON pending_bridge_outs(expires_at)
    WHERE status = 'pending';
