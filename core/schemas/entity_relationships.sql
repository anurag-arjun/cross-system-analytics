-- /core/schemas/entity_relationships.sql
-- Identity graph: links entities across systems with confidence scores.

CREATE TABLE IF NOT EXISTS entity_relationships (
  from_entity        String,
  from_entity_type   LowCardinality(String),
  to_entity          String,
  to_entity_type     LowCardinality(String),
  relationship_type  LowCardinality(String),  -- 'owns', 'resolved_to', 'linked_via_ens'
  confidence_score   Float32,                 -- 0.0 - 1.0
  source             LowCardinality(String),  -- 'ens', 'farcaster', 'ga4_client_id'
  detected_at        DateTime64(3),
  expires_at         Nullable(DateTime64(3))  -- for transient links
) ENGINE = MergeTree
ORDER BY (from_entity, relationship_type, to_entity);
