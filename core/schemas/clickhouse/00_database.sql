-- Runs first (alphabetical order) so the rest of the *.sql files can use
-- the `nexus.<table>` qualifier without depending on which session DB
-- ClickHouse picked. The docker entrypoint also sets CLICKHOUSE_DB=nexus
-- which already creates this DB; this is a belt-and-braces for non-docker
-- runs (e.g. `clickhouse-client < canonical_events.sql` against a fresh
-- instance with no `nexus` DB yet).
CREATE DATABASE IF NOT EXISTS nexus;
