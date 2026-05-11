"""FastAPI service for the BD dashboard.

Stateless read-only API over `canonical_events` in ClickHouse. Two
namespaces matching the BD requirements doc:

- /api/bridge-flow/* — bridge flow analytics
- /api/spikes/*      — contract/app spike detection

No auth in MVP — the live URL is for an internal BD audience and the
service queries pre-aggregated data only.
"""
