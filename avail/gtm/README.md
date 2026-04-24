# GTM / ICP Scoring

Wallet-level prospect ranking and BD pipeline management.

## Components

- `scoring/` — ICP weights + prospect ranking algorithm
- `segments/` — Segment generation rules
- `crm_sync/` — Postgres prospects + outreach_log + attribution loop

## Key Concepts

**Prospect**: A wallet address scored for integration-fit.
**Segment**: A filtered list of prospects (e.g., ">$10k bridged to Base, no Avail interaction").
**Attribution loop**: Dagster job joining outreach_log to canonical_events.
