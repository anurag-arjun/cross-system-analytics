# FastBridge Marketing Analytics

Unified funnel view joining GA4 acquisition data to onchain events.

## Components

- `connectors/` — GA4/PostHog adapter configs for FastBridge
- `funnels/` — Unified funnel views (GA4 → bridge → swap)
- `attribution/` — Campaign attribution logic

## Key Queries

- "Which Twitter campaign drove users who bridged to Base and swapped?"
- "What % of users who read docs never connect a wallet?"
- "Channel comparison by downstream on-chain value"
