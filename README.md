# Nexus Analytics

Unified behavioral analytics for cross-system journeys. Crypto is our first market.

## Repository Structure

```
/
├── core/          # MIT-destined. Generic platform.
│   ├── adapters/  # Ingestion adapters (EVM, GA4, PostHog)
│   ├── schemas/   # Canonical event schema + registry
│   ├── identity/  # Identity graph pipeline
│   ├── trajectory/# Trajectory query engine
│   └── ui/        # Headless components
├── avail/         # Proprietary. Avail-specific surfaces.
│   ├── nexus_cs/  # Integrator CS tool
│   ├── fastbridge/# Marketing analytics
│   └── gtm/       # Prospect scoring
└── commercial/    # Future enterprise tier.
```

## Quick Start

```bash
# Start local infrastructure
docker-compose up -d

# Run tests
cd core && pytest

# Run weekly architectural guardrails
./ops/ci/weekly_arch_tests.sh
```

## Product Plan

See [PRODUCT_PLAN.md](PRODUCT_PLAN.md).

## Engineering Plan

See [ENGINEERING_PLAN.md](ENGINEERING_PLAN.md).
