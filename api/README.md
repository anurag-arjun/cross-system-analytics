# Nexus BD API

FastAPI service for the BD dashboard. Stateless read-only over ClickHouse.

## Run locally

```bash
cd api
pip install -e .
# .env in project root must have CLICKHOUSE_* (defaults to localhost:8124/nexus)
uvicorn api.main:app --reload --port 8000
```

Open http://localhost:8000/docs for the auto-generated Swagger UI.

## Endpoints

### Bridge flow

| Path | Params | Returns |
|---|---|---|
| `GET /api/bridge-flow/summary` | `days`, `chain` | KPI counts (bridge_ins/outs, swaps, non_swap_defi, swap_share) |
| `GET /api/bridge-flow/breakdown` | `days`, `chain` | Bridge events by protocol + chain |
| `GET /api/bridge-flow/first-action` | `days`, `chain` | First non-bridge action after each bridge_in (24h window) |
| `GET /api/bridge-flow/swap-vs-non-swap` | `days`, `chain` | Bar-chart split |
| `GET /api/bridge-flow/second-hop` | `days`, `chain` | What did users do after their first swap-after-bridge? |
| `GET /api/bridge-flow/activity-24h` | `days`, `chain` | Hour-offset activity in the 24h after each bridge |
| `GET /api/bridge-flow/top-protocols-after-bridge` | `days`, `chain` | Most-used protocols in the 24h-post-bridge window |

### Spike detection

| Path | Params | Returns |
|---|---|---|
| `GET /api/spikes/summary` | `days` | KPI counts |
| `GET /api/spikes/hourly` | `days`, `chain`, `alert`, `limit` | Hourly spike rows |
| `GET /api/spikes/daily` | `days`, `chain`, `alert`, `limit` | Daily spike rows (7d rolling baseline) |
| `GET /api/spikes/timeline` | `days`, `chain` | Hourly event totals timeline |
| `GET /api/spikes/protocols` | `days`, `chain`, `limit` | Active-protocol leaderboard |

### Meta

| Path | Returns |
|---|---|
| `GET /healthz` | `{ok: true, clickhouse: "up"}` |
| `GET /api/meta/chains` | `{chains: ["ethereum", ...]}` |

## Common params

- `days` — window in days, 1-30. Default 7.
- `chain` — `all` (default) or one of `ethereum / base / arbitrum / optimism / polygon`.
- `alert` — `all` (default), `extreme`, or `high`.
