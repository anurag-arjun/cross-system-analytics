"""FastAPI entry point for the BD dashboard."""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from . import queries
from .ch import get_client, rows_to_dicts

app = FastAPI(
    title="Nexus Analytics BD API",
    description="Cross-system analytics — bridge flows + spike detection.",
    version="0.1.0",
)

# Permissive CORS for now — the frontend lives on a different port locally
# and on a different subdomain in prod. Lock down before exposing publicly.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


Chain = Literal["all", "ethereum", "base", "arbitrum", "optimism", "polygon"]
Alert = Literal["all", "extreme", "high"]


def _run(sql: str) -> list[dict]:
    client = get_client()
    return rows_to_dicts(client.query(sql))


@app.get("/healthz")
def healthz() -> dict:
    client = get_client()
    rows = client.query("SELECT 1").result_rows
    return {"ok": bool(rows), "clickhouse": "up" if rows else "down"}


@app.get("/api/meta/chains")
def meta_chains() -> dict:
    return {"chains": list(queries.CHAINS)}


# ---------------------------------------------------------------------------
# Bridge flow
# ---------------------------------------------------------------------------


@app.get("/api/bridge-flow/summary")
def bridge_summary(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    rows = _run(queries.bridge_summary(days, None if chain == "all" else chain))
    row = rows[0] if rows else {}
    total_actions = row.get("swaps", 0) + row.get("non_swap_defi", 0)
    return {
        "bridge_ins": row.get("bridge_ins", 0),
        "bridge_outs": row.get("bridge_outs", 0),
        "swaps": row.get("swaps", 0),
        "non_swap_defi": row.get("non_swap_defi", 0),
        "swap_share": round(row.get("swaps", 0) / total_actions, 3) if total_actions else 0.0,
    }


@app.get("/api/bridge-flow/breakdown")
def bridge_breakdown(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    return {"rows": _run(queries.bridge_breakdown(days, None if chain == "all" else chain))}


@app.get("/api/bridge-flow/first-action")
def first_action(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    return {"rows": _run(queries.first_action_after_bridge(days, None if chain == "all" else chain))}


@app.get("/api/bridge-flow/swap-vs-non-swap")
def swap_vs_non_swap(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    return {"rows": _run(queries.swap_vs_non_swap(days, None if chain == "all" else chain))}


@app.get("/api/bridge-flow/second-hop")
def second_hop(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    return {"rows": _run(queries.second_hop_after_swap(days, None if chain == "all" else chain))}


@app.get("/api/bridge-flow/activity-24h")
def activity_24h(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    return {"rows": _run(queries.activity_after_bridge_24h(days, None if chain == "all" else chain))}


@app.get("/api/bridge-flow/top-protocols-after-bridge")
def top_protocols_after_bridge(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    return {"rows": _run(queries.top_protocols_after_bridge_24h(days, None if chain == "all" else chain))}


# ---------------------------------------------------------------------------
# Spike detection
# ---------------------------------------------------------------------------


@app.get("/api/spikes/summary")
def spike_summary(days: int = Query(7, ge=1, le=30)) -> dict:
    rows = _run(queries.spike_summary(days))
    row = rows[0] if rows else {}
    return {
        "venues_tracked": row.get("venues_tracked", 0),
        "extreme_alerts": row.get("extreme", 0),
        "high_alerts": row.get("high", 0),
    }


@app.get("/api/spikes/hourly")
def spikes_hourly(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
    alert: Alert = "all",
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    return {"rows": _run(
        queries.hourly_spikes(
            days,
            None if chain == "all" else chain,
            None if alert == "all" else alert,
            limit,
        )
    )}


@app.get("/api/spikes/daily")
def spikes_daily(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
    alert: Alert = "all",
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    return {"rows": _run(
        queries.daily_spikes(
            days,
            None if chain == "all" else chain,
            None if alert == "all" else alert,
            limit,
        )
    )}


@app.get("/api/spikes/timeline")
def timeline(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    return {"rows": _run(queries.activity_timeline(days, None if chain == "all" else chain))}


@app.get("/api/spikes/protocols")
def protocols(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
    limit: int = Query(25, ge=1, le=200),
) -> dict:
    return {"rows": _run(queries.active_protocols(days, None if chain == "all" else chain, limit))}
