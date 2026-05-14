"""FastAPI entry point for the BD dashboard."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from core.identity.bridge_status import classify as classify_bridge_row

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


@app.get("/api/bridge-flow/cross-chain-matrix")
def cross_chain_matrix(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    """Aggregated (src_chain, dst_chain) flow matrix from bridge_links."""
    return {"rows": _run(queries.cross_chain_matrix(days, None if chain == "all" else chain))}


@app.get("/api/bridges/explorer")
def bridges_explorer(
    hours: int = Query(24, ge=1, le=14 * 24),
    start: datetime | None = Query(None, description="UTC start timestamp; use with end"),
    end: datetime | None = Query(None, description="UTC end timestamp; use with start"),
    chains: str | None = Query(None, description="Comma-separated chain names"),
    bridges: str | None = Query(None, description="Comma-separated bridge slugs"),
    statuses: str | None = Query(None, description="Comma-separated statuses to keep (post-classification filter)"),
    limit: int = Query(500, ge=1, le=5000),
) -> dict:
    """Per-transaction bridge explorer.

    Returns up to `limit` rows, each pre-classified with a `status`
    (matched / pending / in_flight / unmatched-*) and a list of
    data-quality `tags`. Also returns a per-bridge punch-list summary.
    """
    chain_list = [c.strip() for c in chains.split(",")] if chains else None
    bridge_list = [b.strip() for b in bridges.split(",")] if bridges else None
    status_filter = {s.strip() for s in statuses.split(",")} if statuses else None
    if chain_list:
        for c in chain_list:
            if c not in queries.CHAINS:
                raise HTTPException(400, f"unknown chain: {c}")
    if (start is None) != (end is None):
        raise HTTPException(400, "start and end must be provided together")
    if start and end and start >= end:
        raise HTTPException(400, "start must be before end")

    start_sql = start.strftime("%Y-%m-%d %H:%M:%S") if start else None
    end_sql = end.strftime("%Y-%m-%d %H:%M:%S") if end else None
    enriched = []
    summary: dict[str, dict[str, int]] = {}
    if start_sql and end_sql and _run(queries.bridge_explorer_cache_count(start_sql, end_sql))[0]["cached_rows"] > 0:
        enriched = _run(
            queries.bridge_explorer_cached_rows(
                chain_list,
                bridge_list,
                status_filter,
                limit,
                start_sql,
                end_sql,
            )
        )
        for r in enriched:
            bucket = summary.setdefault(r.get("bridge") or "?", {})
            status = r.get("status")
            bucket[status] = bucket.get(status, 0) + 1
    else:
        sql = queries.bridge_explorer_rows(hours, chain_list, bridge_list, limit, start_sql, end_sql)
        rows = _run(sql)

        now = datetime.now(timezone.utc)
        for r in rows:
            verdict = classify_bridge_row(r, now=now)
            if status_filter and verdict["status"] not in status_filter:
                continue
            r["status"] = verdict["status"]
            r["tags"] = verdict["tags"]
            r["status_reason"] = verdict["reason"]
            enriched.append(r)
            bucket = summary.setdefault(r.get("bridge") or "?", {})
            bucket[verdict["status"]] = bucket.get(verdict["status"], 0) + 1

    return {
        "window_hours": hours,
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "row_count": len(enriched),
        "rows": enriched,
        "summary": summary,
    }


@app.get("/api/bridge-flow/completion")
def bridge_completion(
    days: int = Query(7, ge=1, le=30),
    chain: Chain = "all",
) -> dict:
    """% of bridge_outs that got matched to a bridge_in within 7d."""
    rows = _run(queries.bridge_completion(days, None if chain == "all" else chain))
    return rows[0] if rows else {}


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
