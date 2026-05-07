---
title: Nexus Analytics
toc: false
---

# Nexus Analytics

Cross-chain behavioral analytics for Web3 — 1.3M events, 54K wallets, real-time.

<div style="margin-bottom: 1.5rem; display: flex; gap: 1rem; flex-wrap: wrap;">
  <a href="/cross-chain" style="padding: 0.6rem 1.2rem; background: #6366f1; color: white; border-radius: 6px; text-decoration: none; font-weight: 500;">Cross-Chain →</a>
  <a href="/bridge-flow" style="padding: 0.6rem 1.2rem; background: #22c55e; color: white; border-radius: 6px; text-decoration: none; font-weight: 500;">Bridge Flow →</a>
  <a href="/protocols" style="padding: 0.6rem 1.2rem; background: #f59e0b; color: white; border-radius: 6px; text-decoration: none; font-weight: 500;">Protocol Analytics →</a>
  <a href="/spikes" style="padding: 0.6rem 1.2rem; background: #ef4444; color: white; border-radius: 6px; text-decoration: none; font-weight: 500;">Trending Contracts →</a>
</div>

```js
const overview = FileAttachment("data/overview.json").json();
```

<div class="grid grid-cols-4" style="gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Total Events</h2>
    <span class="big">${overview.kpis.total_events.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>Unique Wallets</h2>
    <span class="big">${overview.kpis.unique_wallets.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>DEX Swaps</h2>
    <span class="big">${overview.kpis.total_swaps.toLocaleString()}</span>
  </div>
  <div class="card" style="border-left: 4px solid #6366f1;">
    <h2>Cross-Chain Wallets</h2>
    <span class="big" style="color: #6366f1;">${overview.kpis.cross_chain_wallets.toLocaleString()}</span>
    <span class="muted">${overview.kpis.cross_chain_pct}% of all wallets</span>
  </div>
</div>

## Activity by Chain

```js
Plot.plot({
  marks: [
    Plot.barX(overview.by_chain, {
      y: "chain",
      x: "events",
      fill: "chain",
      tip: true,
      sort: {y: "x", reverse: true}
    }),
    Plot.ruleX([0])
  ],
  x: { label: "Events", grid: true },
  y: { label: null },
  color: { legend: false },
  marginLeft: 80,
  height: 120
})
```

<div class="grid grid-cols-2" style="gap: 1.5rem; margin: 2rem 0;">

## Protocol Market Share

```js
Plot.plot({
  marks: [
    Plot.barY(overview.by_protocol, {
      x: "protocol",
      y: "swaps",
      fill: "#6366f1",
      tip: true,
      sort: {x: "y", reverse: true}
    }),
    Plot.ruleY([0])
  ],
  x: { label: null, tickRotate: -30 },
  y: { label: "Swaps", grid: true },
  marginBottom: 80,
  height: 300
})
```

## Event Type Breakdown

```js
Plot.plot({
  marks: [
    Plot.barX(overview.by_event.slice(0, 8), {
      y: "event_type",
      x: "count",
      fill: "#f59e0b",
      tip: true,
      sort: {y: "x", reverse: true}
    }),
    Plot.ruleX([0])
  ],
  x: { label: "Count", grid: true },
  y: { label: null },
  marginLeft: 120,
  height: 300
})
```

</div>

## Hourly Activity (Last 48h)

```js
Plot.plot({
  marks: [
    Plot.lineY(overview.hourly, {
      x: "hour",
      y: "events",
      stroke: "#6366f1",
      tip: true
    }),
    Plot.lineY(overview.hourly, {
      x: "hour",
      y: "swaps",
      stroke: "#22c55e",
      tip: true
    })
  ],
  x: { label: null, tickRotate: -30 },
  y: { label: "Count", grid: true },
  color: { legend: true, domain: ["Events", "Swaps"] },
  marginBottom: 60,
  height: 250
})
```

## Cross-Chain Distribution

```js
Plot.plot({
  marks: [
    Plot.barY(overview.cross_chain.distribution, {
      x: d => `${d.chains} chain${d.chains > 1 ? 's' : ''}`,
      y: "wallets",
      fill: d => d.chains > 1 ? "#6366f1" : "#374151",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: null },
  y: { label: "Wallets", grid: true },
  height: 200
})
```

<div class="grid grid-cols-2" style="gap: 1rem; margin: 2rem 0;">
  <div class="card">
    <h2>Chains Active</h2>
    <span class="big">${overview.kpis.chains_active}</span>
  </div>
  <div class="card">
    <h2>Snapshot Window</h2>
    <span class="big" style="font-size: 1rem;">${overview.kpis.first_event.slice(0, 10)} → ${overview.kpis.last_event.slice(0, 10)}</span>
    <span class="muted">Single ingestion run · pipeline ready for continuous refresh</span>
  </div>
</div>

<div style="background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.3); border-radius: 8px; padding: 1rem; margin-bottom: 1rem; font-size: 0.85rem;">
  <strong>🟡 Demo Snapshot:</strong> 12-day ingestion from 2 of 4 chains (Base + Arbitrum). Ethereum &amp; Optimism ingestion, hourly price refresh, and bridge matching are built — awaiting continuous pipeline activation.
</div>

<style>
.card {
  background: var(--theme-background-alt);
  border-radius: 8px;
  padding: 1.5rem;
}
.card h2 {
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--theme-foreground-muted);
  margin: 0 0 0.5rem;
}
.big {
  font-size: 2rem;
  font-weight: 700;
}
.muted {
  display: block;
  font-size: 0.8rem;
  color: var(--theme-foreground-muted);
  margin-top: 0.25rem;
}
</style>
