---
title: Trending Contracts
toc: false
---

# Trending Contracts & Spike Detection

Apps/contracts with &gt;200% increase in hourly or daily interactions vs rolling average.

```js
const spikes = FileAttachment("data/spikes.json").json();
```

<div class="grid grid-cols-4" style="gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Venues Tracked</h2>
    <span class="big">${spikes.kpis.venues_tracked.toLocaleString()}</span>
  </div>
  <div class="card" style="border-left: 4px solid #ef4444;">
    <h2>Extreme Spikes</h2>
    <span class="big" style="color: #ef4444;">${spikes.kpis.extreme_alerts}</span>
    <span class="muted">&ge;400% vs rolling avg</span>
  </div>
  <div class="card" style="border-left: 4px solid #f59e0b;">
    <h2>High Spikes</h2>
    <span class="big" style="color: #f59e0b;">${spikes.kpis.high_alerts}</span>
    <span class="muted">&ge;200% vs rolling avg</span>
  </div>
  <div class="card">
    <h2>Protocols</h2>
    <span class="big">${spikes.kpis.protocols_tracked}</span>
  </div>
</div>

<div class="banner">
  <strong>📊 Methodology:</strong> Compares each hour's event/wallet count vs 24h rolling average. &ge;200% = high, &ge;400% = extreme. <em>Current data: 2 chains, 7-day window. Expanding with multi-chain ingestion.</em>
</div>

## Extreme Spikes (&ge;400%)

```js
Inputs.table(spikes.extreme, {
  columns: ["venue", "protocol", "chain", "events", "wallets", "events_ratio", "wallets_ratio"],
  format: {
    events_ratio: d => `${d}x`,
    wallets_ratio: d => `${d}x`
  },
  header: {
    venue: "Contract",
    protocol: "Protocol",
    chain: "Chain",
    events: "Events",
    wallets: "Wallets",
    events_ratio: "Events Spike",
    wallets_ratio: "Wallets Spike"
  },
  sort: "wallets_ratio",
  reverse: true
})
```

## High Spikes (&ge;200%)

```js
Inputs.table(spikes.high, {
  columns: ["venue", "protocol", "chain", "events", "wallets", "events_ratio", "wallets_ratio"],
  format: {
    events_ratio: d => `${d}x`,
    wallets_ratio: d => `${d}x`
  },
  header: {
    venue: "Contract",
    protocol: "Protocol",
    chain: "Chain",
    events: "Events",
    wallets: "Wallets",
    events_ratio: "Events Spike",
    wallets_ratio: "Wallets Spike"
  },
  sort: "wallets_ratio",
  reverse: true
})
```

## Spike Ratios (Events vs Wallets)

```js
Plot.plot({
  marks: [
    Plot.dot(spikes.spikes, {
      x: "events_ratio",
      y: "wallets_ratio",
      r: "events",
      fill: "alert",
      opacity: 0.6,
      tip: true,
      title: d => `${d.venue}\nEvents: ${d.events_ratio}x\nWallets: ${d.wallets_ratio}x`
    }),
    Plot.ruleX([2], {stroke: "#f59e0b", strokeDasharray: "4,4"}),
    Plot.ruleX([4], {stroke: "#ef4444", strokeDasharray: "4,4"}),
    Plot.ruleY([2], {stroke: "#f59e0b", strokeDasharray: "4,4"}),
    Plot.ruleY([4], {stroke: "#ef4444", strokeDasharray: "4,4"})
  ],
  x: {label: "Events Spike Ratio →", type: "log"},
  y: {label: "Wallets Spike Ratio →", type: "log"},
  color: {legend: true, domain: ["extreme", "high", "normal"]},
  height: 400
})
```

## Active Protocols by Venue Count

```js
Plot.plot({
  marks: [
    Plot.barY(spikes.protocols, {
      x: d => `${d.protocol}/${d.chain}`,
      y: "venues",
      fill: "#6366f1",
      tip: true,
      sort: {x: "y", reverse: true}
    }),
    Plot.ruleY([0])
  ],
  x: {label: null, tickRotate: -30},
  y: {label: "Venues", grid: true},
  marginBottom: 80,
  height: 300
})
```

## Hourly Activity (Last 7 Days)

```js
Plot.plot({
  marks: [
    Plot.lineY(spikes.timeline, {
      x: d => d.hour.slice(0, 16),
      y: "events",
      stroke: "#6366f1",
      tip: true
    })
  ],
  x: {label: null, tickRotate: -30},
  y: {label: "Events/Hour", grid: true},
  marginBottom: 60,
  height: 250
})
```

<style>
.card {
  background: var(--theme-background-alt);
  border-radius: 8px;
  padding: 1.5rem;
}
.card h2 {
  font-size: 0.75rem; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.05em; color: var(--theme-foreground-muted); margin: 0 0 0.5rem;
}
.big { font-size: 2rem; font-weight: 700; }
.muted { display: block; font-size: 0.8rem; color: var(--theme-foreground-muted); margin-top: 0.25rem; }
.banner {
  background: rgba(99, 102, 241, 0.1); border: 1px solid rgba(99, 102, 241, 0.3);
  border-radius: 8px; padding: 1rem; margin-bottom: 1.5rem; font-size: 0.85rem;
}
</style>
