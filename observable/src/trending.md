---
title: Trending Contracts
toc: false
---

# Trending Contracts & App Activity

Spike detection on hourly rolling averages. Contracts with >200% wallet activity spikes flagged automatically.

```js
const trending = FileAttachment("data/trending.json").json();
```

<div class="grid grid-cols-4" style="grid-auto-rows: auto; gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Contracts Tracked</h2>
    <span class="big">${trending.kpis.contracts_tracked.toLocaleString()}</span>
  </div>
  <div class="card" style="border-left: 4px solid #ef4444;">
    <h2>Extreme Spikes</h2>
    <span class="big" style="color: #ef4444;">${trending.kpis.extreme_alerts}</span>
    <span class="muted">&ge;400% vs rolling avg</span>
  </div>
  <div class="card" style="border-left: 4px solid #f59e0b;">
    <h2>High Spikes</h2>
    <span class="big" style="color: #f59e0b;">${trending.kpis.high_alerts}</span>
    <span class="muted">&ge;200% vs rolling avg</span>
  </div>
  <div class="card">
    <h2>Protocols</h2>
    <span class="big">${trending.kpis.protocols_tracked}</span>
  </div>
</div>

## Alert Level Distribution

```js
Plot.plot({
  marks: [
    Plot.barY([
      {level: "Extreme", count: trending.kpis.extreme_alerts, color: "#ef4444"},
      {level: "High", count: trending.kpis.high_alerts, color: "#f59e0b"},
      {level: "Moderate", count: trending.kpis.moderate_alerts, color: "#3b82f6"},
    ], {
      x: "level",
      y: "count",
      fill: "color",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  color: {type: "identity"},
  height: 200
})
```

## Top Trending Contracts (Extreme Spikes)

Contracts with the highest wallet activity spikes vs 24h rolling average.

```js
Inputs.table(trending.extreme.slice(0, 25), {
  columns: ["contract", "chain", "protocol", "events", "wallets", "events_spike", "wallets_spike", "volume"],
  format: {
    contract: d => `${d.slice(0, 10)}...`,
    events_spike: d => `${d}x`,
    wallets_spike: d => `${d}x`,
    volume: d => d > 0 ? `$${d.toLocaleString()}` : "-"
  },
  header: {
    contract: "Contract",
    chain: "Chain",
    protocol: "Protocol",
    events: "Events",
    wallets: "Wallets",
    events_spike: "Events Spike",
    wallets_spike: "Wallets Spike",
    volume: "Volume"
  },
  sort: "wallets_spike",
  reverse: true
})
```

## Spike Ratios (Events vs Wallets)

```js
Plot.plot({
  marks: [
    Plot.dot(trending.extreme, {
      x: "events_spike",
      y: "wallets_spike",
      r: "wallets",
      fill: "chain",
      opacity: 0.6,
      tip: true,
      title: d => `${d.contract.slice(0, 12)}..\nEvents: ${d.events_spike}x\nWallets: ${d.wallets_spike}x`
    }),
    Plot.ruleX([2], {stroke: "#f59e0b", strokeDasharray: "4,4"}),
    Plot.ruleX([4], {stroke: "#ef4444", strokeDasharray: "4,4"}),
    Plot.ruleY([2], {stroke: "#f59e0b", strokeDasharray: "4,4"}),
    Plot.ruleY([4], {stroke: "#ef4444", strokeDasharray: "4,4"}),
  ],
  x: { label: "Events Spike Ratio", type: "log" },
  y: { label: "Wallets Spike Ratio", type: "log" },
  color: { legend: true },
  height: 400
})
```

## Protocol Breakdown

```js
Plot.plot({
  marks: [
    Plot.barY(trending.protocols.filter(p => p.wallets > 100), {
      x: d => `${d.protocol} (${d.chain})`,
      y: "wallets",
      fill: "#6366f1",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: null, tickRotate: -45 },
  y: { label: "Total Unique Wallets" },
  marginBottom: 80,
  height: 300
})
```

## Hourly Activity Timeline

```js
Plot.plot({
  marks: [
    Plot.barY(trending.timeline, {
      x: d => d.hour.slice(11, 16),
      y: "events",
      fill: "#8b5cf6",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: "Hour (UTC)" },
  y: { label: "Total Events" },
  height: 250
})
```

## Top Alerts (High + Extreme)

```js
Inputs.table(trending.trending.slice(0, 30), {
  columns: ["contract", "chain", "protocol", "events", "wallets", "events_spike", "wallets_spike", "alert"],
  format: {
    contract: d => `${d.slice(0, 10)}...`,
    events_spike: d => `${d}x`,
    wallets_spike: d => `${d}x`
  },
  header: {
    contract: "Contract",
    chain: "Chain",
    protocol: "Protocol",
    events: "Events",
    wallets: "Wallets",
    events_spike: "Events Spike",
    wallets_spike: "Wallets Spike",
    alert: "Alert"
  },
  sort: "wallets_spike",
  reverse: true
})
```

<style>
.card {
  background: var(--theme-background-alt);
  border-radius: 8px;
  padding: 1.5rem;
}
.card h2 {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--theme-foreground-muted);
  margin: 0 0 0.5rem;
}
.big {
  font-size: 2rem;
  font-weight: 600;
}
.muted {
  font-size: 0.8rem;
  color: var(--theme-foreground-muted);
}
</style>
