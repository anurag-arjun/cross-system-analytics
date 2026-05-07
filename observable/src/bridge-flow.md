---
title: Bridge Flow
toc: false
---

# Bridge Flow Analytics

Post-bridge user behavior — first actions, swap paths, 24h activity after bridging.

```js
const bf = FileAttachment("data/bridge-flow.json").json();
```

<div class="grid grid-cols-4" style="gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Bridge In Events</h2>
    <span class="big">${bf.total_bridge_ins.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>Bridge Out Events</h2>
    <span class="big">${bf.total_bridge_outs.toLocaleString()}</span>
  </div>
  <div class="card" style="border-left: 4px solid #22c55e;">
    <h2>Swap After Bridge</h2>
    <span class="big" style="color: #22c55e;">${bf.swap_split.swap_pct}%</span>
    <span class="muted">${bf.swap_split.swap} wallets swapped within 1h</span>
  </div>
  <div class="card" style="border-left: 4px solid #f59e0b;">
    <h2>Non-Swap After Bridge</h2>
    <span class="big" style="color: #f59e0b;">${(100 - bf.swap_split.swap_pct).toFixed(1)}%</span>
    <span class="muted">${bf.swap_split.non_swap} wallets</span>
  </div>
</div>

<div class="banner">
  <strong>📊 Data Coverage:</strong> ${bf.total_bridge_ins + bf.total_bridge_outs} bridge events across ${[...new Set(bf.bridges.map(b => b.chain))].join(', ')}. Pipeline expanding to 6+ chains.
</div>

## First Action After Bridge

What apps did users immediately interact with after bridging to a chain?

```js
Inputs.table(bf.first_actions, {
  columns: ["bridge_protocol", "next_protocol", "next_type", "count", "median_sec"],
  format: {
    median_sec: d => d > 60 ? `${(d/60).toFixed(1)}m` : `${d}s`
  },
  header: {
    bridge_protocol: "Bridge Protocol",
    next_protocol: "First App",
    next_type: "Action",
    count: "Users",
    median_sec: "Median Time"
  },
  sort: "count",
  reverse: true
})
```

<div class="grid grid-cols-2" style="gap: 1.5rem; margin: 2rem 0;">

## Swap vs Non-Swap Split

Action after bridge that allows swaps vs doesn't.

```js
Plot.plot({
  marks: [
    Plot.barY([
      {type: "Swap", count: bf.swap_split.swap, fill: "#22c55e"},
      {type: "Non-Swap", count: bf.swap_split.non_swap, fill: "#f59e0b"}
    ], {
      x: "type", y: "count", fill: "fill", tip: true
    }),
    Plot.ruleY([0])
  ],
  color: {type: "identity"},
  x: {label: null},
  y: {label: "Wallets", grid: true},
  height: 250
})
```

## 2nd Hop After Swap

If they swapped on another app — what did they do after?

```js
Inputs.table(bf.second_hops, {
  columns: ["protocol", "event_type", "count"],
  header: {
    protocol: "Protocol",
    event_type: "Second Action",
    count: "Count"
  },
  sort: "count",
  reverse: true
})
```

</div>

## 24h Activity After Bridge

What apps did users interact with in the 24 hours after bridging?

```js
Plot.plot({
  marks: [
    Plot.barY(bf.hourly_24h, {
      x: d => `H+${d.hour}`,
      y: "events",
      fill: "#8b5cf6",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: {label: "Hours After Bridge", tickRotate: -45},
  y: {label: "Events", grid: true},
  marginBottom: 60,
  height: 250
})
```

## Top Protocols After Bridge (24h Window)

```js
Plot.plot({
  marks: [
    Plot.barY(bf.post_bridge_protocols.slice(0, 15), {
      x: d => `${d.protocol}/${d.event_type}`,
      y: "count",
      fill: "#6366f1",
      tip: true,
      sort: {x: "y", reverse: true}
    }),
    Plot.ruleY([0])
  ],
  x: {label: null, tickRotate: -30},
  y: {label: "Interactions", grid: true},
  marginBottom: 80,
  height: 300
})
```

## Bridge Events by Protocol

```js
Inputs.table(bf.bridges, {
  columns: ["protocol", "type", "chain", "count"],
  header: {
    protocol: "Protocol",
    type: "Type",
    chain: "Chain",
    count: "Count"
  },
  sort: "count",
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
