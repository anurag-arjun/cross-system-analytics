---
title: Bridge Flow Analytics
toc: false
---

# Bridge Flow Analytics

Post-bridge user behaviour — what happens after users bridge or deposit on Base.

```js
const bf = FileAttachment("data/bridge-flow.json").json();
```

<div class="grid grid-cols-4" style="grid-auto-rows: auto; gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Entry Events</h2>
    <span class="big">${bf.kpis.total_entries.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>Unique Wallets</h2>
    <span class="big">${bf.kpis.unique_wallets.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>Swap Within 1h</h2>
    <span class="big" style="color: #22c55e;">${bf.kpis.swap_pct}%</span>
  </div>
  <div class="card">
    <h2>Median Time to Action</h2>
    <span class="big">${bf.kpis.median_time_to_action_sec}s</span>
  </div>
</div>

## Path Analysis: What Do Users Do After Entry?

<div class="grid grid-cols-3" style="grid-auto-rows: auto; gap: 1rem; margin-bottom: 2rem;">
  <div class="card" style="border-left: 4px solid #22c55e;">
    <h2>Swap Within 1h</h2>
    <span class="big">${bf.path_analysis.find(p => p.type === "swap_within_1h")?.pct || 0}%</span>
    <span class="muted">${(bf.path_analysis.find(p => p.type === "swap_within_1h")?.count || 0).toLocaleString()} wallets</span>
  </div>
  <div class="card" style="border-left: 4px solid #3b82f6;">
    <h2>Non-Swap Activity</h2>
    <span class="big">${bf.path_analysis.find(p => p.type === "non_swap_activity_within_1h")?.pct || 0}%</span>
    <span class="muted">${(bf.path_analysis.find(p => p.type === "non_swap_activity_within_1h")?.count || 0).toLocaleString()} wallets</span>
  </div>
  <div class="card" style="border-left: 4px solid #ef4444;">
    <h2>Idle (No Activity)</h2>
    <span class="big">${bf.kpis.idle_pct}%</span>
    <span class="muted">No interaction within 1h</span>
  </div>
</div>

```js
Plot.plot({
  marks: [
    Plot.barY(bf.path_analysis, {
      x: "type",
      y: "count",
      fill: d => d.type === "swap_within_1h" ? "#22c55e" : d.type === "non_swap_activity_within_1h" ? "#3b82f6" : "#ef4444",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: null, tickFormat: d => d.replace("_within_1h", "").replace("_", " ") },
  y: { label: "Wallets" },
  marginBottom: 60,
  height: 250
})
```

## Top Immediate Apps (First Action After Entry)

```js
Plot.plot({
  marks: [
    Plot.barX(bf.top_immediate_apps, {
      y: d => `${d.protocol}/${d.event_type}`,
      x: "count",
      fill: "#6366f1",
      tip: true
    }),
    Plot.ruleX([0])
  ],
  x: { label: "Count" },
  y: { label: null },
  marginLeft: 160,
  height: 300
})
```

## Time to First Action (Seconds)

```js
Plot.plot({
  marks: [
    Plot.barY([
      {label: "P10", value: bf.time_to_action.p10},
      {label: "P25", value: bf.time_to_action.p25},
      {label: "Median", value: bf.time_to_action.median},
      {label: "P75", value: bf.time_to_action.p75},
      {label: "P90", value: bf.time_to_action.p90},
      {label: "P95", value: bf.time_to_action.p95},
    ], {
      x: "label",
      y: "value",
      fill: d => d.label === "Median" ? "#22c55e" : "#6366f1",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: null },
  y: { label: "Seconds" },
  height: 250
})
```

## 2nd Hop: What Happens After the First Swap?

For users who swap immediately after entry, what do they do next?

```js
Plot.plot({
  marks: [
    Plot.barX(bf.second_hop.slice(0, 10), {
      y: d => `${d.protocol}/${d.event_type}`,
      x: "count",
      fill: "#f59e0b",
      tip: true
    }),
    Plot.ruleX([0])
  ],
  x: { label: "Count" },
  y: { label: null },
  marginLeft: 160,
  height: 250
})
```

## 24-Hour Activity After Entry

```js
Plot.plot({
  marks: [
    Plot.barY(bf.hourly_activity, {
      x: d => `H+${d.hour}`,
      y: "events",
      fill: "#8b5cf6",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: "Hours After Entry", tickRotate: -45 },
  y: { label: "Events" },
  marginBottom: 60,
  height: 250
})
```

## Entry Events by Protocol

```js
Inputs.table(bf.entries, {
  columns: ["protocol", "type", "chain", "count"],
  header: {
    protocol: "Protocol",
    type: "Event Type",
    chain: "Chain",
    count: "Count"
  }
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
