---
title: Nexus Analytics Dashboard
toc: false
---

# Nexus Analytics

Real-time cross-chain behavioral analytics for Web3.

<div style="margin-bottom: 1rem;">
  <a href="./bridge-flow" style="padding: 0.5rem 1rem; background: #6366f1; color: white; border-radius: 6px; text-decoration: none; font-weight: 500;">Bridge Flow Analytics →</a>
  <a href="./trending" style="padding: 0.5rem 1rem; background: #f59e0b; color: white; border-radius: 6px; text-decoration: none; font-weight: 500; margin-left: 0.5rem;">Trending Contracts →</a>
</div>

```js
const stats = FileAttachment("data/stats.json").json();
const events = FileAttachment("data/events.json").json();
const trajectories = FileAttachment("data/trajectories.json").json();
const volume = FileAttachment("data/volume.json").json();
```

<div class="grid grid-cols-4" style="grid-auto-rows: auto; gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Total Events</h2>
    <span class="big">${stats.total_events.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>Unique Wallets</h2>
    <span class="big">${stats.unique_wallets.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>DEX Volume (30m)</h2>
    <span class="big">$${(volume.total_volume_usd / 1000000).toFixed(2)}M</span>
  </div>
  <div class="card">
    <h2>Cross-Chain Users</h2>
    <span class="big">${stats.cross_chain_wallets.toLocaleString()}</span>
  </div>
</div>

## Activity by Chain

```js
Plot.plot({
  marks: [
    Plot.barX(stats.by_chain, {
      y: "chain",
      x: "swaps",
      fill: "chain",
      tip: true
    }),
    Plot.ruleX([0])
  ],
  x: { label: "Swaps" },
  y: { label: null },
  color: { legend: false },
  marginLeft: 80,
  height: 150
})
```

## Protocol Distribution

```js
Plot.plot({
  marks: [
    Plot.barY(stats.by_protocol, {
      x: "protocol",
      y: "swaps",
      fill: "steelblue",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: null, tickRotate: -45 },
  y: { label: "Swaps" },
  marginBottom: 80,
  height: 300
})
```

## Event Type Breakdown

```js
Plot.plot({
  marks: [
    Plot.barX(stats.by_event.slice(0, 6), {
      y: "event_type",
      x: "count",
      fill: "#6366f1",
      tip: true
    }),
    Plot.ruleX([0])
  ],
  x: { label: "Count" },
  y: { label: null },
  marginLeft: 100,
  height: 200
})
```

## Sample User Trajectories

Select a wallet to see their cross-chain journey:

```js
const walletSelect = view(Inputs.select(trajectories, {
  format: t => `${t.wallet} (${t.event_count} events, ${t.chain_count} chains)`,
  label: "Wallet"
}));
```

```js
const selectedTrajectory = walletSelect;
```

<div class="card" style="padding: 1rem;">
<strong>Wallet:</strong> <code>${selectedTrajectory.wallet_full}</code><br/>
<strong>Events:</strong> ${selectedTrajectory.event_count} | <strong>Chains:</strong> ${selectedTrajectory.chain_count}
</div>

```js
Inputs.table(selectedTrajectory.events, {
  columns: ["timestamp", "chain", "event_type", "protocol"],
  header: {
    timestamp: "Time",
    chain: "Chain", 
    event_type: "Event",
    protocol: "Protocol"
  }
})
```

## Top Swaps by Volume

```js
Inputs.table(volume.large_swaps.slice(0, 15), {
  columns: ["timestamp", "wallet", "protocol", "token0", "token1", "volume_usd"],
  format: {
    volume_usd: x => x ? `$${x.toLocaleString(undefined, {maximumFractionDigits: 0})}` : "-"
  },
  header: {
    timestamp: "Time",
    wallet: "Wallet",
    protocol: "Protocol",
    token0: "Token In",
    token1: "Token Out",
    volume_usd: "Volume (USD)"
  }
})
```

## Volume by Protocol

```js
Plot.plot({
  marks: [
    Plot.barY(volume.by_protocol.filter(p => p.volume_usd > 0), {
      x: "protocol",
      y: "volume_usd",
      fill: "#22c55e",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  x: { label: null },
  y: { label: "Volume (USD)", transform: d => d / 1000000, tickFormat: d => `$${d}M` },
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
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--theme-foreground-muted);
  margin: 0 0 0.5rem;
}
.big {
  font-size: 2rem;
  font-weight: 600;
}
</style>
