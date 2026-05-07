---
title: Protocol Analytics
toc: false
---

# Protocol & DEX Analytics

Deep dive into protocol activity, venues, top traders, and token flows.

<div style="margin-bottom: 1.5rem;">
  <a href="/" style="padding: 0.5rem 1rem; border: 1px solid var(--theme-foreground-muted); color: var(--theme-foreground); border-radius: 6px; text-decoration: none;">← Overview</a>
  <a href="/cross-chain" style="padding: 0.5rem 1rem; background: #6366f1; color: white; border-radius: 6px; text-decoration: none; font-weight: 500; margin-left: 0.5rem;">Cross-Chain →</a>
</div>

```js
const proto = FileAttachment("data/protocols.json").json();
```

<div class="grid grid-cols-4" style="gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Protocol+Chain Combos</h2>
    <span class="big">${proto.total_protocols}</span>
  </div>
  <div class="card">
    <h2>Venues Tracked</h2>
    <span class="big">${proto.total_venues}</span>
  </div>
  <div class="card">
    <h2>Via Aggregators</h2>
    <span class="big">${proto.aggregator_stats.agg_swaps.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>Direct DEX</h2>
    <span class="big">${proto.aggregator_stats.direct_swaps.toLocaleString()}</span>
  </div>
</div>

## Top Protocols by Swap Volume

```js
Plot.plot({
  marks: [
    Plot.barY(proto.top_protocols, {
      x: d => `${d.protocol}/${d.chain}`,
      y: "swaps",
      fill: "protocol",
      tip: true,
      sort: {x: "y", reverse: true}
    }),
    Plot.ruleY([0])
  ],
  x: { label: null, tickRotate: -30 },
  y: { label: "Swaps", grid: true },
  color: { legend: false },
  marginBottom: 80,
  height: 350
})
```

<div class="grid grid-cols-2" style="gap: 1.5rem; margin: 2rem 0;">

## Top Venues by Activity

Most active pool/contract addresses.

```js
Inputs.table(proto.top_venues.slice(0, 15), {
  columns: ["protocol", "venue", "chain", "events", "wallets"],
  header: {
    protocol: "Protocol",
    venue: "Venue",
    chain: "Chain",
    events: "Events",
    wallets: "Wallets"
  },
  sort: "events",
  reverse: true
})
```

## Top Tokens by Swap Count

```js
Inputs.table(proto.top_tokens.slice(0, 15), {
  columns: ["symbol", "chain", "swaps", "traders", "volume_usd"],
  format: {
    volume_usd: d => d > 0 ? `$${d.toLocaleString(undefined, {maximumFractionDigits: 0})}` : "-"
  },
  header: {
    symbol: "Token",
    chain: "Chain",
    swaps: "Swaps",
    traders: "Traders",
    volume_usd: "Volume (USD)"
  },
  sort: "swaps",
  reverse: true
})
```

</div>

## Top Traders (Whale Detection)

Most active swappers by event count and protocol diversity.

```js
Inputs.table(proto.top_traders, {
  columns: ["wallet", "swaps", "protocols", "chains", "total_volume_usd"],
  format: {
    total_volume_usd: d => d > 0 ? `$${d.toLocaleString(undefined, {maximumFractionDigits: 0})}` : "-"
  },
  header: {
    wallet: "Wallet",
    swaps: "Swaps",
    protocols: "Protocols Used",
    chains: "Chains",
    total_volume_usd: "Volume (USD)"
  },
  sort: "swaps",
  reverse: true
})
```

## Protocol Distribution by Chain

```js
Plot.plot({
  marks: [
    Plot.barX(proto.top_protocols, {
      y: d => `${d.protocol}/${d.chain}`,
      x: "traders",
      fill: "chain",
      tip: true,
      sort: {y: "x", reverse: true}
    }),
    Plot.ruleX([0])
  ],
  x: { label: "Unique Traders", grid: true },
  y: { label: null },
  color: { legend: true },
  marginLeft: 160,
  height: 350
})
```

## Aggregator vs Direct Routing

```js
Plot.plot({
  marks: [
    Plot.barY([
      {type: "Aggregator", count: proto.aggregator_stats.agg_swaps, fill: "#f59e0b"},
      {type: "Direct DEX", count: proto.aggregator_stats.direct_swaps, fill: "#6366f1"}
    ], {
      x: "type",
      y: "count",
      fill: "fill",
      tip: true
    }),
    Plot.ruleY([0])
  ],
  color: { type: "identity" },
  x: { label: null },
  y: { label: "Swaps", grid: true },
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
</style>
