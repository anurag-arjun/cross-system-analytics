---
title: Cross-Chain Analytics
toc: false
---

# Cross-Chain Behavior

Wallets spanning multiple chains, bridge events, and cross-chain journeys.

<div style="margin-bottom: 1.5rem;">
  <a href="/" style="padding: 0.5rem 1rem; border: 1px solid var(--theme-foreground-muted); color: var(--theme-foreground); border-radius: 6px; text-decoration: none;">← Overview</a>
  <a href="/protocols" style="padding: 0.5rem 1rem; background: #f59e0b; color: white; border-radius: 6px; text-decoration: none; font-weight: 500; margin-left: 0.5rem;">Protocol Analytics →</a>
</div>

```js
const xc = FileAttachment("data/cross-chain.json").json();
const overview = FileAttachment("data/overview.json").json();
```

<div class="grid grid-cols-4" style="gap: 1rem; margin-bottom: 2rem;">
  <div class="card">
    <h2>Cross-Chain Wallets</h2>
    <span class="big">${xc.total_cross_chain.toLocaleString()}</span>
  </div>
  <div class="card">
    <h2>% of All Wallets</h2>
    <span class="big">${overview.kpis.cross_chain_pct}%</span>
  </div>
  <div class="card">
    <h2>Bridge Events</h2>
    <span class="big">${xc.bridges.length}</span>
  </div>
  <div class="card" style="border-left: 4px solid #22c55e;">
    <h2>Matched Links</h2>
    <span class="big" style="color: #22c55e;">${xc.bridge_links.length}</span>
  </div>
</div>

## Chain Pair Activity

Which chain combinations do cross-chain wallets use?

```js
Plot.plot({
  marks: [
    Plot.barY(xc.chain_matrix, {
      x: d => d.chains.join(" + "),
      y: "wallets",
      fill: d => d.chains.length > 1 ? "#6366f1" : "#374151",
      tip: true,
      sort: {x: "y", reverse: true}
    }),
    Plot.ruleY([0])
  ],
  x: { label: null, tickRotate: -20 },
  y: { label: "Wallets", grid: true },
  marginBottom: 40,
  height: 250
})
```

<div class="grid grid-cols-2" style="gap: 1.5rem; margin: 2rem 0;">

## Top Cross-Chain Wallets

Most active wallets spanning multiple chains.

```js
Inputs.table(xc.top_cross_chain.slice(0, 15), {
  columns: ["wallet", "chains", "events", "first_seen", "last_seen"],
  header: {
    wallet: "Wallet",
    chains: "Chains",
    events: "Total Events",
    first_seen: "First Seen",
    last_seen: "Last Seen"
  },
  sort: "events",
  reverse: true
})
```

## Bridge Events

All bridge_in and bridge_out events detected.

```js
Inputs.table(xc.bridges, {
  columns: ["event_type", "protocol", "chain", "token_in", "token_out", "amount_in_usd", "amount_out_usd"],
  format: {
    token_in: d => d ? d.slice(0, 10) + "..." : "-",
    token_out: d => d ? d.slice(0, 10) + "..." : "-",
    amount_in_usd: d => d ? `$${d.toLocaleString()}` : "-",
    amount_out_usd: d => d ? `$${d.toLocaleString()}` : "-"
  },
  header: {
    event_type: "Type",
    protocol: "Protocol",
    chain: "Chain",
    token_in: "Token In",
    token_out: "Token Out",
    amount_in_usd: "Amount In (USD)",
    amount_out_usd: "Amount Out (USD)"
  }
})
```

</div>

## Bridge Links (Matched Pairs)

Cross-chain bridge_out → bridge_in matches with confidence scores.

```js
Inputs.table(xc.bridge_links, {
  columns: ["link_key_type", "src_chain", "dst_chain", "src_wallet", "dst_wallet", "amount_usd", "confidence"],
  format: {
    amount_usd: d => d ? `$${d.toLocaleString()}` : "-",
    confidence: d => `${(d * 100).toFixed(0)}%`
  },
  header: {
    link_key_type: "Bridge Type",
    src_chain: "From",
    dst_chain: "To",
    src_wallet: "Source Wallet",
    dst_wallet: "Dest Wallet",
    amount_usd: "Amount (USD)",
    confidence: "Confidence"
  },
  sort: "confidence",
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
