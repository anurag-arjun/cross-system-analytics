# Decoder validation

Sanity rails for our decoders, comparing `canonical_events` row counts
against external ground-truth datasets.

## `dune_parity.py` — vs Dune `dex.trades`

Diffs `canonical_events.event_type='swap'` against Dune's `dex.trades` for
the same window and chain set. Surfaces:

- **gap** — Dune sees swaps for this protocol, we don't (decoder missing).
- **extra** — We see swaps Dune doesn't (likely false positive).
- **low** — Both sides have rows but our count is < 50% of Dune's (partial coverage).
- **match** — Ratio in [0.5, 1.5].

Spellbook applies its own filters (dex.trades excludes wrapped-token mints,
fee transfers, certain wash patterns) so 1.0 parity is unrealistic. The
diff is a *regression rail*, not a correctness proof: a (chain, protocol)
that was matching last week dropping into gap/low is the signal worth
investigating.

### Manual run

```bash
PYTHONPATH=. python ops/validation/dune_parity.py [--days 1] [--chains ethereum base ...]
```

Output goes to `ops/validation/runs/{YYYY-MM-DD}.json`. Re-running on the
same date overwrites. The previous date's file is the baseline for
regression warnings.

### Reading the output

```json
{
  "summary": {"match": 12, "low": 3, "gap": 7, "extra": 4},
  "rows": [
    {
      "chain": "base",
      "protocol": "aerodrome_v1",
      "our_count": 8421,
      "dune_count": 8390,
      "ratio": 1.004,
      "status": "match"
    },
    ...
  ]
}
```

Investigate when:

- A protocol moves from `match` → `gap`/`low`: a decoder regressed or the
  registry fell out of date.
- An `extra` row has high volume: we're misclassifying. Check the
  `protocol_contracts` rows for this chain+protocol combo.

### Cost

One Dune query per run (group-by aggregation on `dex.trades`). Tiny export
(~5 KB result set), so credit cost is dominated by execution. Expect ~5-10
credits/run on the free tier.

### Schedule

Wired as a weekly Dagster `ScheduleDefinition` (`weekly_dune_parity`)
running Mondays at 06:00 UTC. Re-runnable on demand from the Dagster UI.
