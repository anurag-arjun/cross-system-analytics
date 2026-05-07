---
name: session-end
description: Save a session handoff document capturing current goal, progress, blockers, and decisions. Creates a durable record for resuming work in a future session. Run before ending work, switching contexts, or when asked to wrap up.
---

# Session End

Save a handoff document so you can resume this work in a future session.

## When to Use

- Before ending a pi session
- Before switching to a different task or project
- When asked to "wrap up" or "save state"
- Before context compaction (if you know it's coming)
- Any natural stopping point

## How It Works

1. Interviews you (or infers from conversation) about current state
2. Writes a handoff file to `.pi/sessions/handoffs/{sessionName}/`
3. Updates a lightweight ledger with the latest state
4. Optionally extracts learnings/decisions for long-term memory

## Usage

```bash
/skill:session-end [session-name]
```

Without `session-name`, uses the current session name (inferred from context).  
With `session-name`, saves to that specific session's handoff directory.

## Handoff Content

The handoff captures:

| Section | Description |
|---|---|
| **Goal** | What we were trying to achieve this session |
| **Now** | Current state — what's done, what's in progress |
| **Blockers** | What's blocking progress (empty if none) |
| **Decisions** | Key architectural or design decisions made |
| **Files Changed** | List of files created or modified |
| **Next** | Recommended next steps for the next session |
| **Notes** | Any other context worth preserving |

## Output Format

Saved to `.pi/sessions/handoffs/{sessionName}/handoff-{timestamp}.md`:

```markdown
# Handoff: 2026-04-29T14-30-00

## Ledger

### Goal
Build persistent pending bridge table for cross-chain bridge linking.

### Now
- Postgres schema created and migration applied
- Dagster asset updated to write pending rows on bridge_out decode
- Next: test with real Across Base→Ethereum data

### Blockers
- None

### Decisions
- Chose Postgres over ClickHouse ReplacingMergeTree for operational state
- Retry cadence: 2min (Across), 15min (canonical deposit), 6hr (canonical L2→L1)

### Next
1. Run pipeline against Base mainnet for 1 hour
2. Validate pending rows appear in Postgres
3. Test bridge_in matching

## Files Changed
- `core/schemas/pending_bridge_outs.sql` (new)
- `ops/dagster/nexus_pipeline/assets.py` (modified)
- `core/identity/bridge_links.py` (modified)

## Session Info
- Duration: 2h 15m
- Tickets closed: na-5hcn
- Tickets opened: na-5p48
```

## Ledger Section Rules

The **Ledger** section is what `session-start` reads. Keep it concise:

- **Goal**: One sentence. The north star.
- **Now**: 2-4 bullet points. Current reality.
- **Blockers**: Be honest. Empty is fine.
- **Decisions**: Only non-obvious ones. Include rationale.
- **Next**: 1-3 concrete steps. The next person should know where to start.

## Auto-Handoff (Optional)

If you want handoffs created automatically before context loss, add to your pi config:

```json
{
  "preCompactHook": ".pi/skills/session-end/scripts/auto-handoff.sh"
}
```

This creates a minimal `auto-handoff-{timestamp}.md` with just Goal and Now.

## Project Setup

One-time setup per project:

```bash
mkdir -p .pi/sessions/handoffs
```

## Compatibility

Handoffs are compatible with Claude Code's continuity system:
- Same `.md` format with `## Ledger` section
- Same `thoughts/shared/handoffs/` path structure (if you symlink)
- Can be read by both Claude and pi interchangeably

## See Also

- `session-start` — Load a handoff to resume work
