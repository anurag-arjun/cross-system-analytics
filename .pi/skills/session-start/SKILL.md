---
name: session-start
description: Resume a coding session by loading the most recent handoff document. Injects prior context — goal, current state, blockers, and decisions — so you can continue where you left off. Run at the beginning of a new session.
---

# Session Start

Resume work by loading context from the most recent session handoff.

## When to Use

- Starting a new pi session after a break
- Switching back to a project after working on something else
- After context loss (compaction, new conversation)
- Any time you need to recall what was in progress

## How It Works

1. Scans `.pi/sessions/handoffs/` for the most recent handoff
2. Reads the handoff's **Ledger** section (Goal, Now, Blockers, Decisions)
3. Injects that context into the current session
4. Suggests the next logical action based on "Now" and "Next"

## Usage

```bash
/skill:session-start [session-name]
```

Without `session-name`, loads the most recent handoff across all sessions.  
With `session-name`, loads the most recent handoff for that specific session.

## Handoff Format

Handoffs are markdown files in `.pi/sessions/handoffs/{sessionName}/`:

```markdown
# Handoff: 2026-04-29T14-30-00

## Ledger

### Goal
Build persistent pending bridge table for cross-chain bridge linking.

### Now
- Postgres schema designed, migration pending
- Need to wire Dagster asset to write pending rows

### Blockers
- None

### Decisions
- Using Postgres (not ClickHouse) for operational state
- Retry cadence: 2min for Across, 6hr for canonical L2→L1

### Next
1. Run migration for pending_bridge_outs table
2. Update Dagster decoded_events asset
```

## What Gets Loaded

| Section | Injected As |
|---|---|
| **Goal** | "We are working toward: {goal}" |
| **Now** | "Current state: {now}" |
| **Blockers** | "Blockers: {blockers}" (if any) |
| **Decisions** | "Key decisions: {decisions}" (if any) |
| **Next** | "Suggested next steps: {next}" |

## If No Handoff Exists

If no handoff is found, the skill will:
1. Check for `.claude/` continuity files (Claude Code compatibility)
2. Check for `CONTEXT.md` in the project root
3. Fall back to asking: "No session handoff found. What are you working on?"

## Project Setup

One-time setup per project:

```bash
mkdir -p .pi/sessions/handoffs
```

Add to `.gitignore`:
```
# Session handoffs may contain transient state
.pi/sessions/handoffs/*/auto-handoff-*.md
```

## See Also

- `session-end` — Save a handoff when wrapping up work
