#!/bin/bash
# Save a session handoff
# Usage: save-handoff.sh <session-name> <goal> <now> <blockers> <decisions> <next> <files-changed>

set -e

PROJECT_DIR="${PI_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
SESSION_NAME="${1:-nexus-analytics}"
GOAL="${2:-}"
NOW="${3:-}"
BLOCKERS="${4:-}"
DECISIONS="${5:-}"
NEXT_STEPS="${6:-}"
FILES_CHANGED="${7:-}"

HANDOFFS_DIR="$PROJECT_DIR/.pi/sessions/handoffs/$SESSION_NAME"
mkdir -p "$HANDOFFS_DIR"

TIMESTAMP=$(date -u +"%Y-%m-%dT%H-%M-%S")
HANDOFF_FILE="$HANDOFFS_DIR/handoff-$TIMESTAMP.md"

cat > "$HANDOFF_FILE" << 'EOF'
# Handoff: TIMESTAMP_PLACEHOLDER

## Ledger

### Goal
GOAL_PLACEHOLDER

### Now
NOW_PLACEHOLDER

### Blockers
BLOCKERS_PLACEHOLDER

### Decisions
DECISIONS_PLACEHOLDER

### Next
NEXT_PLACEHOLDER

## Files Changed
FILES_PLACEHOLDER

## Session Info
- Handoff created: TIMESTAMP_PLACEHOLDER
EOF

# Replace placeholders
sed -i "s/TIMESTAMP_PLACEHOLDER/$TIMESTAMP/g" "$HANDOFF_FILE"
sed -i "s/GOAL_PLACEHOLDER/${GOAL//\//\\/}/g" "$HANDOFF_FILE"
sed -i "s/NOW_PLACEHOLDER/${NOW//\//\\/}/g" "$HANDOFF_FILE"
sed -i "s/BLOCKERS_PLACEHOLDER/${BLOCKERS//\//\\/}/g" "$HANDOFF_FILE"
sed -i "s/DECISIONS_PLACEHOLDER/${DECISIONS//\//\\/}/g" "$HANDOFF_FILE"
sed -i "s/NEXT_PLACEHOLDER/${NEXT_STEPS//\//\\/}/g" "$HANDOFF_FILE"
sed -i "s/FILES_PLACEHOLDER/${FILES_CHANGED//\//\\/}/g" "$HANDOFF_FILE"

echo "Handoff saved: $HANDOFF_FILE"
