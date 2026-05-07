#!/bin/bash
# Load the most recent handoff for a session
# Usage: load-handoff.sh [session-name]

set -e

PROJECT_DIR="${PI_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
SESSION_NAME="${1:-}"
HANDOFFS_DIR="$PROJECT_DIR/.pi/sessions/handoffs"

if [ ! -d "$HANDOFFS_DIR" ]; then
    echo "No handoffs directory found at $HANDOFFS_DIR"
    echo "Run /skill:session-end first to create a handoff."
    exit 1
fi

# If no session name provided, find the most recent handoff across all sessions
if [ -z "$SESSION_NAME" ]; then
    LATEST_HANDOFF=$(find "$HANDOFFS_DIR" -name "handoff-*.md" -o -name "auto-handoff-*.md" 2>/dev/null | sort -r | head -1)
    if [ -z "$LATEST_HANDOFF" ]; then
        echo "No handoff files found in $HANDOFFS_DIR"
        exit 1
    fi
    SESSION_NAME=$(basename "$(dirname "$LATEST_HANDOFF")")
else
    SESSION_DIR="$HANDOFFS_DIR/$SESSION_NAME"
    if [ ! -d "$SESSION_DIR" ]; then
        echo "No handoffs found for session: $SESSION_NAME"
        echo "Available sessions:"
        ls -1 "$HANDOFFS_DIR" 2>/dev/null || echo "(none)"
        exit 1
    fi
    LATEST_HANDOFF=$(find "$SESSION_DIR" -name "handoff-*.md" -o -name "auto-handoff-*.md" 2>/dev/null | sort -r | head -1)
fi

if [ -z "$LATEST_HANDOFF" ] || [ ! -f "$LATEST_HANDOFF" ]; then
    echo "No handoff file found for session: $SESSION_NAME"
    exit 1
fi

echo "=== Loading handoff: $(basename "$LATEST_HANDOFF") ==="
echo "Session: $SESSION_NAME"
echo ""

# Extract Ledger section
cat "$LATEST_HANDOFF"
