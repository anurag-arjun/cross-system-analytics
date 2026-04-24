#!/usr/bin/env bash
# Weekly architectural guardrails — run every Friday.
# If any test fails, fixing it is Monday's top priority.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON="python3"
if [ -f "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python"
fi

echo "=== Weekly Architectural Guardrails ==="
echo ""

# Test 1: /core standalone test
echo "[1/3] /core standalone test..."
TMP_DIR=$(mktemp -d)
cp -r "$SCRIPT_DIR/core" "$TMP_DIR/"
cd "$TMP_DIR/core"
if $PYTHON -m pytest tests/standalone/ -q; then
    echo "  PASS: /core runs standalone"
else
    echo "  FAIL: /core has Avail-specific leakage"
    exit 1
fi
cd - > /dev/null
rm -rf "$TMP_DIR"

# Test 2: Fake-integrator test
echo "[2/3] Fake-integrator test..."
if python -m pytest core/tests/fake_integrator/ -q; then
    echo "  PASS: Web2 events ingest and trajectory works"
else
    echo "  FAIL: Identity model is too Web3-specific"
    exit 1
fi

# Test 3: Schema extensibility test
echo "[3/3] Schema extensibility test..."
if python -m pytest core/tests/extensibility/ -q; then
    echo "  PASS: Schema registry is extensible"
else
    echo "  FAIL: Schema registry requires adapter-external changes"
    exit 1
fi

echo ""
echo "=== All guardrails passed ==="
