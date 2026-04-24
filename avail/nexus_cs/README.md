# Avail Nexus CS Tool

Customer Success tooling for Avail Nexus integrators.

## Components

- `integrators/` — Integrator registry + CRUD UI
- `heuristics/` — Rules engine for CS signals
- `signals_inbox/` — UI for CS team to view/act on signals
- `workflows/` — Notes, outreach tracking, tasks

## Key Concepts

**Integrator**: A dApp/wallet/chain that has integrated with Nexus.
**Signal**: A heuristic detection (churn risk, upgrade opportunity, etc.).
**Trajectory**: Cross-chain user journey after bridging through an integrator.

## Usage

```python
from avail.nexus_cs.heuristics import HeuristicEngine

engine = HeuristicEngine()
signals = engine.evaluate(integrator_id="0xabc...")
```
