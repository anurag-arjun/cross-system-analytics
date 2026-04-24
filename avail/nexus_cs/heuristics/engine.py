"""Heuristic engine for Nexus integrator CS signals."""

from typing import List, Dict
from dataclasses import dataclass


@dataclass
class Signal:
    integrator_id: str
    heuristic_name: str
    severity: str  # 'critical', 'warning', 'info'
    detected_at: str
    recommendation: str
    suggested_action: str


class HeuristicEngine:
    HEURISTICS = [
        "high_bridge_only_ratio",
        "outdated_sdk",
        "high_error_rate",
        "stalled_onboarding",
        "churn_imminent",
        "ready_for_upgrade",
        "multi_chain_opportunity",
    ]

    def evaluate(self, integrator_id: str) -> List[Signal]:
        # TODO: Query canonical_events + bridge_links for integrator metrics
        # TODO: Apply heuristics, return signals sorted by severity
        return []
