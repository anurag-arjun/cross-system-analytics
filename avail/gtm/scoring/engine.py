"""Prospect scoring algorithm for GTM/ICP."""

from typing import List, Dict
from dataclasses import dataclass


@dataclass
class ProspectScore:
    wallet_address: str
    score: float  # 0.0 - 100.0
    signals: List[str]
    top_chains: List[str]
    top_protocols: List[str]
    estimated_volume_usd: float


class ScoringEngine:
    def score_wallet(self, wallet_address: str) -> ProspectScore:
        # TODO: Query canonical_events for wallet activity
        # TODO: Apply ICP weights (token holdings, bridge patterns, chain activity)
        return ProspectScore(
            wallet_address=wallet_address,
            score=0.0,
            signals=[],
            top_chains=[],
            top_protocols=[],
            estimated_volume_usd=0.0,
        )

    def generate_segment(self, filters: Dict) -> List[ProspectScore]:
        # TODO: Query ClickHouse for wallets matching filters
        # TODO: Score each wallet, return sorted by score desc
        return []
