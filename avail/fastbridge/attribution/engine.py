"""Campaign attribution logic for FastBridge."""

from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class AttributionResult:
    campaign_id: str
    campaign_name: str
    channel: str
    sessions: int
    wallet_connects: int
    bridges: int
    swaps: int
    cac_per_swap: Optional[float]
    total_onchain_value_usd: float


class AttributionEngine:
    def attribute_campaign(
        self, campaign_id: str, start: datetime, end: datetime
    ) -> AttributionResult:
        # TODO: Join GA4 sessions → identity graph → canonical_events
        # TODO: Count wallet_connects, bridges, swaps per campaign
        # TODO: Calculate CAC and onchain value
        return AttributionResult(
            campaign_id=campaign_id,
            campaign_name="",
            channel="",
            sessions=0,
            wallet_connects=0,
            bridges=0,
            swaps=0,
            cac_per_swap=None,
            total_onchain_value_usd=0.0,
        )

    def compare_channels(self, start: datetime, end: datetime) -> List[AttributionResult]:
        # TODO: Group by channel, return sorted by onchain value
        return []
