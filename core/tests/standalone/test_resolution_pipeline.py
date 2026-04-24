import pytest
from datetime import datetime, timezone

from core.identity.graph import IdentityGraph
from core.identity.pipeline import ResolutionPipeline
from core.identity.resolvers import ResolutionResult, StaticResolver


class TestResolutionPipeline:
    def test_resolve_single_wallet(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                )
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver])
        results = pipeline.resolve_wallets(["0xabc"])

        assert len(results) == 1
        assert results[0]["to_entity"] == "vitalik.eth"
        assert results[0]["confidence_score"] == 0.95

    def test_resolve_multiple_wallets(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                ),
                "wallet:0xdef": ResolutionResult(
                    to_entity="satoshi.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.9,
                    source="ens",
                ),
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver])
        results = pipeline.resolve_wallets(["0xabc", "0xdef"])

        assert len(results) == 2
        entities = {r["to_entity"] for r in results}
        assert entities == {"vitalik.eth", "satoshi.eth"}

    def test_unresolved_wallet_skipped(self):
        resolver = StaticResolver({})
        pipeline = ResolutionPipeline(resolvers=[resolver])
        results = pipeline.resolve_wallets(["0xunknown"])

        assert len(results) == 0

    def test_graph_populated(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                )
            }
        )
        graph = IdentityGraph()
        pipeline = ResolutionPipeline(resolvers=[resolver], identity_graph=graph)
        pipeline.resolve_wallets(["0xabc"])

        resolved = graph.resolve("0xabc", entity_type="wallet")
        assert len(resolved) == 1
        assert resolved[0].entity_id == "vitalik.eth"
        assert resolved[0].confidence == 0.95

    def test_clickhouse_rows_format(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                )
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver])
        pipeline.resolve_wallets(["0xabc"])

        rows = pipeline.to_clickhouse_rows()
        assert len(rows) == 1
        assert rows[0][0] == "0xabc"
        assert rows[0][1] == "wallet"
        assert rows[0][2] == "vitalik.eth"
        assert rows[0][3] == "ens"
        assert rows[0][4] == "resolved_to"
        assert rows[0][5] == 0.95
        assert rows[0][6] == "ens"
        assert isinstance(rows[0][7], datetime)
        assert rows[0][8] is None

    def test_multiple_resolvers_tried(self):
        resolver1 = StaticResolver({})
        resolver2 = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                )
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver1, resolver2])
        results = pipeline.resolve_wallets(["0xabc"])

        assert len(results) == 1
        assert results[0]["to_entity"] == "vitalik.eth"

    def test_first_successful_resolver_wins(self):
        resolver1 = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                )
            }
        )
        resolver2 = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="other.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.8,
                    source="ens_backup",
                )
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver1, resolver2])
        results = pipeline.resolve_wallets(["0xabc"])

        assert len(results) == 1
        assert results[0]["to_entity"] == "vitalik.eth"

    def test_batch_with_mixed_results(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                )
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver])
        results = pipeline.resolve_wallets(["0xabc", "0xunknown"])

        assert len(results) == 1
        assert results[0]["from_entity"] == "0xabc"

    def test_detected_at_timestamp(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                )
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver])
        before = datetime.now(timezone.utc)
        results = pipeline.resolve_wallets(["0xabc"])
        after = datetime.now(timezone.utc)

        assert before <= results[0]["detected_at"] <= after

    def test_extra_preserved(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="ens",
                    extra={"avatar": "https://example.com/avatar.png"},
                )
            }
        )
        pipeline = ResolutionPipeline(resolvers=[resolver])
        results = pipeline.resolve_wallets(["0xabc"])

        assert results[0]["extra"]["avatar"] == "https://example.com/avatar.png"
