"""Tests for identity graph resolution."""

import pytest

from core.identity.graph import IdentityGraph, ResolvedEntity
from core.identity.resolvers import ResolutionResult, StaticResolver


class TestIdentityGraph:
    def test_single_hop_resolution(self):
        graph = IdentityGraph()
        graph.add_relationship(
            from_entity="0xabc",
            from_type="wallet",
            to_entity="vitalik.eth",
            to_type="ens",
            relationship_type="resolved_to",
            confidence=0.95,
            source="ens",
        )

        results = graph.resolve("0xabc", entity_type="wallet")
        assert len(results) == 1
        assert results[0].entity_id == "vitalik.eth"
        assert results[0].entity_type == "ens"
        assert results[0].confidence == 0.95

    def test_multi_hop_confidence_propagation(self):
        graph = IdentityGraph()
        # wallet -> ens -> twitter
        graph.add_relationship("0xabc", "wallet", "vitalik.eth", "ens", "resolved_to", 0.9, "ens")
        graph.add_relationship(
            "vitalik.eth", "ens", "vitalik", "twitter", "linked_via_ens", 0.8, "ensdata"
        )

        results = graph.resolve("0xabc", entity_type="wallet")
        assert len(results) == 2

        ens_results = [r for r in results if r.entity_type == "ens"]
        twitter_results = [r for r in results if r.entity_type == "twitter"]

        assert len(ens_results) == 1
        assert ens_results[0].confidence == 0.9

        assert len(twitter_results) == 1
        assert twitter_results[0].confidence == 0.72  # 0.9 * 0.8

    def test_min_confidence_filter(self):
        graph = IdentityGraph()
        graph.add_relationship("0xabc", "wallet", "ens1", "ens", "resolved_to", 0.9, "ens")
        graph.add_relationship("ens1", "ens", "twitter1", "twitter", "linked", 0.4, "guess")

        # With min_confidence=0.5, twitter1 (0.9*0.4=0.36) should be excluded
        results = graph.resolve("0xabc", entity_type="wallet", min_confidence=0.5)
        assert len(results) == 1
        assert results[0].entity_type == "ens"

    def test_target_type_filter(self):
        graph = IdentityGraph()
        graph.add_relationship("0xabc", "wallet", "ens1", "ens", "resolved_to", 0.9, "ens")
        graph.add_relationship("0xabc", "wallet", "0xdef", "wallet", "linked", 0.8, "cluster")

        results = graph.resolve("0xabc", entity_type="wallet", target_type="wallet")
        assert len(results) == 1
        assert results[0].entity_id == "0xdef"

    def test_max_depth_limit(self):
        graph = IdentityGraph()
        # Chain of 5 hops
        graph.add_relationship("a", "wallet", "b", "ens", "r", 1.0, "s")
        graph.add_relationship("b", "ens", "c", "twitter", "r", 1.0, "s")
        graph.add_relationship("c", "twitter", "d", "email", "r", 1.0, "s")
        graph.add_relationship("d", "email", "e", "device", "r", 1.0, "s")

        # max_depth=2: should reach c (2 hops from a)
        results = graph.resolve("a", entity_type="wallet", max_depth=2)
        assert len(results) == 2  # b and c
        entity_ids = {r.entity_id for r in results}
        assert entity_ids == {"b", "c"}

    def test_walk_specific_path(self):
        graph = IdentityGraph()
        graph.add_relationship("0xabc", "wallet", "vitalik.eth", "ens", "resolved_to", 0.95, "ens")
        graph.add_relationship("vitalik.eth", "ens", "vitalik", "twitter", "linked", 0.8, "ensdata")
        graph.add_relationship("0xabc", "wallet", "0xdef", "wallet", "linked", 0.9, "cluster")

        # Walk wallet -> ens -> twitter specifically
        results = graph.walk("0xabc", "wallet", ["wallet", "ens", "twitter"])
        assert len(results) == 1
        assert results[0].entity_id == "vitalik"
        assert results[0].entity_type == "twitter"
        assert results[0].confidence == 0.76  # 0.95 * 0.8

    def test_walk_no_match(self):
        graph = IdentityGraph()
        graph.add_relationship("0xabc", "wallet", "vitalik.eth", "ens", "resolved_to", 0.95, "ens")

        # No twitter link from ens
        results = graph.walk("0xabc", "wallet", ["wallet", "ens", "twitter"])
        assert len(results) == 0

    def test_isolated_entity_returns_empty(self):
        graph = IdentityGraph()
        results = graph.resolve("0xunknown", entity_type="wallet")
        assert results == []


class TestStaticResolver:
    def test_static_resolution(self):
        resolver = StaticResolver(
            {
                "wallet:0xabc": ResolutionResult(
                    to_entity="vitalik.eth",
                    to_type="ens",
                    relationship_type="resolved_to",
                    confidence=0.95,
                    source="test",
                )
            }
        )

        result = resolver.resolve("0xabc", "wallet")
        assert result is not None
        assert result.to_entity == "vitalik.eth"

    def test_missing_mapping_returns_none(self):
        resolver = StaticResolver({})
        assert resolver.resolve("0xabc", "wallet") is None
