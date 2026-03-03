"""Tests for EntropyNetwork model construction."""

import pytest

from dataraum.entropy.network.config import (
    EdgeConfig,
    NetworkConfig,
    NodeConfig,
)
from dataraum.entropy.network.model import EntropyNetwork


class TestModelConstruction:
    """Test building a valid network."""

    def test_builds_from_small_config(self, small_network: EntropyNetwork):
        assert small_network is not None
        assert small_network.model.check_model()

    def test_builds_from_full_config(self, full_network: EntropyNetwork):
        assert full_network is not None
        assert full_network.model.check_model()


class TestNodeQueries:
    """Test node relationship queries."""

    def test_get_parents_for_root(self, full_network: EntropyNetwork):
        parents = full_network.get_parents("type_fidelity")
        assert parents == []

    def test_get_parents_for_child(self, full_network: EntropyNetwork):
        parents = full_network.get_parents("join_path_determinism")
        assert parents == ["type_fidelity"]

    def test_get_parents_for_multi_parent(self, full_network: EntropyNetwork):
        parents = full_network.get_parents("aggregation_safety")
        assert set(parents) == {"unit_declaration", "null_ratio", "outlier_rate"}

    def test_get_children(self, full_network: EntropyNetwork):
        children = full_network.get_children("type_fidelity")
        expected = {
            "join_path_determinism",
            "relationship_quality",
            "query_intent",
            "reporting_intent",
        }
        assert set(children) == expected

    def test_get_children_leaf_node(self, full_network: EntropyNetwork):
        children = full_network.get_children("query_intent")
        assert children == []

    def test_get_root_nodes(self, full_network: EntropyNetwork):
        roots = full_network.get_root_nodes()
        expected = {
            "type_fidelity",
            "null_ratio",
            "outlier_rate",
            "naming_clarity",
            "unit_declaration",
            "time_role",
            "temporal_drift",
            "benford_compliance",
        }
        assert set(roots) == expected

    def test_get_leaf_nodes(self, full_network: EntropyNetwork):
        leaves = full_network.get_leaf_nodes()
        expected = {"query_intent", "aggregation_intent", "reporting_intent"}
        assert set(leaves) == expected

    def test_get_intent_nodes(self, full_network: EntropyNetwork):
        intents = full_network.get_intent_nodes()
        assert set(intents) == {"query_intent", "aggregation_intent", "reporting_intent"}

    def test_get_node_config(self, full_network: EntropyNetwork):
        config = full_network.get_node_config("type_fidelity")
        assert config.layer == "structural"
        assert config.dimension == "types"
        assert config.sub_dimension == "type_fidelity"

    def test_get_node_config_unknown_raises(self, full_network: EntropyNetwork):
        with pytest.raises(KeyError, match="Unknown node"):
            full_network.get_node_config("nonexistent")


class TestInvalidConfig:
    """Test error handling for invalid configurations."""

    def test_undefined_parent_raises(self):
        config = NetworkConfig(
            nodes={
                "a": NodeConfig(
                    name="a", layer="x", dimension="y", sub_dimension="z", prior=[0.5, 0.3, 0.2]
                ),
            },
            edges=[
                EdgeConfig(parent="nonexistent", child="a", strength=0.5),
            ],
        )
        with pytest.raises(ValueError, match="undefined parent node"):
            EntropyNetwork(config=config)

    def test_undefined_child_raises(self):
        config = NetworkConfig(
            nodes={
                "a": NodeConfig(
                    name="a", layer="x", dimension="y", sub_dimension="z", prior=[0.5, 0.3, 0.2]
                ),
            },
            edges=[
                EdgeConfig(parent="a", child="nonexistent", strength=0.5),
            ],
        )
        with pytest.raises(ValueError, match="undefined child node"):
            EntropyNetwork(config=config)


class TestSmallNetwork:
    """Test with the 4-node small network."""

    def test_root_nodes(self, small_network: EntropyNetwork):
        roots = small_network.get_root_nodes()
        assert set(roots) == {"root_a", "root_b"}

    def test_leaf_nodes(self, small_network: EntropyNetwork):
        leaves = small_network.get_leaf_nodes()
        assert set(leaves) == {"leaf_z"}

    def test_child_has_two_parents(self, small_network: EntropyNetwork):
        parents = small_network.get_parents("child_x")
        assert set(parents) == {"root_a", "root_b"}
