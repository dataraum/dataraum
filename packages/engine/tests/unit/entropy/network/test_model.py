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


class TestSubgraph:
    """Test dynamic subgraph construction."""

    def test_full_evidence_returns_self(self, full_network: EntropyNetwork):
        """When all root nodes are observed, no pruning needed."""
        all_roots = {
            "type_fidelity",
            "null_ratio",
            "outlier_rate",
            "naming_clarity",
            "unit_declaration",
            "time_role",
            "temporal_drift",
            "benford_compliance",
            "slice_stability",
            "dimension_coverage",
            "cross_table_consistency",
            "business_cycle_health",
        }
        # Also include observable children
        observed = all_roots | {"join_path_determinism", "relationship_quality", "formula_match"}
        sub = full_network.subgraph(observed)
        assert sub is full_network  # Same object, no pruning

    def test_partial_evidence_removes_unobserved_roots(self, full_network: EntropyNetwork):
        """Unobserved root nodes are removed from the subgraph."""
        observed = {"type_fidelity", "null_ratio"}
        sub = full_network.subgraph(observed)
        sub_nodes = set(sub.node_names)

        # Observed roots kept
        assert "type_fidelity" in sub_nodes
        assert "null_ratio" in sub_nodes

        # Unobserved roots removed
        assert "outlier_rate" not in sub_nodes
        assert "benford_compliance" not in sub_nodes
        assert "temporal_drift" not in sub_nodes
        assert "time_role" not in sub_nodes
        assert "unit_declaration" not in sub_nodes
        assert "dimension_coverage" not in sub_nodes

        # Intent leaves always kept
        assert "query_intent" in sub_nodes
        assert "aggregation_intent" in sub_nodes
        assert "reporting_intent" in sub_nodes

    def test_inferrable_children_kept(self, full_network: EntropyNetwork):
        """Non-root nodes with at least one observed parent are kept."""
        observed = {"type_fidelity", "null_ratio", "naming_clarity"}
        sub = full_network.subgraph(observed)
        sub_nodes = set(sub.node_names)

        # join_path_determinism has parent type_fidelity (observed) → kept
        assert "join_path_determinism" in sub_nodes
        # relationship_quality has parent type_fidelity (observed) → kept
        assert "relationship_quality" in sub_nodes
        # formula_match has parents naming_clarity, null_ratio (observed) + unit_declaration (removed)
        # Still has 2 observed parents → kept
        assert "formula_match" in sub_nodes

    def test_orphaned_children_removed(self, full_network: EntropyNetwork):
        """Non-root nodes whose ALL parents are removed get removed too."""
        # Only observe naming_clarity — type_fidelity is NOT observed
        observed = {"naming_clarity"}
        sub = full_network.subgraph(observed)
        sub_nodes = set(sub.node_names)

        # join_path_determinism has only parent type_fidelity (removed) → removed
        assert "join_path_determinism" not in sub_nodes
        # relationship_quality has only parent type_fidelity (removed) → removed
        assert "relationship_quality" not in sub_nodes

    def test_orphaned_intermediate_removed(self, full_network: EntropyNetwork):
        """A non-root node that loses all parents is removed from the subgraph."""
        observed = {"naming_clarity", "time_role"}
        sub = full_network.subgraph(observed)
        sub_nodes = set(sub.node_names)

        # aggregation_safety: parents are null_ratio, outlier_rate, unit_declaration — all removed
        # aggregation_safety becomes orphaned → removed (not observed, no kept parents)
        assert "aggregation_safety" not in sub_nodes

    def test_disconnected_intent_removed(self, full_network: EntropyNetwork):
        """Intent leaves with no remaining parents are removed (avoids false investigate)."""
        # Only observe naming_clarity — this connects to formula_match and reporting_intent
        # but NOT to aggregation_intent via its required path
        observed = {"type_fidelity"}
        sub = full_network.subgraph(observed)
        sub_nodes = set(sub.node_names)

        # query_intent has type_fidelity as parent → kept
        assert "query_intent" in sub_nodes
        # aggregation_intent's parents: aggregation_safety, formula_match, temporal_drift, benford
        # None of these are in the subgraph → aggregation_intent removed
        assert "aggregation_intent" not in sub_nodes

    def test_observed_node_losing_parents_gets_uniform_prior(self, full_network: EntropyNetwork):
        """An observed node that loses all parents becomes a root with uniform prior."""
        # join_path_determinism has parent type_fidelity
        # If type_fidelity is NOT observed but join_path_determinism IS observed,
        # join_path_determinism becomes a root with uniform prior
        observed = {"join_path_determinism"}
        sub = full_network.subgraph(observed)
        sub_nodes = set(sub.node_names)

        assert "join_path_determinism" in sub_nodes
        assert "type_fidelity" not in sub_nodes  # parent removed
        # join_path_determinism is now a root with uniform prior
        node_cfg = sub.get_node_config("join_path_determinism")
        assert node_cfg.prior is not None
        n = len(sub.states)
        assert node_cfg.prior == [1.0 / n] * n

    def test_subgraph_model_is_valid(self, full_network: EntropyNetwork):
        """Subgraph produces a valid pgmpy model."""
        observed = {"type_fidelity", "null_ratio", "naming_clarity"}
        sub = full_network.subgraph(observed)
        assert sub.model.check_model()

    def test_small_network_subgraph(self, small_network: EntropyNetwork):
        """Subgraph on the small 4-node network."""
        # Only observe root_a, not root_b
        sub = small_network.subgraph({"root_a"})
        sub_nodes = set(sub.node_names)

        assert "root_a" in sub_nodes
        assert "root_b" not in sub_nodes  # Unobserved root removed
        assert "child_x" in sub_nodes  # Has root_a as parent
        assert "leaf_z" in sub_nodes  # Intent leaf

        # child_x now has only 1 parent instead of 2
        assert sub.get_parents("child_x") == ["root_a"]


class TestSmallNetwork:
    """Test with the 4-node small network."""

    def test_child_has_two_parents(self, small_network: EntropyNetwork):
        parents = small_network.get_parents("child_x")
        assert set(parents) == {"root_a", "root_b"}
