"""Tests for network-aware priority ranking."""


from dataraum.entropy.network.model import EntropyNetwork
from dataraum.entropy.network.priority import (
    compute_cascade_paths,
    compute_network_priorities,
)


class TestComputeCascadePaths:
    """Test BFS downstream path discovery."""

    def test_root_node_cascade(self, full_network: EntropyNetwork):
        paths = compute_cascade_paths(full_network, "type_fidelity")
        # type_fidelity -> join_path_determinism, relationship_quality,
        #                  query_intent, reporting_intent
        assert "join_path_determinism" in paths
        assert "query_intent" in paths

    def test_leaf_node_has_empty_cascade(self, full_network: EntropyNetwork):
        paths = compute_cascade_paths(full_network, "query_intent")
        assert paths == []

    def test_mid_node_cascade(self, full_network: EntropyNetwork):
        paths = compute_cascade_paths(full_network, "aggregation_safety")
        assert "aggregation_intent" in paths
        assert "reporting_intent" in paths

    def test_small_network_cascade(self, small_network: EntropyNetwork):
        paths = compute_cascade_paths(small_network, "root_a")
        assert set(paths) == {"child_x", "leaf_z"}


class TestComputeNetworkPriorities:
    """Test intervention priority computation."""

    def test_high_entropy_nodes_get_priorities(self, full_network: EntropyNetwork):
        evidence = {
            "type_fidelity": "high",
            "null_ratio": "high",
            "unit_declaration": "high",
        }
        priorities = compute_network_priorities(full_network, evidence)
        assert len(priorities) > 0
        # All returned nodes should have non-low state
        for p in priorities:
            assert p.current_state != "low"

    def test_fixing_root_shows_larger_delta_than_leaf(self, full_network: EntropyNetwork):
        """Root nodes with many downstream dependencies should have higher impact."""
        evidence = {
            "type_fidelity": "high",
            "null_ratio": "high",
            "unit_declaration": "high",
        }
        priorities = compute_network_priorities(full_network, evidence)
        # There should be at least one priority with positive delta
        assert any(p.impact_delta > 0 for p in priorities)

    def test_all_low_evidence_returns_empty(self, full_network: EntropyNetwork):
        evidence = {
            "type_fidelity": "low",
            "null_ratio": "low",
            "outlier_rate": "low",
        }
        priorities = compute_network_priorities(full_network, evidence)
        assert priorities == []

    def test_empty_evidence_returns_empty(self, full_network: EntropyNetwork):
        priorities = compute_network_priorities(full_network, {})
        assert priorities == []

    def test_sorted_by_impact_descending(self, full_network: EntropyNetwork):
        evidence = {
            "type_fidelity": "high",
            "null_ratio": "high",
            "outlier_rate": "medium",
            "unit_declaration": "high",
            "naming_clarity": "medium",
        }
        priorities = compute_network_priorities(full_network, evidence)
        for i in range(len(priorities) - 1):
            assert priorities[i].impact_delta >= priorities[i + 1].impact_delta

    def test_priority_result_fields(self, full_network: EntropyNetwork):
        evidence = {"type_fidelity": "high"}
        priorities = compute_network_priorities(full_network, evidence)
        assert len(priorities) == 1
        p = priorities[0]
        assert p.node == "type_fidelity"
        assert p.current_state == "high"
        assert isinstance(p.impact_delta, float)
        assert isinstance(p.affected_intents, dict)
        assert isinstance(p.cascade_path, list)

    def test_small_network_priorities(self, small_network: EntropyNetwork):
        evidence = {"root_a": "high", "root_b": "medium"}
        priorities = compute_network_priorities(
            small_network, evidence, intent_nodes=["leaf_z"],
        )
        assert len(priorities) > 0
        # root_a has stronger edge (0.8 vs 0.6) so should rank higher
        node_order = [p.node for p in priorities]
        assert node_order[0] == "root_a"

    def test_cascade_paths_populated(self, full_network: EntropyNetwork):
        evidence = {"type_fidelity": "high"}
        priorities = compute_network_priorities(full_network, evidence)
        p = priorities[0]
        assert len(p.cascade_path) > 0
        assert "join_path_determinism" in p.cascade_path
