"""Tests for inference operations."""


from dataraum.entropy.network.inference import (
    backward_diagnose,
    forward_propagate,
    most_probable_explanation,
    what_if_analysis,
)
from dataraum.entropy.network.model import EntropyNetwork


class TestForwardPropagate:
    """Test forward propagation through the network."""

    def test_high_evidence_shifts_intent_toward_high(self, full_network: EntropyNetwork):
        evidence = {
            "type_fidelity": "high",
            "null_ratio": "high",
            "outlier_rate": "high",
        }
        result = forward_propagate(full_network, evidence, query_nodes=["query_intent"])
        p_high = result["query_intent"]["high"]
        assert p_high > 0.4, f"Expected query_intent P(high) > 0.4, got {p_high}"

    def test_low_evidence_shifts_intent_toward_low(self, full_network: EntropyNetwork):
        evidence = {
            "type_fidelity": "low",
            "null_ratio": "low",
            "outlier_rate": "low",
        }
        result = forward_propagate(full_network, evidence, query_nodes=["query_intent"])
        p_low = result["query_intent"]["low"]
        assert p_low > 0.3, f"Expected query_intent P(low) > 0.3, got {p_low}"

    def test_distributions_sum_to_one(self, full_network: EntropyNetwork):
        evidence = {"type_fidelity": "medium"}
        result = forward_propagate(full_network, evidence, query_nodes=["join_path_determinism"])
        dist = result["join_path_determinism"]
        total = sum(dist.values())
        assert abs(total - 1.0) < 1e-6

    def test_empty_evidence_returns_prior_based_results(self, full_network: EntropyNetwork):
        result = forward_propagate(full_network, {}, query_nodes=["type_fidelity"])
        # With no evidence, should get the prior
        dist = result["type_fidelity"]
        assert abs(dist["low"] - 0.7) < 0.01
        assert abs(dist["medium"] - 0.2) < 0.01
        assert abs(dist["high"] - 0.1) < 0.01

    def test_partial_evidence_works(self, full_network: EntropyNetwork):
        evidence = {"type_fidelity": "high"}
        result = forward_propagate(full_network, evidence)
        # Should return results for all non-evidence nodes
        assert "type_fidelity" not in result
        assert "join_path_determinism" in result
        assert "query_intent" in result

    def test_query_nodes_filters_output(self, full_network: EntropyNetwork):
        result = forward_propagate(
            full_network, {"type_fidelity": "high"},
            query_nodes=["join_path_determinism"],
        )
        assert list(result.keys()) == ["join_path_determinism"]

    def test_empty_query_nodes_returns_empty(self, full_network: EntropyNetwork):
        result = forward_propagate(full_network, {"type_fidelity": "high"}, query_nodes=[])
        assert result == {}

    def test_small_network_propagation(self, small_network: EntropyNetwork):
        result = forward_propagate(
            small_network, {"root_a": "high", "root_b": "high"},
            query_nodes=["leaf_z"],
        )
        assert "leaf_z" in result
        assert result["leaf_z"]["high"] > result["leaf_z"]["low"]


class TestBackwardDiagnose:
    """Test backward diagnosis (root cause analysis)."""

    def test_high_aggregation_intent_identifies_causes(self, full_network: EntropyNetwork):
        result = backward_diagnose(
            full_network, {"aggregation_intent": "high"},
        )
        # All root nodes should be returned
        assert set(result.keys()) == set(full_network.get_root_nodes())
        # unit_declaration should have elevated P(high) since it has
        # strong edge to aggregation_safety -> aggregation_intent
        p_high_unit = result["unit_declaration"]["high"]
        assert p_high_unit > 0.2

    def test_custom_candidate_causes(self, full_network: EntropyNetwork):
        result = backward_diagnose(
            full_network,
            {"aggregation_intent": "high"},
            candidate_causes=["unit_declaration", "null_ratio"],
        )
        assert set(result.keys()) == {"unit_declaration", "null_ratio"}

    def test_distributions_sum_to_one(self, full_network: EntropyNetwork):
        result = backward_diagnose(full_network, {"query_intent": "high"})
        for node, dist in result.items():
            total = sum(dist.values())
            assert abs(total - 1.0) < 1e-6, f"{node} distribution sums to {total}"


class TestWhatIfAnalysis:
    """Test what-if intervention analysis."""

    def test_fixing_node_reduces_intent_high(self, full_network: EntropyNetwork):
        # Baseline: type_fidelity is high
        baseline = forward_propagate(
            full_network, {"type_fidelity": "high"},
            query_nodes=["query_intent"],
        )
        # What-if: fix type_fidelity to low
        fixed = what_if_analysis(
            full_network,
            current_evidence={},
            intervention={"type_fidelity": "low"},
            target_nodes=["query_intent"],
        )
        baseline_high = baseline["query_intent"]["high"]
        fixed_high = fixed["query_intent"]["high"]
        assert fixed_high < baseline_high

    def test_returns_only_target_nodes(self, full_network: EntropyNetwork):
        result = what_if_analysis(
            full_network,
            current_evidence={},
            intervention={"type_fidelity": "low"},
            target_nodes=["query_intent"],
        )
        assert list(result.keys()) == ["query_intent"]

    def test_defaults_to_intent_nodes(self, full_network: EntropyNetwork):
        result = what_if_analysis(
            full_network,
            current_evidence={},
            intervention={"type_fidelity": "low"},
        )
        assert set(result.keys()) == {"query_intent", "aggregation_intent", "reporting_intent"}


class TestMostProbableExplanation:
    """Test MAP inference."""

    def test_returns_valid_states(self, full_network: EntropyNetwork):
        evidence = {"type_fidelity": "high", "null_ratio": "high"}
        result = most_probable_explanation(full_network, evidence)
        valid_states = set(full_network.states)
        for node, state in result.items():
            assert state in valid_states, f"Node {node} has invalid state {state}"

    def test_covers_all_non_evidence_nodes(self, full_network: EntropyNetwork):
        evidence = {"type_fidelity": "high"}
        result = most_probable_explanation(full_network, evidence)
        expected_nodes = {n for n in full_network.node_names if n != "type_fidelity"}
        assert set(result.keys()) == expected_nodes

    def test_empty_evidence(self, full_network: EntropyNetwork):
        result = most_probable_explanation(full_network, {})
        assert len(result) == 15

    def test_small_network_map(self, small_network: EntropyNetwork):
        result = most_probable_explanation(small_network, {"root_a": "high"})
        assert "root_b" in result
        assert "child_x" in result
        assert "leaf_z" in result
