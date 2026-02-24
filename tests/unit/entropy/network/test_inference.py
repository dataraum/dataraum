"""Tests for inference operations."""


from dataraum.entropy.network.inference import (
    forward_propagate,
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


