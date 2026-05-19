"""Tests for network configuration loading."""

import math

from dataraum.entropy.network.config import NetworkConfig, get_network_config


class TestNetworkConfigLoading:
    """Test loading config/entropy/network.yaml."""

    def test_loads_successfully(self, full_config: NetworkConfig):
        assert full_config is not None
        assert len(full_config.states) == 3

    def test_node_count(self, full_config: NetworkConfig):
        assert len(full_config.nodes) == 19

    def test_edge_count(self, full_config: NetworkConfig):
        # Count edges in the YAML
        assert len(full_config.edges) == 32

    def test_states_are_low_medium_high(self, full_config: NetworkConfig):
        assert full_config.states == ["low", "medium", "high"]


class TestRootNodePriors:
    """Test that root nodes have valid priors."""

    def test_root_nodes_have_priors(self, full_config: NetworkConfig):
        root_names = [
            "type_fidelity",
            "null_ratio",
            "outlier_rate",
            "naming_clarity",
            "unit_declaration",
            "time_role",
            "temporal_drift",
            "benford_compliance",
        ]
        for name in root_names:
            node = full_config.nodes[name]
            assert node.prior is not None, f"Root node {name} missing prior"

    def test_priors_sum_to_one(self, full_config: NetworkConfig):
        for name, node in full_config.nodes.items():
            if node.prior is not None:
                total = sum(node.prior)
                assert math.isclose(total, 1.0, rel_tol=1e-6), (
                    f"Prior for {name} sums to {total}, not 1.0"
                )

    def test_priors_have_correct_length(self, full_config: NetworkConfig):
        n_states = len(full_config.states)
        for name, node in full_config.nodes.items():
            if node.prior is not None:
                assert len(node.prior) == n_states, (
                    f"Prior for {name} has {len(node.prior)} values, expected {n_states}"
                )


class TestEdgeValidation:
    """Test edge configuration validity."""

    def test_edge_strengths_in_valid_range(self, full_config: NetworkConfig):
        for edge in full_config.edges:
            assert 0.0 < edge.strength <= 1.0, (
                f"Edge {edge.parent}->{edge.child} has strength {edge.strength} outside (0, 1]"
            )

    def test_all_edge_endpoints_reference_defined_nodes(self, full_config: NetworkConfig):
        node_names = set(full_config.nodes.keys())
        for edge in full_config.edges:
            assert edge.parent in node_names, f"Edge parent '{edge.parent}' not in defined nodes"
            assert edge.child in node_names, f"Edge child '{edge.child}' not in defined nodes"

    def test_child_nodes_have_no_priors(self, full_config: NetworkConfig):
        """Nodes that are only children (no prior) should not have explicit priors."""
        # Nodes that are children but NOT roots can't have priors
        children = {edge.child for edge in full_config.edges}
        roots = {name for name, node in full_config.nodes.items() if node.prior is not None}
        pure_children = children - roots
        for name in pure_children:
            node = full_config.nodes[name]
            if node.prior is not None:
                raise AssertionError(f"Pure child node '{name}' should not have an explicit prior")


class TestNodeConfig:
    """Test node configuration properties."""

    def test_dimension_path(self, full_config: NetworkConfig):
        node = full_config.nodes["type_fidelity"]
        assert node.dimension_path == "structural.types.type_fidelity"

    def test_is_root_with_prior(self, full_config: NetworkConfig):
        assert full_config.nodes["type_fidelity"].is_root

    def test_is_not_root_without_prior(self, full_config: NetworkConfig):
        assert not full_config.nodes["join_path_determinism"].is_root

    def test_caching(self):
        """get_network_config returns same instance on repeated calls."""
        c1 = get_network_config()
        c2 = get_network_config()
        assert c1 is c2
