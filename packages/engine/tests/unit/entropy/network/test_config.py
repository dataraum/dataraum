"""Tests for network configuration loading."""

from dataraum.entropy.network.config import NetworkConfig, get_network_config


class TestNetworkConfigLoading:
    """Test loading config/entropy/network.yaml."""

    def test_loads_successfully(self, full_config: NetworkConfig):
        assert full_config is not None
        assert len(full_config.states) == 3

    def test_node_count(self, full_config: NetworkConfig):
        # benford moved off the network onto the loss path (DAT-442): 20 → 19.
        assert len(full_config.nodes) == 19

    def test_edge_count(self, full_config: NetworkConfig):
        # Count edges in the YAML (benford_compliance → aggregation_intent removed).
        assert len(full_config.edges) == 35

    def test_states_are_low_medium_high(self, full_config: NetworkConfig):
        assert full_config.states == ["low", "medium", "high"]


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


class TestNodeConfig:
    """Test node configuration properties."""

    def test_dimension_path(self, full_config: NetworkConfig):
        node = full_config.nodes["type_fidelity"]
        assert node.dimension_path == "structural.types.type_fidelity"

    def test_caching(self):
        """get_network_config returns same instance on repeated calls."""
        c1 = get_network_config()
        c2 = get_network_config()
        assert c1 is c2
