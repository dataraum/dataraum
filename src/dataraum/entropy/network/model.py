"""Bayesian Entropy Network model.

Wraps pgmpy's DiscreteBayesianNetwork with configuration-driven construction
and provides a clean interface for the entropy layer.
"""

from pgmpy.models import DiscreteBayesianNetwork

from dataraum.entropy.network.config import NetworkConfig, NodeConfig, get_network_config
from dataraum.entropy.network.cpts import generate_all_cpds


class EntropyNetwork:
    """Bayesian network for entropy dimension dependencies.

    Builds a DAG from config where nodes represent entropy sub-dimensions
    and edges represent causal dependencies. CPTs are generated from
    edge strengths using the influence matrix approach.

    Usage:
        network = EntropyNetwork()
        # Use with inference functions from inference.py
    """

    def __init__(self, config: NetworkConfig | None = None) -> None:
        """Build network from config.

        Args:
            config: Network configuration. If None, loads from default path.

        Raises:
            ValueError: If network structure is invalid (cycles, missing nodes).
        """
        if config is None:
            config = get_network_config()

        self._config = config

        # Build edge list for pgmpy
        edge_tuples = [(e.parent, e.child) for e in config.edges]

        # Validate all edge endpoints reference defined nodes
        node_names = set(config.nodes.keys())
        for edge in config.edges:
            if edge.parent not in node_names:
                raise ValueError(f"Edge references undefined parent node: '{edge.parent}'")
            if edge.child not in node_names:
                raise ValueError(f"Edge references undefined child node: '{edge.child}'")

        # Create DiscreteBayesianNetwork
        self._model = DiscreteBayesianNetwork(edge_tuples)

        # Add isolated nodes (nodes with no edges)
        for name in config.nodes:
            if name not in self._model.nodes():
                self._model.add_node(name)

        # Generate and add CPDs
        cpds = generate_all_cpds(config)
        for cpd in cpds:
            self._model.add_cpds(cpd)

        # Validate the model
        if not self._model.check_model():
            raise ValueError("Network model validation failed. Check CPDs and structure.")

    @property
    def model(self) -> DiscreteBayesianNetwork:
        """Access the underlying pgmpy DiscreteBayesianNetwork."""
        return self._model

    @property
    def config(self) -> NetworkConfig:
        """Access the network configuration."""
        return self._config

    @property
    def node_names(self) -> list[str]:
        """All node names in the network."""
        return list(self._config.nodes.keys())

    @property
    def states(self) -> list[str]:
        """Discrete state names (e.g., ['low', 'medium', 'high'])."""
        return self._config.states

    def get_node_config(self, name: str) -> NodeConfig:
        """Get configuration for a specific node.

        Args:
            name: Node name.

        Raises:
            KeyError: If node doesn't exist.
        """
        if name not in self._config.nodes:
            raise KeyError(f"Unknown node: '{name}'")
        return self._config.nodes[name]

    def get_parents(self, node: str) -> list[str]:
        """Get parent node names for a given node."""
        return list(self._model.get_parents(node))

    def get_children(self, node: str) -> list[str]:
        """Get child node names for a given node."""
        return list(self._model.get_children(node))

    def get_root_nodes(self) -> list[str]:
        """Get nodes with no parents (root/exogenous nodes)."""
        return [name for name in self._config.nodes if not self.get_parents(name)]

    def get_leaf_nodes(self) -> list[str]:
        """Get nodes with no children (leaf/terminal nodes)."""
        return [name for name in self._config.nodes if not self.get_children(name)]

    def get_intent_nodes(self) -> list[str]:
        """Get intent nodes (nodes in the 'intent' layer)."""
        return [name for name, node in self._config.nodes.items() if node.layer == "intent"]
