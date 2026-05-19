"""Bayesian Entropy Network model.

Wraps pgmpy's DiscreteBayesianNetwork with configuration-driven construction
and provides a clean interface for the entropy layer.
"""

from __future__ import annotations

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

    def get_intent_nodes(self) -> list[str]:
        """Get intent nodes (nodes in the 'intent' layer)."""
        return [name for name, node in self._config.nodes.items() if node.layer == "intent"]

    def subgraph(self, observed_nodes: set[str]) -> EntropyNetwork:
        """Build a subgraph relevant to the observed evidence.

        Iteratively removes nodes that are unobserved, non-intent, and
        have no remaining parents in the kept set. This eliminates prior
        leakage from unobserved root nodes while keeping all nodes that
        are inferrable from observed evidence.

        Args:
            observed_nodes: Node names that have evidence (from detectors).

        Returns:
            A new EntropyNetwork containing only relevant nodes, or self
            if all nodes are relevant (no pruning needed).
        """
        all_node_names = set(self._config.nodes.keys())

        # Build parent map from edges
        parent_map: dict[str, set[str]] = {n: set() for n in all_node_names}
        for edge in self._config.edges:
            parent_map[edge.child].add(edge.parent)

        # Iteratively remove unobserved nodes with no kept parents.
        # Root nodes have no parents, so "no kept parents" is vacuously true
        # for them — unobserved roots are always removed.
        # Intent leaves are also removed if they lose all parents (they'd
        # have uniform P(high)=1/n which produces false "investigate" signals).
        kept = set(all_node_names)
        while True:
            removable = set()
            for node in kept:
                if node in observed_nodes:
                    continue
                parents_in_kept = parent_map[node] & kept
                if not parents_in_kept:
                    removable.add(node)
            if not removable:
                break
            kept -= removable

        if kept == all_node_names:
            return self

        # Build filtered config
        n_states = len(self._config.states)
        uniform_prior = [1.0 / n_states] * n_states

        # Determine which nodes lost all parents (become new roots)
        kept_parent_map: dict[str, set[str]] = {n: parent_map[n] & kept for n in kept}

        filtered_nodes: dict[str, NodeConfig] = {}
        for name in kept:
            node_cfg = self._config.nodes[name]
            if node_cfg.prior is None and not kept_parent_map[name]:
                # Non-root node that lost all parents — assign uniform prior
                filtered_nodes[name] = NodeConfig(
                    name=name,
                    layer=node_cfg.layer,
                    dimension=node_cfg.dimension,
                    sub_dimension=node_cfg.sub_dimension,
                    prior=uniform_prior,
                )
            else:
                filtered_nodes[name] = node_cfg

        filtered_edges = [e for e in self._config.edges if e.parent in kept and e.child in kept]

        filtered_config = NetworkConfig(
            states=self._config.states,
            discretization=self._config.discretization,
            nodes=filtered_nodes,
            edges=filtered_edges,
            cpt_generation=self._config.cpt_generation,
        )

        return EntropyNetwork(config=filtered_config)
