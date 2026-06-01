"""Entropy network — a weighted DAG of entropy sub-dimensions.

A thin, dependency-free holder over the configured graph (``network.yaml``):
nodes are entropy sub-dimensions, edges carry causal influence strengths.
Detector scores are rolled up the DAG with a noisy-OR combiner (see
``rollup.py``) — there is no probabilistic inference engine.

The model caches the parent map and a topological order so per-column rollups
don't recompute them.
"""

from __future__ import annotations

from dataraum.entropy.network.config import NetworkConfig, NodeConfig, get_network_config
from dataraum.entropy.network.rollup import parent_map, topo_order


class EntropyNetwork:
    """Weighted DAG of entropy dimension dependencies.

    Usage:
        network = EntropyNetwork()
        # Use with rollup functions from rollup.py, passing network.config.
    """

    def __init__(self, config: NetworkConfig | None = None) -> None:
        """Build network from config.

        Args:
            config: Network configuration. If None, loads from default path.

        Raises:
            ValueError: If an edge references an undefined node, or the graph
                contains a cycle.
        """
        if config is None:
            config = get_network_config()

        self._config = config

        node_names = set(config.nodes.keys())
        for edge in config.edges:
            if edge.parent not in node_names:
                raise ValueError(f"Edge references undefined parent node: '{edge.parent}'")
            if edge.child not in node_names:
                raise ValueError(f"Edge references undefined child node: '{edge.child}'")

        # Cache structure; topo_order also validates acyclicity.
        self._parent_map = parent_map(config)
        self._topo_order = topo_order(config)
        self._children: dict[str, list[str]] = {name: [] for name in config.nodes}
        for edge in config.edges:
            self._children[edge.parent].append(edge.child)

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

    @property
    def parent_map(self) -> dict[str, list[tuple[str, float]]]:
        """Cached child -> [(parent, strength)] map."""
        return self._parent_map

    @property
    def topo_order(self) -> list[str]:
        """Cached topological order (parents before children)."""
        return self._topo_order

    def get_node_config(self, name: str) -> NodeConfig:
        """Get configuration for a specific node.

        Raises:
            KeyError: If node doesn't exist.
        """
        if name not in self._config.nodes:
            raise KeyError(f"Unknown node: '{name}'")
        return self._config.nodes[name]

    def get_intent_nodes(self) -> list[str]:
        """Get intent nodes (nodes in the 'intent' layer)."""
        return [name for name, node in self._config.nodes.items() if node.layer == "intent"]
