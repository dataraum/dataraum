"""Bayesian Entropy Network configuration loader.

Loads network topology and CPT generation parameters from
config/entropy/network.yaml. Follows the same pattern as entropy/config.py.

Usage:
    from dataraum.entropy.network.config import get_network_config

    config = get_network_config()
    nodes = config.nodes
    edges = config.edges
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dataraum.core.config import get_config_file
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

NETWORK_CONFIG_PATH = "entropy/network.yaml"


@dataclass(frozen=True)
class NodeConfig:
    """Configuration for a single network node."""

    name: str
    layer: str
    dimension: str
    sub_dimension: str
    prior: list[float] | None = None  # Only root nodes have priors

    @property
    def dimension_path(self) -> str:
        """Full dimension path: layer.dimension.sub_dimension."""
        return f"{self.layer}.{self.dimension}.{self.sub_dimension}"

    @property
    def is_root(self) -> bool:
        """Root nodes have explicit prior distributions."""
        return self.prior is not None


@dataclass(frozen=True)
class EdgeConfig:
    """Configuration for a directed edge between nodes."""

    parent: str
    child: str
    strength: float  # Causal influence strength in (0, 1]


@dataclass(frozen=True)
class CptGenerationConfig:
    """Parameters for CPT generation from edge strengths."""

    influence_blend: float = 0.7
    pessimistic_shift: float = 0.1
    min_probability: float = 0.01


@dataclass(frozen=True)
class DiscretizationConfig:
    """Thresholds for converting continuous scores to discrete states."""

    low_upper: float = 0.3
    medium_upper: float = 0.6


@dataclass(frozen=True)
class NetworkConfig:
    """Complete Bayesian Entropy Network configuration."""

    states: list[str] = field(default_factory=lambda: ["low", "medium", "high"])
    discretization: DiscretizationConfig = field(default_factory=DiscretizationConfig)
    nodes: dict[str, NodeConfig] = field(default_factory=dict)
    edges: list[EdgeConfig] = field(default_factory=list)
    cpt_generation: CptGenerationConfig = field(default_factory=CptGenerationConfig)


# Module-level cache
_config_cache: NetworkConfig | None = None
_config_path_cache: Path | None = None


def load_network_config(config_path: Path | None = None) -> NetworkConfig:
    """Load network configuration from YAML file.

    Args:
        config_path: Absolute path to network.yaml, for testing.
                     If None, resolves via central config loader.

    Returns:
        NetworkConfig with loaded values.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        RuntimeError: If config file cannot be parsed.
    """
    if config_path is None:
        config_path = get_config_file(NETWORK_CONFIG_PATH)

    if not config_path.exists():
        raise FileNotFoundError(f"Required network config not found: {config_path}.")

    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}
        return _parse_config(raw)
    except FileNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(f"Error loading network config from {config_path}: {e}") from e


def _parse_config(raw: dict[str, Any]) -> NetworkConfig:
    """Parse raw YAML config into NetworkConfig."""
    states = raw.get("states", ["low", "medium", "high"])

    # Discretization
    disc_raw = raw.get("discretization", {})
    discretization = DiscretizationConfig(
        low_upper=disc_raw.get("low_upper", 0.3),
        medium_upper=disc_raw.get("medium_upper", 0.6),
    )

    # Nodes
    nodes: dict[str, NodeConfig] = {}
    for name, node_raw in raw.get("nodes", {}).items():
        prior = node_raw.get("prior")
        nodes[name] = NodeConfig(
            name=name,
            layer=node_raw["layer"],
            dimension=node_raw["dimension"],
            sub_dimension=node_raw["sub_dimension"],
            prior=list(prior) if prior is not None else None,
        )

    # Edges
    edges: list[EdgeConfig] = []
    for edge_raw in raw.get("edges", []):
        edges.append(
            EdgeConfig(
                parent=edge_raw[0],
                child=edge_raw[1],
                strength=float(edge_raw[2]),
            )
        )

    # CPT generation params
    cpt_raw = raw.get("cpt_generation", {})
    cpt_generation = CptGenerationConfig(
        influence_blend=cpt_raw.get("influence_blend", 0.7),
        pessimistic_shift=cpt_raw.get("pessimistic_shift", 0.1),
        min_probability=cpt_raw.get("min_probability", 0.01),
    )

    return NetworkConfig(
        states=states,
        discretization=discretization,
        nodes=nodes,
        edges=edges,
        cpt_generation=cpt_generation,
    )


def get_network_config(config_path: Path | None = None) -> NetworkConfig:
    """Get network configuration, using cache if available.

    Args:
        config_path: Optional absolute path to override default config location.

    Returns:
        Cached or newly loaded NetworkConfig.
    """
    global _config_cache, _config_path_cache

    # Resolve path
    if config_path is not None:
        path = config_path
    elif _config_path_cache is not None:
        path = _config_path_cache
    else:
        path = get_config_file(NETWORK_CONFIG_PATH)

    # Return cached config if path matches
    if _config_cache is not None and _config_path_cache == path:
        return _config_cache

    # Load and cache
    _config_cache = load_network_config(path)
    _config_path_cache = path
    return _config_cache
