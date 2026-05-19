"""Bridge between EntropyObject scores and Bayesian network evidence.

Converts continuous entropy scores from detectors into discrete states
suitable for network inference, and maps dimension paths to node names.
"""

from dataraum.entropy.models import EntropyObject
from dataraum.entropy.network.model import EntropyNetwork


def discretize_score(
    score: float,
    low_upper: float = 0.3,
    medium_upper: float = 0.6,
) -> str:
    """Convert continuous entropy score [0,1] to discrete state.

    Args:
        score: Entropy score between 0.0 and 1.0.
        low_upper: Upper bound for "low" state (exclusive).
        medium_upper: Upper bound for "medium" state (exclusive).

    Returns:
        One of "low", "medium", "high".
    """
    if score <= low_upper:
        return "low"
    elif score <= medium_upper:
        return "medium"
    return "high"


def build_dimension_path_to_node_map(
    network: EntropyNetwork,
) -> dict[str, str]:
    """Build mapping from 'layer.dimension.sub_dimension' to node_name.

    Args:
        network: The entropy network.

    Returns:
        Dict mapping dimension paths to node names,
        e.g. {"structural.types.type_fidelity": "type_fidelity"}.
    """
    path_map: dict[str, str] = {}
    for name, node_config in network.config.nodes.items():
        path_map[node_config.dimension_path] = name
    return path_map


def entropy_objects_to_evidence(
    objects: list[EntropyObject],
    network: EntropyNetwork,
) -> dict[str, str]:
    """Convert EntropyObjects to network evidence dict.

    Maps each object's dimension_path to a network node name,
    discretizes the score, returns {node_name: state}.
    Skips objects that don't map to any network node.

    Args:
        objects: List of EntropyObject instances from detectors.
        network: The entropy network for node lookup.

    Returns:
        Evidence dict suitable for inference functions,
        e.g. {"type_fidelity": "low", "null_ratio": "high"}.
    """
    path_map = build_dimension_path_to_node_map(network)
    disc = network.config.discretization

    evidence: dict[str, str] = {}
    for obj in objects:
        node_name = path_map.get(obj.dimension_path)
        if node_name is None:
            continue
        state = discretize_score(obj.score, disc.low_upper, disc.medium_upper)
        evidence[node_name] = state

    return evidence
