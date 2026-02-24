"""Bayesian Entropy Network for causal dependency modeling.

Models an 18-node DAG capturing causal dependencies between entropy
sub-dimensions. Enables:
- Inference of unobserved dimensions from observed evidence
- Intervention priority via information-theoretic impact analysis
- Root cause diagnosis for high-entropy intent nodes
- What-if analysis for simulating fixes
"""

from dataraum.entropy.network.bridge import (
    build_dimension_path_to_node_map,
    discretize_score,
    entropy_objects_to_evidence,
)
from dataraum.entropy.network.config import NetworkConfig, get_network_config
from dataraum.entropy.network.inference import (
    forward_propagate,
    what_if_analysis,
)
from dataraum.entropy.network.model import EntropyNetwork
from dataraum.entropy.network.priority import (
    PriorityResult,
    compute_network_priorities,
)

__all__ = [
    # Model
    "EntropyNetwork",
    # Config
    "NetworkConfig",
    "get_network_config",
    # Inference
    "forward_propagate",
    "what_if_analysis",
    # Bridge
    "discretize_score",
    "entropy_objects_to_evidence",
    "build_dimension_path_to_node_map",
    # Priority
    "compute_network_priorities",
    "PriorityResult",
]
