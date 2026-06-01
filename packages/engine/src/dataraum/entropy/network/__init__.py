"""Entropy network — weighted DAG of entropy sub-dimensions.

Models a DAG capturing dependencies between entropy sub-dimensions (detector
outputs → causal composites → query/aggregation/reporting intent readiness).
Detector scores are rolled up the DAG with a deterministic noisy-OR combiner
(``rollup.py``) using the edge strengths in ``network.yaml`` — no probabilistic
inference, no fitted constants. Enables:

- Readiness per intent (ready / investigate / blocked) from observed evidence
- Intervention priority — which fix lowers intent risk the most
"""

from dataraum.entropy.network.bridge import (
    build_dimension_path_to_node_map,
    discretize_score,
    entropy_objects_to_evidence,
    entropy_objects_to_scores,
)
from dataraum.entropy.network.config import NetworkConfig, get_network_config
from dataraum.entropy.network.model import EntropyNetwork
from dataraum.entropy.network.rollup import (
    PriorityResult,
    compute_priorities,
    intent_nodes,
    readiness_from_risk,
    roll_up,
)

__all__ = [
    # Model
    "EntropyNetwork",
    # Config
    "NetworkConfig",
    "get_network_config",
    # Rollup
    "roll_up",
    "readiness_from_risk",
    "compute_priorities",
    "intent_nodes",
    "PriorityResult",
    # Bridge
    "discretize_score",
    "entropy_objects_to_evidence",
    "entropy_objects_to_scores",
    "build_dimension_path_to_node_map",
]
