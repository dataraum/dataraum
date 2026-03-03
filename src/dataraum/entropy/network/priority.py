"""Network-aware intervention priority ranking.

Computes which entropy dimensions to fix first based on their causal
impact on intent nodes via network-derived dependency analysis.
"""

from collections import deque
from dataclasses import dataclass

from dataraum.entropy.network.inference import forward_propagate, what_if_analysis
from dataraum.entropy.network.model import EntropyNetwork


@dataclass
class PriorityResult:
    """Result of intervention priority analysis for a single node."""

    node: str
    current_state: str
    impact_delta: float  # How much fixing this reduces P(intent=high)
    affected_intents: dict[str, float]  # Per-intent delta
    cascade_path: list[str]  # Nodes downstream that would improve


def compute_cascade_paths(
    network: EntropyNetwork,
    node: str,
) -> list[str]:
    """BFS from node through children to find all downstream nodes.

    Args:
        network: The entropy network.
        node: Starting node.

    Returns:
        List of downstream node names (excluding the start node).
    """
    visited: set[str] = set()
    queue: deque[str] = deque()

    for child in network.get_children(node):
        if child not in visited:
            visited.add(child)
            queue.append(child)

    while queue:
        current = queue.popleft()
        for child in network.get_children(current):
            if child not in visited:
                visited.add(child)
                queue.append(child)

    return list(visited)


def compute_network_priorities(
    network: EntropyNetwork,
    evidence: dict[str, str],
    intent_nodes: list[str] | None = None,
) -> list[PriorityResult]:
    """For each high-entropy node, compute intervention priority.

    Algorithm:
    1. Forward propagate current evidence -> baseline P(intent=high)
    2. For each observed node with state != "low":
       a. what_if_analysis: set node to "low"
       b. Compute delta = baseline_P(intent=high) - new_P(intent=high)
       c. PriorityResult(node, current_state, delta, affected_intents)
    3. Sort by max delta descending

    Args:
        network: The entropy network.
        evidence: Current observed states for network nodes.
        intent_nodes: Intent nodes to optimize. None = all intent nodes.

    Returns:
        List of PriorityResult sorted by impact_delta descending.
    """
    if intent_nodes is None:
        intent_nodes = network.get_intent_nodes()

    if not intent_nodes or not evidence:
        return []

    # Step 1: Baseline P(intent=high)
    baseline = forward_propagate(network, evidence, query_nodes=intent_nodes)
    baseline_high: dict[str, float] = {}
    for intent in intent_nodes:
        if intent in baseline:
            baseline_high[intent] = baseline[intent].get("high", 0.0)
        elif intent in evidence:
            # Intent is observed directly
            baseline_high[intent] = 1.0 if evidence[intent] == "high" else 0.0

    # Step 2: For each non-low node, simulate fixing it
    priorities: list[PriorityResult] = []

    for node, state in evidence.items():
        if state == "low":
            continue

        # What-if: set this node to "low"
        intervention = {node: "low"}
        # Remove the node from evidence for the what-if (it's now intervened)
        remaining_evidence = {k: v for k, v in evidence.items() if k != node}

        intervened = what_if_analysis(
            network,
            remaining_evidence,
            intervention,
            target_nodes=intent_nodes,
        )

        # Compute per-intent deltas
        affected_intents: dict[str, float] = {}
        for intent in intent_nodes:
            new_high = 0.0
            if intent in intervened:
                new_high = intervened[intent].get("high", 0.0)
            elif intent in remaining_evidence:
                new_high = 1.0 if remaining_evidence[intent] == "high" else 0.0

            delta = baseline_high.get(intent, 0.0) - new_high
            if delta > 0.001:  # Only record meaningful improvements
                affected_intents[intent] = round(delta, 4)

        max_delta = max(affected_intents.values()) if affected_intents else 0.0

        cascade_path = compute_cascade_paths(network, node)

        priorities.append(
            PriorityResult(
                node=node,
                current_state=state,
                impact_delta=round(max_delta, 4),
                affected_intents=affected_intents,
                cascade_path=cascade_path,
            )
        )

    # Sort by impact_delta descending
    priorities.sort(key=lambda p: p.impact_delta, reverse=True)

    return priorities
