"""Inference operations for the Bayesian Entropy Network.

Provides two core inference operations:
- forward_propagate: Compute posterior distributions given observed evidence
- what_if_analysis: Simulate interventions (do-calculus)
"""

from pgmpy.inference import CausalInference, VariableElimination

from dataraum.entropy.network.model import EntropyNetwork


def forward_propagate(
    network: EntropyNetwork,
    evidence: dict[str, str],
    query_nodes: list[str] | None = None,
) -> dict[str, dict[str, float]]:
    """Propagate observed entropy through network.

    Given observed states for some nodes, compute the posterior probability
    distribution for all other nodes.

    Args:
        network: The entropy network.
        evidence: Observed node states, e.g. {"type_fidelity": "high"}.
        query_nodes: Nodes to query. None = all non-evidence nodes.

    Returns:
        Dict mapping each query node to its posterior distribution,
        e.g. {"parse_success": {"low": 0.1, "medium": 0.3, "high": 0.6}}.
    """
    if query_nodes is None:
        query_nodes = [n for n in network.node_names if n not in evidence]

    if not query_nodes:
        return {}

    ve = VariableElimination(network.model)
    results: dict[str, dict[str, float]] = {}

    for node in query_nodes:
        if node in evidence:
            continue
        factor = ve.query(
            variables=[node],
            evidence=evidence,
            show_progress=False,
        )
        values = factor.values
        results[node] = {state: float(values[i]) for i, state in enumerate(network.states)}

    return results


def what_if_analysis(
    network: EntropyNetwork,
    current_evidence: dict[str, str],
    intervention: dict[str, str],
    target_nodes: list[str] | None = None,
) -> dict[str, dict[str, float]]:
    """Simulate intervention: what happens if we fix node X?

    Uses do-calculus via pgmpy's CausalInference to model interventions
    (setting a node to a specific state, removing its parent dependencies).

    Args:
        network: The entropy network.
        current_evidence: Current observed states.
        intervention: Nodes to intervene on, e.g. {"type_fidelity": "low"}.
        target_nodes: Nodes to compute posteriors for. None = intent nodes.

    Returns:
        Dict mapping each target node to its posterior distribution
        under the intervention.
    """
    if target_nodes is None:
        target_nodes = network.get_intent_nodes()

    if not target_nodes:
        return {}

    # Separate evidence: current observations minus intervened nodes
    obs_evidence = {k: v for k, v in current_evidence.items() if k not in intervention}

    ci = CausalInference(network.model)
    results: dict[str, dict[str, float]] = {}

    for node in target_nodes:
        if node in intervention or node in current_evidence:
            continue
        factor = ci.query(
            variables=[node],
            do=intervention,
            evidence=obs_evidence if obs_evidence else None,
            show_progress=False,
        )
        values = factor.values
        results[node] = {state: float(values[i]) for i, state in enumerate(network.states)}

    return results
