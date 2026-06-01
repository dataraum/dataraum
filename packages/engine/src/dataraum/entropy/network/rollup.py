"""Deterministic entropy rollup — replaces the pgmpy Bayesian network.

The network is a DAG of entropy sub-dimensions (``network.yaml``): observable
roots (detector outputs), causal composites, and intent leaves
(query / aggregation / reporting readiness). We no longer run probabilistic
inference over it. Instead we roll observed detector scores up the DAG with a
**noisy-OR** combiner whose link probabilities are the existing edge strengths.

Why noisy-OR rather than a weighted average:

- It **compounds**: several mildly-bad parents push a child toward 1.0, which a
  mean cannot do (calibration already learned that weighted-average composites
  hide problems — see relationship_entropy's move to max aggregation).
- It is **monotonic** and bounded in [0, 1]: more / worse evidence never lowers
  risk, and a single parent's contribution is capped at its edge strength
  (``strength`` encodes "how much this signal alone condemns the child").
- It has **one parameter per edge** (the strength we already author) — no priors,
  no CPDs, no fitted constants. Honest about being a hand-weighted rollup.

No priors means no prior leakage: a node with no *observed* parents is simply
absent from the result, so the per-column subgraph pruning the BBN needed is
gone. Each node's risk is ``r``; for a child with resolved parents P:

    r(child) = 1 - Π_{p in P} (1 - strength(p, child) * r(p))

Observed roots take their raw detector score as ``r`` (no discretization loss).
Readiness bands the intent risk with the same thresholds the BBN used.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dataraum.entropy.network.config import NetworkConfig


@dataclass
class PriorityResult:
    """Causal contribution of one observed node to intent risk.

    ``impact_delta`` is how much intent risk would drop if this node were fixed
    to clean (risk 0) — computed by re-running the rollup, not do-calculus.
    """

    node: str
    current_state: str
    impact_delta: float
    affected_intents: dict[str, float] = field(default_factory=dict)
    cascade_path: list[str] = field(default_factory=list)


def parent_map(config: NetworkConfig) -> dict[str, list[tuple[str, float]]]:
    """Build child -> [(parent, strength)] from the edge list."""
    pmap: dict[str, list[tuple[str, float]]] = {name: [] for name in config.nodes}
    for edge in config.edges:
        pmap[edge.child].append((edge.parent, edge.strength))
    return pmap


def topo_order(config: NetworkConfig) -> list[str]:
    """Topologically sort nodes (parents before children) via Kahn's algorithm.

    Raises:
        ValueError: if the graph contains a cycle.
    """
    pmap = parent_map(config)
    indegree = {name: len(pmap[name]) for name in config.nodes}
    children: dict[str, list[str]] = {name: [] for name in config.nodes}
    for edge in config.edges:
        children[edge.parent].append(edge.child)

    # Deterministic order: iterate node dict insertion order for ready set.
    ready = [n for n in config.nodes if indegree[n] == 0]
    order: list[str] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for child in children[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)

    if len(order) != len(config.nodes):
        cyclic = [n for n, d in indegree.items() if d > 0]
        raise ValueError(f"Network has a cycle involving: {sorted(cyclic)}")
    return order


def roll_up(
    config: NetworkConfig,
    scores: dict[str, float],
    *,
    low_upper: float | None = None,
    _pmap: dict[str, list[tuple[str, float]]] | None = None,
    _order: list[str] | None = None,
) -> dict[str, float]:
    """Propagate observed scores up the DAG with noisy-OR.

    Scores in the "low" band (<= ``low_upper``) are treated as clean and dropped:
    the system defines that floor as the detection threshold, so a low score is
    noise, not evidence. Without this gate, many sub-threshold signals would
    noisy-OR into a false "investigate" and destroy precision.

    Args:
        config: Network configuration (nodes + weighted edges).
        scores: Observed risk per node in [0, 1] (raw detector scores), for the
            roots / observable children that have evidence.
        low_upper: Clean-band ceiling; observations at or below it contribute
            nothing. Defaults to ``config.discretization.low_upper``.
        _pmap, _order: optional precomputed parent map / topo order (the network
            holds these so we don't recompute per column).

    Returns:
        Risk per node for every node *derivable* from the (above-floor) evidence.
        Observed nodes keep their score; unobserved nodes with no resolved
        parents are omitted (no prior leakage).
    """
    pmap = _pmap if _pmap is not None else parent_map(config)
    order = _order if _order is not None else topo_order(config)
    floor = low_upper if low_upper is not None else config.discretization.low_upper

    risk: dict[str, float] = {k: _clamp(v) for k, v in scores.items() if v > floor}

    for node in order:
        if node in risk:
            continue  # observed — keep the raw score
        present = [(p, s) for (p, s) in pmap.get(node, []) if p in risk]
        if not present:
            continue  # not derivable from evidence — omit (no prior)
        prod = 1.0
        for parent, strength in present:
            prod *= 1.0 - strength * risk[parent]
        risk[node] = 1.0 - prod

    return risk


def readiness_from_risk(risk: float, low_upper: float, medium_upper: float) -> str:
    """Band an intent risk into ready / investigate / blocked.

    Same thresholds the BBN used on P(intent=high).
    """
    if risk > medium_upper:
        return "blocked"
    if risk > low_upper:
        return "investigate"
    return "ready"


def intent_nodes(config: NetworkConfig) -> list[str]:
    """Intent-layer node names."""
    return [name for name, n in config.nodes.items() if n.layer == "intent"]


def cascade_paths(config: NetworkConfig, node: str) -> list[str]:
    """All nodes downstream of ``node`` (BFS over children), excluding it."""
    children: dict[str, list[str]] = {name: [] for name in config.nodes}
    for edge in config.edges:
        children[edge.parent].append(edge.child)

    visited: set[str] = set()
    queue = list(children.get(node, []))
    while queue:
        cur = queue.pop(0)
        if cur in visited:
            continue
        visited.add(cur)
        queue.extend(children.get(cur, []))
    return list(visited)


def compute_priorities(
    config: NetworkConfig,
    scores: dict[str, float],
    *,
    low_upper: float = 0.3,
    intents: list[str] | None = None,
) -> list[PriorityResult]:
    """Rank observed non-clean nodes by how much fixing each lowers intent risk.

    For each observed node above ``low_upper``, re-run the rollup with that node
    pinned to 0 and measure the drop in each intent's risk. Pure recomputation —
    the deterministic stand-in for the BBN's do-calculus what-if.
    """
    targets = intents if intents is not None else intent_nodes(config)
    if not targets or not scores:
        return []

    pmap = parent_map(config)
    order = topo_order(config)

    baseline = roll_up(config, scores, _pmap=pmap, _order=order)
    base_high = {i: baseline.get(i, 0.0) for i in targets}

    results: list[PriorityResult] = []
    for node, score in scores.items():
        if score <= low_upper:
            continue
        fixed_scores = dict(scores)
        fixed_scores[node] = 0.0
        fixed = roll_up(config, fixed_scores, _pmap=pmap, _order=order)

        affected: dict[str, float] = {}
        for intent in targets:
            delta = base_high.get(intent, 0.0) - fixed.get(intent, 0.0)
            if delta > 0.001:
                affected[intent] = round(delta, 4)

        max_delta = max(affected.values()) if affected else 0.0
        results.append(
            PriorityResult(
                node=node,
                current_state=_state(score, low_upper),
                impact_delta=round(max_delta, 4),
                affected_intents=affected,
                cascade_path=cascade_paths(config, node),
            )
        )

    results.sort(key=lambda p: p.impact_delta, reverse=True)
    return results


def _clamp(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def _state(score: float, low_upper: float, medium_upper: float = 0.6) -> str:
    if score > medium_upper:
        return "high"
    if score > low_upper:
        return "medium"
    return "low"
