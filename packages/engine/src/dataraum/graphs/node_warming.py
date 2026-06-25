"""The cross-metric DAG of unique cache-keyed nodes (DAT-629/DAT-636).

The substrate for the single **authoring pass** that ``metrics_phase`` runs
(DAT-636): dedup every step across all metric graphs to its global cache key,
topologically order the result, and author each unique node EXACTLY once. The
per-metric path then *assembles* from the resulting binding map and never
re-authors — so a sub-node shared by several metrics (e.g. the
``cost_of_goods_sold`` extract) is decided a single time and can no longer
diverge across siblings. (DAT-629 first cached shared successes; DAT-636 closes
the gap where a node that failed/abstained was uncached and re-authored per
metric.)

This module is the **pure** layer — no execution, fully unit-testable:

* :func:`build_warm_dag` / :func:`warming_generations` — the dedup'd DAG and its
  topological generations;
* :func:`build_mini_graph` — the single-output graph the authoring pass runs per
  node;
* :class:`NodeDecision` — one node's run-scoped authoring outcome (the binding
  map value); the pass itself lives in ``metrics_phase``.

The node key mirrors the snippet cache key *exactly* (extract → standard_field /
statement / aggregation; constant → parameter / value; formula → normalized
expression), so authoring mints precisely what the per-metric assembly's
``_lookup_snippets`` later finds.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING

import networkx as nx

from dataraum.graphs.models import StepType
from dataraum.query.snippet_utils import normalize_expression

if TYPE_CHECKING:
    from dataraum.graphs.models import GraphStep, TransformationGraph

# A node's global identity — a tuple mirroring the snippet cache key. Hashable,
# so it doubles as the networkx node id.
NodeKey = tuple[str | None, ...]


@dataclass(frozen=True)
class WarmNode:
    """A unique cache-keyed node in the cross-metric DAG.

    ``key`` is the global dedup identity (mirrors the snippet cache key, so
    warming this node mints exactly what a later per-metric lookup finds).
    ``graph`` / ``step`` are the *representative* occurrence — the first one
    seen — used to build the warming mini-graph. The snippet is concept-keyed,
    so which representative we pick does not affect later lookups.
    """

    key: NodeKey
    graph: TransformationGraph
    step: GraphStep


@dataclass(frozen=True)
class NodeDecision:
    """The authoring pass's decision for one node (DAT-636).

    Run-scoped and in-memory: each unique node is authored EXACTLY once per run
    and its outcome recorded here. ``grounded`` means the node's concept-keyed
    snippet was minted (its SQL lives in the cross-run cache); a not-grounded
    decision carries the born-loud ``reason``. The per-metric assembly reads this
    map and NEVER re-authors — a metric whose dependency is not grounded
    honest-fails immediately, with no LLM call. Negatives stay in this map only
    (not persisted), so a later run with different context can re-decide.
    """

    grounded: bool
    reason: str | None = None


def _resolve_constant_value(step: GraphStep, graph: TransformationGraph) -> str | None:
    """Resolve a constant step's parameter default to its string cache value.

    INVARIANT (DAT-636): a constant's resolved value is part of its ``NodeKey``,
    and the authoring pass keys off the *representative* graph while the per-metric
    assembly keys off *its own* graph. So two graphs that share a parameter name
    MUST agree on its default, or the same concept gets two keys and the assembly
    lookup misses (→ honest-fail "not authored"). Vertical configs use globally
    consistent defaults today; a divergent default is a config error, surfaced
    born-loud rather than silently mis-grounded.
    """
    if not step.parameter:
        return None
    for param in graph.parameters:
        if param.name == step.parameter:
            return str(param.default) if param.default is not None else None
    return None


def node_key(step: GraphStep, graph: TransformationGraph) -> NodeKey | None:
    """Global dedup key for a step, mirroring the snippet cache key.

    Returns ``None`` for a step that cannot be cache-keyed (an extract with no
    source, a formula with no expression) — such a step is never warmed; it
    falls through to per-metric authoring as before.
    """
    if step.step_type == StepType.EXTRACT:
        if not step.source:
            return None
        return ("extract", step.source.standard_field, step.source.statement, step.aggregation)
    if step.step_type == StepType.CONSTANT:
        # Mirror _save_snippets/_lookup_snippets: keyed by parameter name (or the
        # local step_id when there is no parameter) + the resolved value. The
        # step_id fallback is graph-local, so two graphs sharing a step_id but
        # different values could in principle collide — that's the EXISTING cache
        # keying (we mirror it exactly so warming mints what lookup finds); it
        # does not arise in practice (parameterized constants carry a parameter).
        return ("constant", step.parameter or step.step_id, _resolve_constant_value(step, graph))
    if step.step_type == StepType.FORMULA:
        if not step.expression:
            return None
        normalized, _, _ = normalize_expression(step.expression)
        return ("formula", normalized)
    return None


def build_warm_dag(
    graphs: dict[str, TransformationGraph],
) -> tuple[nx.DiGraph, dict[NodeKey, WarmNode]]:
    """Build the cross-metric DAG of unique cache-keyed nodes.

    Every cache-keyable step across all graphs collapses to one node by its
    global key; edges follow ``depends_on`` (resolved through each graph's local
    step ids). A cycle is a malformed metric set — raised, not tolerated.

    Returns the ``networkx`` DiGraph (node ids are :data:`NodeKey` tuples) and a
    map from key to its representative :class:`WarmNode`.
    """
    dag: nx.DiGraph = nx.DiGraph()
    nodes: dict[NodeKey, WarmNode] = {}
    # Per-graph: local step_id -> global node key, to resolve depends_on edges.
    local_to_key: dict[str, dict[str, NodeKey]] = {}

    for graph_id, graph in graphs.items():
        per_graph: dict[str, NodeKey] = {}
        local_to_key[graph_id] = per_graph
        for step_id, step in graph.steps.items():
            key = node_key(step, graph)
            if key is None:
                continue
            per_graph[step_id] = key
            if key not in nodes:
                nodes[key] = WarmNode(key=key, graph=graph, step=step)
                dag.add_node(key)

    for graph_id, graph in graphs.items():
        per_graph = local_to_key[graph_id]
        for step_id, step in graph.steps.items():
            key = per_graph.get(step_id)
            if key is None:
                continue
            for dep_local in step.depends_on:
                dep_key = per_graph.get(dep_local)
                if dep_key is not None and dep_key != key:
                    dag.add_edge(dep_key, key)

    try:
        cycle = nx.find_cycle(dag)
    except nx.NetworkXNoCycle:
        cycle = None
    if cycle:
        raise ValueError(f"metric DAG has a dependency cycle: {cycle}")

    return dag, nodes


def warming_generations(dag: nx.DiGraph) -> list[list[NodeKey]]:
    """Topologically ordered waves: generation N depends only on < N.

    Nodes within a generation are independent — safe to warm concurrently.
    """
    return [list(generation) for generation in nx.topological_generations(dag)]


def ungroundable_dep_reason(node: WarmNode, bindings: dict[NodeKey, NodeDecision]) -> str | None:
    """First dependency of ``node`` that did not ground, or ``None`` if all did.

    Gates formula/composite authoring (DAT-636): a node whose dependency is
    ungroundable must NOT reach the LLM. The composition prompt is told to
    reproduce each dependency step EXACTLY — given an ABSENT dependency it
    fabricates one instead (e.g. a `dio` formula over an ungroundable
    `cost_of_goods_sold` emitted ``SELECT 30 AS value``, copying the
    days_in_period constant), and the save path then persists that fabrication
    as a healthy extract snippet: a cross-run precision landmine. Honest-fail
    the node here instead — symmetric to the per-metric assembly's guard.

    The barrier between warming generations guarantees every dependency is
    already decided in ``bindings`` by the time its dependent node is gated — so
    a dependency that is missing, un-keyable, or not grounded is treated as
    ungroundable (fail loud), never silently passed through (which would hand the
    LLM a dep it can only fabricate).
    """
    graph = node.graph
    for dep_id in node.step.depends_on:
        dep_step = graph.steps.get(dep_id)
        if dep_step is None:
            return f"dependency '{dep_id}' is ungroundable: not defined in the graph"
        dep_key = node_key(dep_step, graph)
        if dep_key is None:
            return f"dependency '{dep_id}' is ungroundable: has no cache key (never authored)"
        decision = bindings.get(dep_key)
        if decision is None or not decision.grounded:
            reason = decision.reason if decision and decision.reason else "not authored"
            return f"dependency '{dep_id}' is ungroundable: {reason}"
    return None


def build_mini_graph(node: WarmNode) -> TransformationGraph:
    """Minimal single-output graph for warming one node.

    The representative step plus its transitive dependency steps (taken from the
    representative graph, original local step ids preserved so ``depends_on``
    resolves), with **only** the warmed node marked as the output step. The
    deps are already warm by the time a later-generation node is warmed, so the
    agent assembles them from cache and only authors this node.

    Steps are **copied** (:func:`dataclasses.replace`) — the originals belong to
    the real metric graphs that execute later in the phase; warming must never
    mutate their ``output_step`` flag.
    """
    graph = node.graph
    needed: dict[str, GraphStep] = {}
    stack = [node.step.step_id]
    while stack:
        step_id = stack.pop()
        if step_id in needed:
            continue
        step = graph.steps.get(step_id)
        if step is None:
            continue
        needed[step_id] = step
        stack.extend(step.depends_on)

    mini_steps = {
        step_id: dataclasses.replace(step, output_step=(step_id == node.step.step_id))
        for step_id, step in needed.items()
    }
    return dataclasses.replace(graph, steps=mini_steps)
