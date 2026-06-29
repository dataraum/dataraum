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


def node_key(step: GraphStep, graph: TransformationGraph) -> NodeKey | None:
    """Cross-metric dedup key for a step — ONLY leaf EXTRACTs warm (DAT-646).

    An EXTRACT is the sole LLM authoring surface and is genuinely shared across
    metrics (e.g. ``revenue``), so it is warmed ONCE, keyed by its concept. A
    FORMULA or CONSTANT is deterministic and metric-specific: it is NOT warmed and
    NOT cross-metric shared — the per-metric ``assemble`` composes it directly from
    the DAG. (Keying a formula by its normalized SHAPE aliased distinct metrics that
    share an arithmetic pattern — e.g. every ``x / revenue * 100`` margin collapsed
    to one snippet, reusing the wrong operand's CTE — DAT-646. The fix is to not
    share formulas at all, not to refine the key.)

    Returns ``None`` for a step that is never warmed: a FORMULA, a CONSTANT, or an
    extract with no source.
    """
    if step.step_type == StepType.EXTRACT:
        if not step.source:
            return None
        return ("extract", step.source.standard_field, step.source.statement, step.aggregation)
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
