"""Entropy context for the network rollup — per-column design.

Rolls detector scores up the entropy network independently for each column
target, then aggregates intent readiness and cross-column fix priorities.

Follows the build_for_* pattern from graph_context.py and query_context.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.core.storage import EntropyRepository
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.network.bridge import (
    build_dimension_path_to_node_map,
    discretize_score,
    entropy_objects_to_scores,
)
from dataraum.entropy.network.model import EntropyNetwork
from dataraum.entropy.network.rollup import compute_priorities, roll_up

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class DirectSignal:
    """Entropy signal not mapped to any network node."""

    dimension_path: str = ""
    target: str = ""
    score: float = 0.0
    evidence: list[dict[str, Any]] = field(default_factory=list)
    detector_id: str = ""


@dataclass
class IntentDriver:
    """One observed node's causal contribution to a specific intent's risk.

    ``impact_delta`` is how much THIS intent's risk would drop if the node were
    fixed to clean (from ``compute_priorities``' per-intent ``affected_intents``)
    — the per-intent split the collapsed ``ColumnNodeEvidence.impact_delta`` (a
    max across intents) loses.
    """

    node: str = ""
    state: str = "low"
    impact_delta: float = 0.0


@dataclass
class IntentReadiness:
    """Posterior and readiness for an intent node, with its ranked drivers."""

    intent_name: str = ""
    posterior: dict[str, float] = field(default_factory=dict)
    dominant_state: str = "low"
    p_high: float = 0.0
    readiness: str = "ready"
    drivers: list[IntentDriver] = field(default_factory=list)


@dataclass
class ColumnNodeEvidence:
    """One network node's evidence within a specific column."""

    node_name: str = ""
    dimension_path: str = ""
    state: str = "low"
    score: float = 0.0
    impact_delta: float = 0.0  # causal impact of fixing this node (from priorities)
    evidence: list[dict[str, Any]] = field(default_factory=list)
    detector_id: str = ""


@dataclass
class ColumnNetworkResult:
    """Network inference result for a single column."""

    target: str = ""
    node_evidence: list[ColumnNodeEvidence] = field(default_factory=list)
    intents: list[IntentReadiness] = field(default_factory=list)
    top_priority_node: str = ""
    top_priority_impact: float = 0.0
    nodes_observed: int = 0
    nodes_high: int = 0
    worst_intent_p_high: float = 0.0
    readiness: str = "ready"

    def needs_attention(self, p_high_threshold: float = 0.35) -> bool:
        """Whether this column needs further analysis based on network inference.

        A column needs attention if readiness is "investigate" or "blocked",
        or if any intent P(high) exceeds the threshold.
        """
        return (
            self.readiness in ("investigate", "blocked")
            or self.worst_intent_p_high > p_high_threshold
        )


@dataclass
class AggregateIntentReadiness:
    """Cross-column aggregation of one intent."""

    intent_name: str = ""
    worst_p_high: float = 0.0
    mean_p_high: float = 0.0
    columns_blocked: int = 0
    columns_investigate: int = 0
    columns_ready: int = 0
    overall_readiness: str = "ready"


@dataclass
class CrossColumnFix:
    """Which node, if fixed everywhere, helps the most columns."""

    node_name: str = ""
    dimension_path: str = ""
    columns_affected: int = 0
    total_intent_delta: float = 0.0
    example_columns: list[str] = field(default_factory=list)


@dataclass
class EntropyForNetwork:
    """Top-level: per-column results + aggregated summaries."""

    columns: dict[str, ColumnNetworkResult] = field(default_factory=dict)
    intents: list[AggregateIntentReadiness] = field(default_factory=list)
    top_fix: CrossColumnFix | None = None
    direct_signals: list[DirectSignal] = field(default_factory=list)
    total_columns: int = 0
    columns_blocked: int = 0
    columns_investigate: int = 0
    columns_ready: int = 0
    total_direct_signals: int = 0
    overall_readiness: str = "ready"
    avg_entropy_score: float = 0.0
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _object_to_direct_signal(obj: EntropyObject) -> DirectSignal:
    """Convert an unmapped EntropyObject to a DirectSignal."""
    return DirectSignal(
        dimension_path=obj.dimension_path,
        target=obj.target,
        score=obj.score,
        evidence=list(obj.evidence),
        detector_id=obj.detector_id,
    )


def _readiness_from_p_high(
    p_high: float,
    disc_medium_upper: float = 0.6,
    disc_low_upper: float = 0.3,
) -> str:
    """Determine readiness from P(intent=high).

    Uses the same thresholds as score discretization:
    - P(high) > medium_upper -> blocked
    - P(high) > low_upper -> investigate
    - else -> ready
    """
    if p_high > disc_medium_upper:
        return "blocked"
    if p_high > disc_low_upper:
        return "investigate"
    return "ready"


def _build_column_result(
    target: str,
    objects: list[EntropyObject],
    network: EntropyNetwork,
    path_map: dict[str, str],
) -> tuple[ColumnNetworkResult | None, list[DirectSignal]]:
    """Run network inference for a single column's objects.

    Args:
        target: Column target string (e.g. "column:table.col").
        objects: EntropyObjects for this column only.
        network: The entropy network.
        path_map: Pre-built dimension_path -> node_name map.

    Returns:
        Tuple of (ColumnNetworkResult or None, list of DirectSignals).
        Returns None for the result if no objects map to network nodes.
    """
    disc = network.config.discretization

    # Split into mapped vs unmapped
    mapped: list[EntropyObject] = []
    direct_signals: list[DirectSignal] = []

    for obj in objects:
        if obj.dimension_path in path_map:
            mapped.append(obj)
        else:
            direct_signals.append(_object_to_direct_signal(obj))

    if not mapped:
        return None, direct_signals

    # Per-column: no collisions within a target, safe to use bridge directly.
    # The rollup consumes raw scores; states are derived only for display.
    scores = entropy_objects_to_scores(mapped, network)
    if not scores:
        return None, direct_signals

    states = {
        node: discretize_score(score, disc.low_upper, disc.medium_upper)
        for node, score in scores.items()
    }

    # Roll observed scores up the DAG. Unobserved nodes with no resolved parents
    # are simply absent — no prior leakage, so no subgraph pruning is needed.
    risk = roll_up(network.config, scores, _pmap=network.parent_map, _order=network.topo_order)

    # Causal fix priorities: how much each observed node lowers intent risk.
    priorities = compute_priorities(network.config, scores, low_upper=disc.low_upper)

    # Build ColumnNodeEvidence for each observed node
    # Build lookups: node_name -> source object, node_name -> impact_delta
    node_to_obj: dict[str, EntropyObject] = {}
    for obj in mapped:
        node_name = path_map.get(obj.dimension_path)
        if node_name:
            node_to_obj[node_name] = obj

    node_to_delta: dict[str, float] = {pr.node: pr.impact_delta for pr in priorities}

    node_evidence: list[ColumnNodeEvidence] = []
    for node_name, state in states.items():
        source_obj = node_to_obj.get(node_name)
        node_ev = ColumnNodeEvidence(
            node_name=node_name,
            dimension_path=network.get_node_config(node_name).dimension_path,
            state=state,
            score=source_obj.score if source_obj else 0.0,
            impact_delta=node_to_delta.get(node_name, 0.0),
            evidence=list(source_obj.evidence) if source_obj else [],
            detector_id=source_obj.detector_id if source_obj else "",
        )
        node_evidence.append(node_ev)

    # Build per-column IntentReadiness. Intents with no resolved parents from the
    # observed evidence are absent from `risk` and skipped (same as before).
    intents: list[IntentReadiness] = []

    for intent_name in network.get_intent_nodes():
        if intent_name not in risk:
            continue

        p_high = risk[intent_name]
        readiness = _readiness_from_p_high(p_high, disc.medium_upper, disc.low_upper)
        # Risk is a single value, not a distribution; surface it as P(high).
        posterior = {"high": round(p_high, 4)}
        dominant = "high" if p_high > disc.medium_upper else "low"
        # Per-intent drivers: nodes that lower THIS intent's risk, ranked by how
        # much. ``affected_intents`` carries the per-intent split that the
        # collapsed ColumnNodeEvidence.impact_delta (a max across intents) drops.
        drivers = [
            IntentDriver(
                node=pr.node,
                state=pr.current_state,
                impact_delta=pr.affected_intents[intent_name],
            )
            for pr in priorities
            if intent_name in pr.affected_intents
        ]
        drivers.sort(key=lambda d: d.impact_delta, reverse=True)
        intents.append(
            IntentReadiness(
                intent_name=intent_name,
                posterior=posterior,
                dominant_state=dominant,
                p_high=p_high,
                readiness=readiness,
                drivers=drivers,
            )
        )

    # Summary stats
    nodes_observed = len(scores)
    nodes_high = sum(1 for s in states.values() if s == "high")
    worst_intent_p_high = max((i.p_high for i in intents), default=0.0)
    readiness = _readiness_from_p_high(
        worst_intent_p_high,
        disc.medium_upper,
        disc.low_upper,
    )

    # Top priority node
    top_priority_node = ""
    top_priority_impact = 0.0
    if priorities:
        top_priority_node = priorities[0].node
        top_priority_impact = priorities[0].impact_delta

    return ColumnNetworkResult(
        target=target,
        node_evidence=node_evidence,
        intents=intents,
        top_priority_node=top_priority_node,
        top_priority_impact=top_priority_impact,
        nodes_observed=nodes_observed,
        nodes_high=nodes_high,
        worst_intent_p_high=worst_intent_p_high,
        readiness=readiness,
    ), direct_signals


def _aggregate_intents(
    columns: dict[str, ColumnNetworkResult],
    disc_medium_upper: float = 0.6,
    disc_low_upper: float = 0.3,
) -> list[AggregateIntentReadiness]:
    """Aggregate intent readiness across all columns.

    Args:
        columns: Per-column network results keyed by target.
        disc_medium_upper: Threshold for blocked.
        disc_low_upper: Threshold for investigate.

    Returns:
        List of AggregateIntentReadiness, one per intent.
    """
    # Collect per-intent p_high and readiness from all columns
    intent_data: dict[str, list[tuple[float, str]]] = {}

    for col_result in columns.values():
        for intent in col_result.intents:
            intent_data.setdefault(intent.intent_name, []).append((intent.p_high, intent.readiness))

    aggregates: list[AggregateIntentReadiness] = []
    for intent_name, entries in intent_data.items():
        p_highs = [e[0] for e in entries]
        readinesses = [e[1] for e in entries]

        worst_p_high = max(p_highs)
        mean_p_high = sum(p_highs) / len(p_highs)
        columns_blocked = readinesses.count("blocked")
        columns_investigate = readinesses.count("investigate")
        columns_ready = readinesses.count("ready")
        overall_readiness = _readiness_from_p_high(
            worst_p_high,
            disc_medium_upper,
            disc_low_upper,
        )

        aggregates.append(
            AggregateIntentReadiness(
                intent_name=intent_name,
                worst_p_high=worst_p_high,
                mean_p_high=mean_p_high,
                columns_blocked=columns_blocked,
                columns_investigate=columns_investigate,
                columns_ready=columns_ready,
                overall_readiness=overall_readiness,
            )
        )

    return aggregates


def _compute_cross_column_fix(
    columns: dict[str, ColumnNetworkResult],
    network: EntropyNetwork,
) -> CrossColumnFix | None:
    """Find the node that, if fixed everywhere, helps the most columns.

    For each network node: count columns where it is non-low,
    sum per-column impact_delta from priorities. Pick the node with
    the highest total_intent_delta.

    Args:
        columns: Per-column network results.
        network: The entropy network (for node config).

    Returns:
        CrossColumnFix or None if no non-low nodes exist.
    """
    node_stats: dict[str, dict[str, Any]] = {}

    for target, col_result in columns.items():
        for node_ev in col_result.node_evidence:
            if node_ev.state == "low":
                continue

            if node_ev.node_name not in node_stats:
                node_stats[node_ev.node_name] = {
                    "columns_affected": 0,
                    "total_delta": 0.0,
                    "worst_columns": [],  # (impact_delta, target)
                    "dimension_path": node_ev.dimension_path,
                }

            stats = node_stats[node_ev.node_name]
            stats["columns_affected"] += 1
            # Use the node's actual causal impact from network priorities
            stats["total_delta"] += node_ev.impact_delta
            stats["worst_columns"].append((node_ev.impact_delta, target))

    if not node_stats:
        return None

    # Pick node with highest total_delta
    best_node = max(node_stats, key=lambda n: node_stats[n]["total_delta"])
    stats = node_stats[best_node]

    # Sort worst columns by p_high descending, take top 3
    worst_sorted = sorted(stats["worst_columns"], key=lambda x: x[0], reverse=True)
    example_columns = [t for _, t in worst_sorted[:3]]

    return CrossColumnFix(
        node_name=best_node,
        dimension_path=stats["dimension_path"],
        columns_affected=stats["columns_affected"],
        total_intent_delta=round(stats["total_delta"], 4),
        example_columns=example_columns,
    )


# ---------------------------------------------------------------------------
# Core assembly (pure logic, no DB)
# ---------------------------------------------------------------------------


def assemble_network_context(
    objects: list[EntropyObject],
    network: EntropyNetwork,
) -> EntropyForNetwork:
    """Assemble network context from entropy objects and network.

    Rolls scores up the entropy network independently per column target,
    then aggregates across columns.

    Args:
        objects: All EntropyObject instances for the tables being analyzed.
        network: The entropy network.

    Returns:
        EntropyForNetwork with per-column results and aggregated summaries.
    """
    if not objects:
        return EntropyForNetwork()

    # Step 1: Build path map once
    path_map = build_dimension_path_to_node_map(network)
    disc = network.config.discretization

    # Step 2: Group objects by target
    by_target: dict[str, list[EntropyObject]] = {}
    for obj in objects:
        by_target.setdefault(obj.target, []).append(obj)

    # Step 3: Separate column targets from table targets
    column_targets: dict[str, list[EntropyObject]] = {}
    table_targets: dict[str, list[EntropyObject]] = {}
    for target, target_objects in by_target.items():
        if target.startswith("column:"):
            column_targets[target] = target_objects
        else:
            table_targets[target] = target_objects

    # Step 4: Per-column network inference
    columns: dict[str, ColumnNetworkResult] = {}
    all_direct_signals: list[DirectSignal] = []

    for target, target_objects in column_targets.items():
        col_result, col_signals = _build_column_result(
            target,
            target_objects,
            network,
            path_map,
        )
        all_direct_signals.extend(col_signals)
        if col_result is not None:
            columns[target] = col_result

    # Step 5: Table targets -> all objects become DirectSignal
    for _target, target_objects in table_targets.items():
        for obj in target_objects:
            all_direct_signals.append(_object_to_direct_signal(obj))

    # Step 5b: Deduplicate direct signals — keep highest score per key
    seen: dict[tuple[str, str, str], DirectSignal] = {}
    for ds in all_direct_signals:
        key = (ds.dimension_path, ds.target, ds.detector_id)
        existing = seen.get(key)
        if existing is None or ds.score > existing.score:
            seen[key] = ds
    all_direct_signals = list(seen.values())

    # Step 6: Aggregate intents across columns
    agg_intents = _aggregate_intents(columns, disc.medium_upper, disc.low_upper)

    # Step 7: Cross-column fix
    top_fix = _compute_cross_column_fix(columns, network)

    # Step 8: Summary stats
    total_columns = len(columns)
    columns_blocked = sum(1 for c in columns.values() if c.readiness == "blocked")
    columns_investigate = sum(1 for c in columns.values() if c.readiness == "investigate")
    columns_ready = sum(1 for c in columns.values() if c.readiness == "ready")

    # Overall readiness derived from per-column readiness (which uses
    # dynamic subgraphs to avoid prior leakage from unobserved nodes).
    if columns_blocked > 0:
        overall_readiness = "blocked"
    elif columns_investigate > 0:
        overall_readiness = "investigate"
    else:
        overall_readiness = "ready"

    # Average entropy: per-target max score, then mean across targets.
    target_max: dict[str, float] = {}
    for obj in objects:
        if obj.target not in target_max or obj.score > target_max[obj.target]:
            target_max[obj.target] = obj.score
    avg_entropy_score = sum(target_max.values()) / len(target_max) if target_max else 0.0

    return EntropyForNetwork(
        columns=columns,
        intents=agg_intents,
        top_fix=top_fix,
        direct_signals=all_direct_signals,
        total_columns=total_columns,
        columns_blocked=columns_blocked,
        columns_investigate=columns_investigate,
        columns_ready=columns_ready,
        total_direct_signals=len(all_direct_signals),
        overall_readiness=overall_readiness,
        avg_entropy_score=avg_entropy_score,
    )


# ---------------------------------------------------------------------------
# DB wrapper (follows build_for_* pattern)
# ---------------------------------------------------------------------------


def build_for_network(
    session: Session,
    table_ids: list[str],
) -> EntropyForNetwork:
    """Build entropy context for network inference view.

    Loads entropy data for typed tables and assembles the network context
    joining rollup results with source evidence.

    Args:
        session: SQLAlchemy session.
        table_ids: List of table IDs to include.

    Returns:
        EntropyForNetwork with computed context.
    """
    if not table_ids:
        return EntropyForNetwork()

    repo = EntropyRepository(session)

    typed_table_ids = repo.get_typed_table_ids(table_ids)
    if not typed_table_ids:
        logger.warning("No typed tables found for network context")
        return EntropyForNetwork()

    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)
    if not entropy_objects:
        logger.debug("No entropy objects found for network context")
        return EntropyForNetwork()

    network = EntropyNetwork()
    return assemble_network_context(entropy_objects, network)


# ---------------------------------------------------------------------------
# Markdown formatter
# ---------------------------------------------------------------------------


def format_network_context(ctx: EntropyForNetwork) -> dict[str, Any]:
    """Format network context as structured dict for JSON MCP output.

    Args:
        ctx: EntropyForNetwork to format.

    Returns:
        Dict with network analysis data.
    """
    result: dict[str, Any] = {
        "overall_readiness": ctx.overall_readiness.upper(),
        "total_columns": ctx.total_columns,
        "columns_blocked": ctx.columns_blocked,
        "columns_investigate": ctx.columns_investigate,
        "columns_ready": ctx.columns_ready,
        "total_direct_signals": ctx.total_direct_signals,
    }

    if ctx.intents:
        result["intents"] = [
            {
                "intent": ai.intent_name,
                "worst_p_high": round(ai.worst_p_high, 3),
                "mean_p_high": round(ai.mean_p_high, 3),
                "columns_blocked": ai.columns_blocked,
                "columns_investigate": ai.columns_investigate,
                "columns_ready": ai.columns_ready,
            }
            for ai in ctx.intents
        ]

    if ctx.top_fix is not None:
        tf = ctx.top_fix
        top_fix: dict[str, Any] = {
            "node": tf.node_name,
            "columns_affected": tf.columns_affected,
            "total_intent_delta": round(tf.total_intent_delta, 3),
            "example_columns": tf.example_columns,
        }
        result["top_fix"] = top_fix

    # At-risk columns (blocked + investigate), capped at 10
    at_risk = [(target, col) for target, col in ctx.columns.items() if col.readiness != "ready"]
    at_risk.sort(key=lambda x: x[1].worst_intent_p_high, reverse=True)

    if at_risk:
        at_risk_list: list[dict[str, Any]] = []
        for target, col in at_risk[:10]:
            entry: dict[str, Any] = {
                "target": target,
                "readiness": col.readiness,
                "worst_intent_p_high": round(col.worst_intent_p_high, 3),
            }
            high_nodes = sorted(
                [ne for ne in col.node_evidence if ne.state != "low"],
                key=lambda ne: ne.impact_delta,
                reverse=True,
            )
            if high_nodes:
                entry["high_nodes"] = [
                    {
                        "node": ne.node_name,
                        "state": ne.state,
                        "impact_delta": round(ne.impact_delta, 3),
                    }
                    for ne in high_nodes
                ]
            at_risk_list.append(entry)
        result["at_risk_columns"] = at_risk_list
        if len(at_risk) > 10:
            result["at_risk_total"] = len(at_risk)

    if ctx.direct_signals:
        result["direct_signals"] = [
            {
                "dimension_path": ds.dimension_path,
                "score": round(ds.score, 2),
                "target": ds.target,
                "evidence": ds.evidence[:1] if ds.evidence else [],
            }
            for ds in ctx.direct_signals
        ]

    return result
