"""Entropy context for the readiness rollup — per-column design.

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
    max across intents) loses. ``dimension_path`` + ``label`` make the driver
    self-describing so the cockpit needs no node→label dictionary (the network
    vocabulary stays in the engine).
    """

    node: str = ""
    dimension_path: str = ""
    label: str = ""
    state: str = "low"
    impact_delta: float = 0.0


@dataclass
class IntentReadiness:
    """Risk + readiness band for an intent node, with its ranked drivers."""

    intent_name: str = ""
    risk: float = 0.0
    readiness: str = "ready"
    drivers: list[IntentDriver] = field(default_factory=list)


@dataclass
class ColumnNodeEvidence:
    """One network node's evidence within a specific column."""

    node_name: str = ""
    dimension_path: str = ""
    label: str = ""
    state: str = "low"
    score: float = 0.0
    impact_delta: float = 0.0  # causal impact of fixing this node (from priorities)
    evidence: list[dict[str, Any]] = field(default_factory=list)
    detector_id: str = ""


@dataclass
class ColumnReadinessResult:
    """Readiness rollup result for a single column."""

    target: str = ""
    node_evidence: list[ColumnNodeEvidence] = field(default_factory=list)
    intents: list[IntentReadiness] = field(default_factory=list)
    top_priority_node: str = ""
    top_priority_impact: float = 0.0
    nodes_observed: int = 0
    nodes_high: int = 0
    worst_intent_risk: float = 0.0
    readiness: str = "ready"


@dataclass
class EntropyForReadiness:
    """Top-level: per-column results + source-wide summaries.

    Cross-column aggregation (per-intent rollup + top cross-column fix) is NOT
    here — it belongs at dataset scope (DAT-396); the only consumer of the old
    aggregate was the retired MCP formatter.
    """

    columns: dict[str, ColumnReadinessResult] = field(default_factory=dict)
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


def _node_label(node: str) -> str:
    """Human-readable label for a network node (humanized node name).

    Keeps the network vocabulary in the engine so the cockpit can render a
    driver without its own node→label dictionary.
    """
    return node.replace("_", " ").capitalize()


def _object_to_direct_signal(obj: EntropyObject) -> DirectSignal:
    """Convert an unmapped EntropyObject to a DirectSignal."""
    return DirectSignal(
        dimension_path=obj.dimension_path,
        target=obj.target,
        score=obj.score,
        evidence=list(obj.evidence),
        detector_id=obj.detector_id,
    )


def _readiness_from_risk(
    risk: float,
    disc_medium_upper: float = 0.6,
    disc_low_upper: float = 0.3,
) -> str:
    """Determine readiness from an intent's risk score.

    Uses the same thresholds as score discretization:
    - risk > medium_upper -> blocked
    - risk > low_upper -> investigate
    - else -> ready
    """
    if risk > disc_medium_upper:
        return "blocked"
    if risk > disc_low_upper:
        return "investigate"
    return "ready"


def _build_column_result(
    target: str,
    objects: list[EntropyObject],
    network: EntropyNetwork,
    path_map: dict[str, str],
) -> tuple[ColumnReadinessResult | None, list[DirectSignal]]:
    """Run network inference for a single column's objects.

    Args:
        target: Column target string (e.g. "column:table.col").
        objects: EntropyObjects for this column only.
        network: The entropy network.
        path_map: Pre-built dimension_path -> node_name map.

    Returns:
        Tuple of (ColumnReadinessResult or None, list of DirectSignals).
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
            label=_node_label(node_name),
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

        intent_risk = risk[intent_name]
        readiness = _readiness_from_risk(intent_risk, disc.medium_upper, disc.low_upper)
        # Per-intent drivers: nodes that lower THIS intent's risk, ranked by how
        # much. ``affected_intents`` carries the per-intent split that the
        # collapsed ColumnNodeEvidence.impact_delta (a max across intents) drops.
        # Each driver carries dimension_path + label so the payload is
        # self-describing (the cockpit needs no node vocabulary).
        drivers = [
            IntentDriver(
                node=pr.node,
                dimension_path=network.get_node_config(pr.node).dimension_path,
                label=_node_label(pr.node),
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
                risk=intent_risk,
                readiness=readiness,
                drivers=drivers,
            )
        )

    # Summary stats
    nodes_observed = len(scores)
    nodes_high = sum(1 for s in states.values() if s == "high")
    worst_intent_risk = max((i.risk for i in intents), default=0.0)
    readiness = _readiness_from_risk(
        worst_intent_risk,
        disc.medium_upper,
        disc.low_upper,
    )

    # Top priority node
    top_priority_node = ""
    top_priority_impact = 0.0
    if priorities:
        top_priority_node = priorities[0].node
        top_priority_impact = priorities[0].impact_delta

    return ColumnReadinessResult(
        target=target,
        node_evidence=node_evidence,
        intents=intents,
        top_priority_node=top_priority_node,
        top_priority_impact=top_priority_impact,
        nodes_observed=nodes_observed,
        nodes_high=nodes_high,
        worst_intent_risk=worst_intent_risk,
        readiness=readiness,
    ), direct_signals


# ---------------------------------------------------------------------------
# Core assembly (pure logic, no DB)
# ---------------------------------------------------------------------------


def assemble_readiness_context(
    objects: list[EntropyObject],
    network: EntropyNetwork,
) -> EntropyForReadiness:
    """Assemble readiness context from entropy objects and network.

    Rolls scores up the entropy network independently per column target. Per-column
    results + source-wide summary stats only; cross-column aggregation is DAT-396.

    Args:
        objects: All EntropyObject instances for the tables being analyzed.
        network: The entropy network.

    Returns:
        EntropyForReadiness with per-column results + source-wide summaries.
    """
    if not objects:
        return EntropyForReadiness()

    # Step 1: Build path map once
    path_map = build_dimension_path_to_node_map(network)

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
    columns: dict[str, ColumnReadinessResult] = {}
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

    # Step 6: Summary stats
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

    return EntropyForReadiness(
        columns=columns,
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


def build_for_readiness(
    session: Session,
    table_ids: list[str],
) -> EntropyForReadiness:
    """Build entropy context for the readiness view.

    Loads entropy data for typed tables and assembles the readiness context
    joining rollup results with source evidence.

    Args:
        session: SQLAlchemy session.
        table_ids: List of table IDs to include.

    Returns:
        EntropyForReadiness with computed context.
    """
    if not table_ids:
        return EntropyForReadiness()

    repo = EntropyRepository(session)

    typed_table_ids = repo.get_typed_table_ids(table_ids)
    if not typed_table_ids:
        logger.warning("No typed tables found for readiness context")
        return EntropyForReadiness()

    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)
    if not entropy_objects:
        logger.debug("No entropy objects found for readiness context")
        return EntropyForReadiness()

    network = EntropyNetwork()
    return assemble_readiness_context(entropy_objects, network)
