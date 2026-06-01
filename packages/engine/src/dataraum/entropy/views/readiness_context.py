"""Entropy context for the readiness rollup — per-column design.

Rolls detector scores up the entropy network independently for each column
target, then aggregates intent readiness and cross-column fix priorities.

Follows the build_for_* pattern from graph_context.py and query_context.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.core.storage import EntropyRepository
from dataraum.entropy.db_models import EntropyReadinessRecord
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.network.bridge import (
    build_dimension_path_to_node_map,
    discretize_score,
    entropy_objects_to_scores,
)
from dataraum.entropy.network.model import EntropyNetwork
from dataraum.entropy.network.rollup import compute_priorities, roll_up
from dataraum.storage import Column, Table

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
    *,
    compute_rollup: bool = True,
) -> tuple[ColumnReadinessResult | None, list[DirectSignal]]:
    """Run network inference for a single column's objects.

    Args:
        target: Column target string (e.g. "column:table.col").
        objects: EntropyObjects for this column only.
        network: The entropy network.
        path_map: Pre-built dimension_path -> node_name map.
        compute_rollup: Run the noisy-OR rollup (intents + causal priorities).
            When False, only the raw per-node evidence + direct signals are
            built — the cheap half the contract gate needs at query time, which
            never went through the rollup. The persisted ``entropy_readiness``
            rows are the source of truth for the banded result, so the rollup
            runs only at the terminal ``detect`` step (DAT-399 slice D).

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

    # Build lookup: node_name -> source object (for raw score + evidence).
    node_to_obj: dict[str, EntropyObject] = {}
    for obj in mapped:
        node_name = path_map.get(obj.dimension_path)
        if node_name:
            node_to_obj[node_name] = obj

    risk: dict[str, float] = {}
    priorities: list[Any] = []
    node_to_delta: dict[str, float] = {}
    if compute_rollup:
        # Roll observed scores up the DAG. Unobserved nodes with no resolved
        # parents are simply absent — no prior leakage, so no subgraph pruning.
        risk = roll_up(network.config, scores, _pmap=network.parent_map, _order=network.topo_order)
        # Causal fix priorities: how much each observed node lowers intent risk.
        priorities = compute_priorities(network.config, scores, low_upper=disc.low_upper)
        node_to_delta = {pr.node: pr.impact_delta for pr in priorities}

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
    *,
    compute_rollup: bool = True,
) -> EntropyForReadiness:
    """Assemble readiness context from entropy objects and network.

    Rolls scores up the entropy network independently per column target. Per-column
    results + source-wide summary stats only; cross-column aggregation is DAT-396.

    Args:
        objects: All EntropyObject instances for the tables being analyzed.
        network: The entropy network.
        compute_rollup: Run the noisy-OR rollup. When False, per-column results
            carry only raw node evidence (no intents/bands) — the cheap evidence
            half for the query-time contract gate (DAT-399 slice D).

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
            compute_rollup=compute_rollup,
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


def _load_entropy_objects(session: Session, table_ids: list[str]) -> list[EntropyObject]:
    """Load entropy objects for the typed tables among ``table_ids`` (or empty)."""
    if not table_ids:
        return []

    repo = EntropyRepository(session)
    typed_table_ids = repo.get_typed_table_ids(table_ids)
    if not typed_table_ids:
        logger.warning("No typed tables found for readiness context")
        return []

    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)
    if not entropy_objects:
        logger.debug("No entropy objects found for readiness context")
    return entropy_objects


def build_for_readiness(
    session: Session,
    table_ids: list[str],
) -> EntropyForReadiness:
    """Build the full readiness rollup (intents + bands) for typed tables.

    Runs the noisy-OR rollup. This is the terminal ``detect`` step's computation
    (persisted to ``entropy_readiness``); query-time consumers read the persisted
    band instead and use :func:`build_column_evidence` for the contract gate.

    Args:
        session: SQLAlchemy session.
        table_ids: List of table IDs to include.

    Returns:
        EntropyForReadiness with computed context.
    """
    entropy_objects = _load_entropy_objects(session, table_ids)
    if not entropy_objects:
        return EntropyForReadiness()
    return assemble_readiness_context(entropy_objects, EntropyNetwork())


def build_column_evidence(
    session: Session,
    table_ids: list[str],
) -> EntropyForReadiness:
    """Build raw per-column entropy evidence WITHOUT the noisy-OR rollup.

    The contract gate (``query_context.network_to_column_summaries``) only reads
    raw per-node scores + direct signals, never the rollup. This loads exactly
    that — the cheap half — so the rollup is computed once, at ``detect``, not
    re-run per query (DAT-399 slice D). ``avg_entropy_score`` is raw-derived and
    so still populated; intents/bands are intentionally empty.

    Args:
        session: SQLAlchemy session.
        table_ids: List of table IDs to include.

    Returns:
        EntropyForReadiness with per-column node evidence + direct signals only.
    """
    entropy_objects = _load_entropy_objects(session, table_ids)
    if not entropy_objects:
        return EntropyForReadiness()
    return assemble_readiness_context(entropy_objects, EntropyNetwork(), compute_rollup=False)


def load_persisted_readiness(
    session: Session,
    table_ids: list[str],
) -> EntropyForReadiness:
    """Reconstruct the banded readiness view from persisted ``entropy_readiness``.

    The single source of truth for the band/intents/drivers (DAT-399 slice D):
    the terminal ``detect`` step ran the rollup once and persisted it, so query
    time reads those rows rather than recomputing. Reconstructs the same
    ``EntropyForReadiness`` shape the band consumers (graph context, query
    counts) already read, keyed by ``column:{table}.{column}`` target.

    Per-node raw ``score`` is not persisted (the contract gate uses
    :func:`build_column_evidence` for that); reconstructed ``node_evidence``
    carries only the non-clean driver nodes, which is exactly what the band
    consumers read (``high_entropy_dimensions``).

    Precondition: the terminal detect step has run. Query time always follows
    detect in the workflow, so empty here means "no readiness yet" and is
    treated as ready — same as a genuinely clean source.
    """
    if not table_ids:
        return EntropyForReadiness()

    records = list(
        session.execute(
            select(EntropyReadinessRecord).where(EntropyReadinessRecord.table_id.in_(table_ids))
        ).scalars()
    )
    if not records:
        return EntropyForReadiness()

    target_by_ids = _target_by_ids(session, table_ids)

    columns: dict[str, ColumnReadinessResult] = {}
    for rec in records:
        if rec.table_id is None or rec.column_id is None:
            continue
        target = target_by_ids.get((rec.table_id, rec.column_id))
        if target is None:
            # Column row the readiness record points at no longer resolves
            # (renamed/dropped) — skip, don't guess.
            continue
        columns[target] = _record_to_column_result(target, rec)

    columns_blocked = sum(1 for c in columns.values() if c.readiness == "blocked")
    columns_investigate = sum(1 for c in columns.values() if c.readiness == "investigate")
    columns_ready = sum(1 for c in columns.values() if c.readiness == "ready")
    if columns_blocked > 0:
        overall_readiness = "blocked"
    elif columns_investigate > 0:
        overall_readiness = "investigate"
    else:
        overall_readiness = "ready"

    return EntropyForReadiness(
        columns=columns,
        total_columns=len(columns),
        columns_blocked=columns_blocked,
        columns_investigate=columns_investigate,
        columns_ready=columns_ready,
        overall_readiness=overall_readiness,
    )


def _target_by_ids(session: Session, table_ids: list[str]) -> dict[tuple[str, str], str]:
    """Map ``(table_id, column_id)`` -> ``"column:{table_name}.{column_name}"``.

    Inverse of the target string the detectors write (``engine.py`` builds it as
    ``f"column:{table.table_name}.{col.column_name}"``).
    """
    table_name_by_id: dict[str, str] = {}
    for table_id, table_name in session.execute(
        select(Table.table_id, Table.table_name).where(Table.table_id.in_(table_ids))
    ):
        table_name_by_id[table_id] = table_name

    out: dict[tuple[str, str], str] = {}
    for table_id, column_id, column_name in session.execute(
        select(Column.table_id, Column.column_id, Column.column_name).where(
            Column.table_id.in_(table_ids)
        )
    ):
        table_name = table_name_by_id.get(table_id)
        if table_name is None:
            continue
        out[(table_id, column_id)] = f"column:{table_name}.{column_name}"
    return out


def _record_to_column_result(target: str, rec: EntropyReadinessRecord) -> ColumnReadinessResult:
    """Reconstruct a ColumnReadinessResult from a persisted readiness row."""
    intents = [
        IntentReadiness(
            intent_name=i.get("intent", ""),
            risk=i.get("risk", 0.0),
            readiness=i.get("band", "ready"),
            drivers=[_driver_from_dict(d) for d in i.get("drivers", [])],
        )
        for i in (rec.intents or [])
    ]
    # Persisted top_drivers are exactly the non-clean nodes — what the band
    # consumers read as node_evidence (high_entropy_dimensions). Raw per-node
    # ``score`` is NOT persisted, so it stays 0.0 here: this reconstructed result
    # must only feed consumers that don't read ``ne.score`` (the band view). For
    # contract dimension_scores, use build_column_evidence (which has real scores).
    top_drivers = rec.top_drivers or []
    node_evidence = [
        ColumnNodeEvidence(
            node_name=d.get("node", ""),
            dimension_path=d.get("dimension_path", ""),
            label=d.get("label", ""),
            state=d.get("state", "low"),
            impact_delta=d.get("impact_delta", 0.0),
        )
        for d in top_drivers
    ]
    return ColumnReadinessResult(
        target=target,
        node_evidence=node_evidence,
        intents=intents,
        top_priority_node=top_drivers[0].get("node", "") if top_drivers else "",
        top_priority_impact=top_drivers[0].get("impact_delta", 0.0) if top_drivers else 0.0,
        nodes_observed=len(node_evidence),
        nodes_high=sum(1 for ne in node_evidence if ne.state == "high"),
        worst_intent_risk=rec.worst_intent_risk,
        readiness=rec.band,
    )


def _driver_from_dict(d: dict[str, Any]) -> IntentDriver:
    """Reconstruct an IntentDriver from a persisted driver dict."""
    return IntentDriver(
        node=d.get("node", ""),
        dimension_path=d.get("dimension_path", ""),
        label=d.get("label", ""),
        state=d.get("state", "low"),
        impact_delta=d.get("impact_delta", 0.0),
    )
