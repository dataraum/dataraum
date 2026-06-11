"""Entropy context for the readiness rollup — per-column design.

Rolls detector scores up the LOSS table (entropy/loss.yaml) independently for each
column target: per-intent risk = clamp01(Σ weight·value), banded ready/investigate/
blocked. No network DAG — the per-intent loss weights ARE the rollup (DAT-442).

Follows the build_for_* pattern from graph_context.py and query_context.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.core.storage import EntropyRepository
from dataraum.entropy.db_models import EntropyReadinessRecord
from dataraum.entropy.loss import (
    LossConfig,
    compute_loss_risk,
    get_loss_config,
    loss_risk_for_object,
)
from dataraum.entropy.models import EntropyObject
from dataraum.storage import Column, Table
from dataraum.storage.snapshot_head import head_run_id

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class DirectSignal:
    """Entropy signal with no loss measurement (informative context, not a band driver)."""

    dimension_path: str = ""
    target: str = ""
    score: float = 0.0
    evidence: list[dict[str, Any]] = field(default_factory=list)
    detector_id: str = ""


@dataclass
class IntentDriver:
    """One measurement's contribution to a specific intent's risk.

    ``impact_delta`` is this measurement's per-intent loss risk
    (``clamp01(Σ weight·value)`` for the intent). The worst measurement sets the
    band, so the top driver's delta equals the intent risk. ``dimension_path`` +
    ``label`` make the driver self-describing so the cockpit needs no
    detector→label dictionary.
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
    """One measurement's evidence within a specific column."""

    node_name: str = ""
    dimension_path: str = ""
    label: str = ""
    state: str = "low"
    score: float = 0.0
    impact_delta: float = 0.0  # worst per-intent loss this measurement contributes
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
    """Human-readable label for a measurement (humanized detector_id).

    Keeps the vocabulary in the engine so the cockpit can render a driver
    without its own detector→label dictionary.
    """
    return node.replace("_", " ").capitalize()


def _state_from_score(score: float, low_upper: float, medium_upper: float) -> str:
    """Driver STATE bucket (low/medium/high) from a raw measurement score.

    Distinct vocabulary from the readiness band (ready/investigate/blocked); same
    0.3/0.6 edges. ``state`` reflects the raw measurement severity, the band the
    per-intent expected loss.
    """
    if score > medium_upper:
        return "high"
    if score > low_upper:
        return "medium"
    return "low"


def _object_to_direct_signal(obj: EntropyObject) -> DirectSignal:
    """Convert an unmapped EntropyObject to a DirectSignal."""
    return DirectSignal(
        dimension_path=obj.dimension_path,
        target=obj.target,
        score=obj.score,
        evidence=list(obj.evidence),
        detector_id=obj.detector_id,
    )


def _build_column_result(
    target: str,
    objects: list[EntropyObject],
    loss_config: LossConfig,
    *,
    compute_rollup: bool = True,
) -> tuple[ColumnReadinessResult | None, list[DirectSignal]]:
    """Roll a single target's objects up the LOSS table into per-intent readiness.

    Every tunable measurement (a detector with a ``loss.yaml`` row) contributes
    ``risk(intent) = clamp01(Σ weight·value)``; the column's per-intent risk is the
    worst measurement (max). Detectors with no loss row (informative signals like
    benford) fall through to direct signals — context, never a band driver.

    Args:
        target: Target string (e.g. "column:table.col", "relationship:..", "table:..").
        objects: EntropyObjects for this target only.
        loss_config: The loss table (weights + bands).
        compute_rollup: Build the per-intent bands + drivers. When False only the
            raw per-measurement node evidence + direct signals are built — the cheap
            half the query-time contract gate reads (DAT-399 slice D). The persisted
            ``entropy_readiness`` rows are the source of truth for the banded result.

    Returns:
        Tuple of (ColumnReadinessResult or None, list of DirectSignals). None when no
        object maps to a loss measurement (nothing to band).
    """
    low_upper = loss_config.readiness_bands["low_upper"]
    medium_upper = loss_config.readiness_bands["medium_upper"]

    loss_objects: list[EntropyObject] = []
    direct_signals: list[DirectSignal] = []
    for obj in objects:
        if loss_config.is_loss_measurement(obj.detector_id):
            loss_objects.append(obj)
        else:
            direct_signals.append(_object_to_direct_signal(obj))

    if not loss_objects:
        return None, direct_signals

    # Per-measurement node evidence (the raw half — always built). One node per loss
    # object; its impact is the worst per-intent loss it contributes (a rollup product,
    # so 0.0 on the cheap query-time path).
    node_evidence: list[ColumnNodeEvidence] = []
    for obj in loss_objects:
        impact = (
            max(loss_risk_for_object(obj, loss_config).values(), default=0.0)
            if compute_rollup
            else 0.0
        )
        node_evidence.append(
            ColumnNodeEvidence(
                node_name=obj.detector_id,
                dimension_path=obj.dimension_path,
                label=_node_label(obj.detector_id),
                state=_state_from_score(obj.score, low_upper, medium_upper),
                score=obj.score,
                impact_delta=impact,
                evidence=list(obj.evidence),
                detector_id=obj.detector_id,
            )
        )

    nodes_high = sum(1 for ne in node_evidence if ne.state == "high")
    if not compute_rollup:
        # Cheap evidence-only path (query-time contract gate): raw node evidence +
        # direct signals, no intents/bands.
        return ColumnReadinessResult(
            target=target,
            node_evidence=node_evidence,
            nodes_observed=len(node_evidence),
            nodes_high=nodes_high,
        ), direct_signals

    # Per-intent risk = worst measurement (max across objects). Drivers = each
    # measurement's contribution to that intent, ranked; the top one sets the band.
    loss_risk = compute_loss_risk(loss_objects, loss_config)
    intents: list[IntentReadiness] = []
    for intent_name in loss_config.intents():
        if intent_name not in loss_risk:
            continue
        intent_risk = loss_risk[intent_name]
        drivers = [
            IntentDriver(
                node=obj.detector_id,
                dimension_path=obj.dimension_path,
                label=_node_label(obj.detector_id),
                state=_state_from_score(obj.score, low_upper, medium_upper),
                impact_delta=per,
            )
            for obj in loss_objects
            if (per := loss_risk_for_object(obj, loss_config).get(intent_name, 0.0)) > 0.0
        ]
        drivers.sort(key=lambda d: d.impact_delta, reverse=True)
        intents.append(
            IntentReadiness(
                intent_name=intent_name,
                risk=intent_risk,
                readiness=loss_config.band(intent_risk),
                drivers=drivers,
            )
        )

    worst_intent_risk = max((i.risk for i in intents), default=0.0)
    top_node = max(node_evidence, key=lambda ne: ne.impact_delta, default=None)
    return ColumnReadinessResult(
        target=target,
        node_evidence=node_evidence,
        intents=intents,
        top_priority_node=top_node.node_name if top_node else "",
        top_priority_impact=top_node.impact_delta if top_node else 0.0,
        nodes_observed=len(node_evidence),
        nodes_high=nodes_high,
        worst_intent_risk=worst_intent_risk,
        readiness=loss_config.band(worst_intent_risk),
    ), direct_signals


# ---------------------------------------------------------------------------
# Core assembly (pure logic, no DB)
# ---------------------------------------------------------------------------


def assemble_readiness_context(
    objects: list[EntropyObject],
    *,
    compute_rollup: bool = True,
) -> EntropyForReadiness:
    """Assemble readiness context from entropy objects via the loss table.

    Rolls scores up the loss table independently per column target. Per-column
    results + source-wide summary stats only; cross-column aggregation is DAT-396.

    Args:
        objects: All EntropyObject instances for the tables being analyzed.
        compute_rollup: Build the per-intent bands + drivers. When False, per-column
            results carry only raw node evidence (no intents/bands) — the cheap
            evidence half for the query-time contract gate (DAT-399 slice D).

    Returns:
        EntropyForReadiness with per-column results + source-wide summaries.
    """
    if not objects:
        return EntropyForReadiness()

    loss_config = get_loss_config()

    # Step 1: Group objects by target
    by_target: dict[str, list[EntropyObject]] = {}
    for obj in objects:
        by_target.setdefault(obj.target, []).append(obj)

    # Step 2: Targets that roll up the loss table — column, relationship (DAT-408)
    # AND table (DAT-415) granularity — vs the rest (view:), which stay raw
    # DirectSignals. The rollup (``_build_column_result``) is target-agnostic, so
    # a relationship's or a table's objects roll up the same intents as a
    # column's. ``table:`` carries the fact table's dimension_coverage measurement
    # → query/reporting intent bands.
    rollup_targets: dict[str, list[EntropyObject]] = {}
    other_targets: dict[str, list[EntropyObject]] = {}
    for target, target_objects in by_target.items():
        if (
            target.startswith("column:")
            or target.startswith("relationship:")
            or target.startswith("table:")
        ):
            rollup_targets[target] = target_objects
        else:
            other_targets[target] = target_objects

    # Step 3: Per-target loss rollup. ``columns`` is keyed by target string and may
    # hold ``relationship:`` targets alongside ``column:`` ones (DAT-408).
    columns: dict[str, ColumnReadinessResult] = {}
    all_direct_signals: list[DirectSignal] = []

    for target, target_objects in rollup_targets.items():
        col_result, col_signals = _build_column_result(
            target,
            target_objects,
            loss_config,
            compute_rollup=compute_rollup,
        )
        all_direct_signals.extend(col_signals)
        if col_result is not None:
            columns[target] = col_result

    # Step 5: Other targets (table:/view:) -> all objects become DirectSignal
    for _target, target_objects in other_targets.items():
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


def _load_entropy_objects(
    session: Session,
    table_ids: list[str],
    *,
    current_run_id: str | None = None,
    session_id: str | None = None,
) -> list[EntropyObject]:
    """Load entropy objects for the typed tables among ``table_ids`` (or empty)."""
    if not table_ids:
        return []

    repo = EntropyRepository(session)
    typed_table_ids = repo.get_typed_table_ids(table_ids)
    if not typed_table_ids:
        logger.warning("No typed tables found for readiness context")
        return []

    entropy_objects = repo.load_for_tables(
        typed_table_ids,
        enforce_typed=True,
        current_run_id=current_run_id,
        session_id=session_id,
    )
    if not entropy_objects:
        logger.debug("No entropy objects found for readiness context")
    return entropy_objects


def build_for_readiness(
    session: Session,
    table_ids: list[str],
    *,
    current_run_id: str | None = None,
    session_id: str | None = None,
) -> EntropyForReadiness:
    """Build the full readiness rollup (intents + bands) for typed tables.

    Runs the loss rollup. This is the terminal ``detect`` step's computation
    (persisted to ``entropy_readiness``); query-time consumers read the persisted
    band instead and use :func:`build_column_evidence` for the contract gate.

    Args:
        session: SQLAlchemy session.
        table_ids: List of table IDs to include.
        current_run_id: The in-flight detect run whose rows take precedence.
        session_id: Scope for resolving the promoted session detect head.

    Returns:
        EntropyForReadiness with computed context.
    """
    entropy_objects = _load_entropy_objects(
        session, table_ids, current_run_id=current_run_id, session_id=session_id
    )
    if not entropy_objects:
        return EntropyForReadiness()
    return assemble_readiness_context(entropy_objects)


def build_column_evidence(
    session: Session,
    table_ids: list[str],
    *,
    session_id: str | None = None,
) -> EntropyForReadiness:
    """Build raw per-column entropy evidence WITHOUT the loss rollup.

    The contract gate (``query_context.network_to_column_summaries``) only reads
    raw per-node scores + direct signals, never the rollup. This loads exactly
    that — the cheap half — so the rollup is computed once, at ``detect``, not
    re-run per query (DAT-399 slice D). ``avg_entropy_score`` is raw-derived and
    so still populated; intents/bands are intentionally empty.

    Args:
        session: SQLAlchemy session.
        table_ids: List of table IDs to include.
        session_id: Analytical session — resolves rows to the promoted session
            detect head (then table heads/legacy) instead of loading blindly.
            After a begin_session promote, add_source and session rows coexist
            for re-adjudicated detectors; omitted ⇒ the legacy blind load.

    Returns:
        EntropyForReadiness with per-column node evidence + direct signals only.
    """
    # Query time has no in-flight detect run, so only session_id is threaded —
    # resolution degrades to: session detect head > table heads/legacy.
    entropy_objects = _load_entropy_objects(session, table_ids, session_id=session_id)
    if not entropy_objects:
        return EntropyForReadiness()
    return assemble_readiness_context(entropy_objects, compute_rollup=False)


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

    Multi-run head-resolution (DAT-413): two add_source runs over the same table
    leave two readiness snapshots (distinct ``run_id`` under stage ``"detect"``).
    Per table, the promoted head names the current run, so the query is filtered
    to ``(table_id, run_id == head)`` — never a blind ``table_id.in_()`` that
    would mix runs. A table with no promoted detect run yet contributes nothing
    (graceful ``None`` — treated as "no readiness", i.e. ready).
    """
    if not table_ids:
        return EntropyForReadiness()

    # Resolve each table's promoted detect run; keep only tables that have one.
    # Column readiness is table-grain, so the head key is ``table:{id}`` (DAT-408).
    head_by_table = {
        table_id: run_id
        for table_id in table_ids
        if (run_id := head_run_id(session, f"table:{table_id}", "detect")) is not None
    }
    if not head_by_table:
        return EntropyForReadiness()

    record_filter = or_(
        *(
            and_(
                EntropyReadinessRecord.table_id == table_id,
                EntropyReadinessRecord.run_id == run_id,
            )
            for table_id, run_id in head_by_table.items()
        )
    )
    records = list(session.execute(select(EntropyReadinessRecord).where(record_filter)).scalars())
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


def load_relationship_readiness(session: Session, session_id: str) -> list[EntropyReadinessRecord]:
    """Current, gated relationship readiness for a session (DAT-408).

    Resolves the session's **current run** via the per-session head
    (``session:{id}``, "detect") — begin_session seals per session, not per target
    — then returns that run's ``relationship:`` readiness, **gated** on a live,
    non-suppressed ``Relationship`` *in the same run*. A dropped (overlay-rejected)
    relationship keeps its rows for audit but isn't surfaced, so there is no ghost
    readiness. Returns ``[]`` until the session has a promoted run.
    """
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.relationships.utils import load_suppressed_relationship_pairs
    from dataraum.entropy.models import parse_relationship_target
    from dataraum.storage import session_head_target

    current_run = head_run_id(session, session_head_target(session_id), "detect")
    if current_run is None:
        return []

    rows = list(
        session.execute(
            select(EntropyReadinessRecord).where(
                EntropyReadinessRecord.session_id == session_id,
                EntropyReadinessRecord.run_id == current_run,
                EntropyReadinessRecord.target.like("relationship:%"),
            )
        ).scalars()
    )
    if not rows:
        return []

    # The current run's live directional column pairs, minus user-dropped ones.
    live_pairs = set(
        session.execute(
            select(Relationship.from_column_id, Relationship.to_column_id).where(
                Relationship.session_id == session_id,
                Relationship.run_id == current_run,
            )
        ).tuples()
    )
    live_pairs -= load_suppressed_relationship_pairs(session)

    gated: list[EntropyReadinessRecord] = []
    for rec in rows:
        pair = parse_relationship_target(rec.target)
        if pair is None or pair not in live_pairs:
            continue
        gated.append(rec)
    return gated


def load_table_readiness(session: Session, session_id: str) -> list[EntropyReadinessRecord]:
    """Current table-grain readiness for a begin_session (DAT-415).

    Resolves the session's **current run** via the per-session head
    (``session:{id}``, "detect") — begin_session seals per session, not per table,
    and its terminal promote flips only that head — then returns that run's
    ``table:`` readiness rows (one per fact table whose enriched view the
    ``dimension_coverage`` detector measured). Returns ``[]`` until the session has
    a promoted run. Unlike relationships, tables are never user-suppressed, so
    there is no liveness gate. The cockpit reads these via Drizzle; this is the
    engine-side reader (tests, agent context).
    """
    from dataraum.storage import session_head_target

    current_run = head_run_id(session, session_head_target(session_id), "detect")
    if current_run is None:
        return []

    return list(
        session.execute(
            select(EntropyReadinessRecord).where(
                EntropyReadinessRecord.session_id == session_id,
                EntropyReadinessRecord.run_id == current_run,
                EntropyReadinessRecord.target.like("table:%"),
            )
        ).scalars()
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
