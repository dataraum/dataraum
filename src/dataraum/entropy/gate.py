"""Quality gate measurement and contract assessment.

Provides the measurement and assessment logic that runs at quality gate phases:
- ``measure_at_gate``: run all eligible detectors fresh
- ``assess_contracts``: check scores against contract thresholds
- ``match_threshold``: find the applicable threshold for a dimension path
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.dimensions import AnalysisKey

logger = get_logger(__name__)


@dataclass
class SkippedDetector:
    """A detector that was eligible but could not run."""

    detector_id: str
    reason: str  # e.g. "missing analyses: typing, statistics"


@dataclass
class GateResult:
    """Result of measuring entropy at a quality gate."""

    scores: dict[str, float] = field(default_factory=dict)
    column_details: dict[str, dict[str, float]] = field(default_factory=dict)
    table_details: dict[str, dict[str, float]] = field(default_factory=dict)
    view_details: dict[str, dict[str, float]] = field(default_factory=dict)
    skipped_detectors: list[SkippedDetector] = field(default_factory=list)
    # dim_path -> {action_name, ...} — resolution options from all entropy objects
    resolution_actions: dict[str, set[str]] = field(default_factory=dict)
    # Per-target component evidence: dim_path -> target -> {component_key: value}
    # Enables smart context: LLM sees which component drives each column's score
    column_evidence: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)


@dataclass
class ExitCheckIssue:
    """A contract violation detected at a quality gate."""

    dimension_path: str  # e.g. "structural.types.type_fidelity"
    score: float
    threshold: float
    producing_phase: str  # phase after which this was measured
    affected_targets: list[str] = field(default_factory=list)
    available_actions: list[str] = field(default_factory=list)


_EVIDENCE_KEYS = frozenset(
    {
        "ri_entropy",
        "card_entropy",
        "semantic_entropy",
        "from_table",
        "to_table",
        "accepted",
        "aggregation_method",
        "ri_boosted",
    }
)
"""Evidence keys to persist in gate results for smart context."""


def _collect_evidence(
    obj: Any,
    target: str,
    evidence_by_dim: dict[str, dict[str, dict[str, Any]]],
) -> None:
    """Extract component evidence from an EntropyObject into the evidence dict.

    Picks only keys useful for smart context (component scores, metadata).
    Keyed by sub_dimension string and target.
    """
    if not obj.evidence:
        return
    ev = obj.evidence[0] if isinstance(obj.evidence, list) else obj.evidence
    if not isinstance(ev, dict):
        return
    summary = {k: v for k, v in ev.items() if k in _EVIDENCE_KEYS}
    if summary:
        evidence_by_dim[str(obj.sub_dimension)][target] = summary


def measure_at_gate(
    session: Session,
    duckdb_conn: Any,
    source_id: str,
    available_analyses: set[AnalysisKey],
) -> GateResult:
    """Run all eligible detectors fresh at a quality gate.

    Finds detectors whose required_analyses are fully satisfied by
    available_analyses, then runs them against typed tables/columns/views.

    Args:
        session: SQLAlchemy session for DB queries.
        duckdb_conn: DuckDB connection.
        source_id: The source being processed.
        available_analyses: Analysis keys produced by completed phases.

    Returns:
        GateResult with dimension scores and per-column details.
    """
    from dataraum.entropy.detectors.base import get_default_registry
    from dataraum.entropy.snapshot import take_snapshot
    from dataraum.storage.models import Column as ColumnModel
    from dataraum.storage.models import Table

    registry = get_default_registry()

    # Partition detectors into runnable vs skipped
    all_detectors = registry.get_all_detectors()
    runnable = []
    skipped: list[SkippedDetector] = []
    for d in all_detectors:
        missing = [a for a in d.required_analyses if a not in available_analyses]
        if missing:
            skipped.append(
                SkippedDetector(
                    detector_id=d.detector_id,
                    reason=f"missing analyses: {', '.join(str(a) for a in missing)}",
                )
            )
        else:
            runnable.append(d)

    if not runnable:
        return GateResult(skipped_detectors=skipped)

    # Partition by scope
    col_detectors = [d for d in runnable if d.scope == "column"]
    tbl_detectors = [d for d in runnable if d.scope == "table"]
    view_detectors = [d for d in runnable if d.scope == "view"]

    col_dims = [d.sub_dimension for d in col_detectors]
    tbl_dims = [d.sub_dimension for d in tbl_detectors]
    view_dims = [d.sub_dimension for d in view_detectors]

    # Get all typed tables for this source
    typed_tables = (
        session.execute(select(Table).where(Table.source_id == source_id, Table.layer == "typed"))
        .scalars()
        .all()
    )

    scores_by_dim: dict[str, list[float]] = defaultdict(list)
    column_scores: dict[str, dict[str, float]] = defaultdict(dict)
    table_scores: dict[str, dict[str, float]] = defaultdict(dict)
    view_scores: dict[str, dict[str, float]] = defaultdict(dict)
    actions_by_dim: dict[str, set[str]] = defaultdict(set)
    evidence_by_dim: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    # Column-scoped pass
    if col_dims:
        for table in typed_tables:
            columns = (
                session.execute(select(ColumnModel).where(ColumnModel.table_id == table.table_id))
                .scalars()
                .all()
            )
            for col in columns:
                target = f"column:{table.table_name}.{col.column_name}"
                snapshot = take_snapshot(
                    target=target,
                    session=session,
                    duckdb_conn=duckdb_conn,
                    dimensions=col_dims,
                )
                for sub_dim, score in snapshot.scores.items():
                    scores_by_dim[sub_dim].append(score)
                    column_scores[sub_dim][target] = score
                for obj in snapshot.objects:
                    for opt in obj.resolution_options:
                        actions_by_dim[str(obj.sub_dimension)].add(opt.action)
                    _collect_evidence(obj, target, evidence_by_dim)

    # Table-scoped pass
    if tbl_dims:
        for table in typed_tables:
            target = f"table:{table.table_name}"
            snapshot = take_snapshot(
                target=target,
                session=session,
                duckdb_conn=duckdb_conn,
                dimensions=tbl_dims,
            )
            for sub_dim, score in snapshot.scores.items():
                scores_by_dim[sub_dim].append(score)
                table_scores[sub_dim][target] = score
            for obj in snapshot.objects:
                for opt in obj.resolution_options:
                    actions_by_dim[str(obj.sub_dimension)].add(opt.action)
                _collect_evidence(obj, target, evidence_by_dim)

    # View-scoped pass
    if view_dims:
        from dataraum.analysis.views.db_models import EnrichedView

        enriched_views = (
            session.execute(
                select(EnrichedView).where(
                    EnrichedView.fact_table_id.in_([t.table_id for t in typed_tables])
                )
            )
            .scalars()
            .all()
        )

        for ev in enriched_views:
            target = f"view:{ev.view_name}"
            snapshot = take_snapshot(
                target=target,
                session=session,
                duckdb_conn=duckdb_conn,
                dimensions=view_dims,
            )
            for sub_dim, score in snapshot.scores.items():
                scores_by_dim[sub_dim].append(score)
                view_scores[sub_dim][target] = score
            for obj in snapshot.objects:
                for opt in obj.resolution_options:
                    actions_by_dim[str(obj.sub_dimension)].add(opt.action)
                _collect_evidence(obj, target, evidence_by_dim)

    # Build sub_dimension -> dimension_path mapping
    sub_dim_to_path: dict[str, str] = {str(d.sub_dimension): d.dimension_path for d in runnable}

    # Aggregate per dimension: max(mean, max²)
    # The squared-max term ensures a single bad column (e.g. VARCHAR date)
    # is not diluted away when averaged across many healthy columns.
    result_scores: dict[str, float] = {}
    for sub_dim, score_list in scores_by_dim.items():
        mean_score = sum(score_list) / len(score_list)
        max_score = max(score_list)
        path = sub_dim_to_path.get(sub_dim, sub_dim)
        result_scores[path] = round(max(mean_score, max_score**2), 4)

    # Build column details keyed by dimension_path
    result_column_details: dict[str, dict[str, float]] = {}
    for sd, targets in column_scores.items():
        path = sub_dim_to_path.get(sd, sd)
        result_column_details[path] = targets

    # Build table details keyed by dimension_path
    result_table_details: dict[str, dict[str, float]] = {}
    for sd, targets in table_scores.items():
        path = sub_dim_to_path.get(sd, sd)
        result_table_details[path] = targets

    # Build view details keyed by dimension_path
    result_view_details: dict[str, dict[str, float]] = {}
    for sd, targets in view_scores.items():
        path = sub_dim_to_path.get(sd, sd)
        result_view_details[path] = targets

    # Build resolution actions keyed by dimension_path
    result_actions: dict[str, set[str]] = {}
    for sd, acts in actions_by_dim.items():
        path = sub_dim_to_path.get(sd, sd)
        result_actions.setdefault(path, set()).update(acts)

    # Build evidence keyed by dimension_path
    result_evidence: dict[str, dict[str, dict[str, Any]]] = {}
    for sd, targets_ev in evidence_by_dim.items():
        path = sub_dim_to_path.get(sd, sd)
        result_evidence[path] = targets_ev

    return GateResult(
        scores=result_scores,
        column_details=result_column_details,
        table_details=result_table_details,
        view_details=result_view_details,
        skipped_detectors=skipped,
        resolution_actions=result_actions,
        column_evidence=result_evidence,
    )


def match_threshold(
    dimension_path: str,
    thresholds: dict[str, float],
) -> float | None:
    """Find contract threshold by prefix match.

    Single source of truth for threshold resolution, used by gate
    assessment (``assess_contracts``), contract evaluation, and CLI
    display (``render_gate_scores``, ``_print_summary``).

    Given "structural.types.type_fidelity", checks for thresholds at:
    - "structural.types.type_fidelity" (exact)
    - "structural.types" (prefix)
    - "structural" (prefix)

    Args:
        dimension_path: Full dimension path to match.
        thresholds: Map of threshold prefixes to values.

    Returns:
        The matching threshold value, or None if no match.
    """
    parts = dimension_path.split(".")
    for i in range(len(parts), 0, -1):
        prefix = ".".join(parts[:i])
        if prefix in thresholds:
            return thresholds[prefix]
    return None


def assess_contracts(
    scores: dict[str, float],
    thresholds: dict[str, float],
    column_details: dict[str, dict[str, float]],
    producing_phase: str,
    resolution_actions: dict[str, set[str]] | None = None,
    column_evidence: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> list[ExitCheckIssue]:
    """Check scores against contract thresholds.

    Accepted targets (``evidence.accepted=True``) are excluded from
    violation assessment.  The dimension score is still computed from all
    targets, but only non-accepted targets above threshold count as
    affected.  If every above-threshold target is accepted, the dimension
    is not reported as a violation (contract overrule).

    Args:
        scores: Dimension path -> score mapping.
        thresholds: Contract thresholds to check against.
        column_details: Per-column scores for affected target identification.
        producing_phase: Phase name where measurement occurred.
        resolution_actions: Dimension path -> action names from detector
            resolution options. Used to filter fix schemas to only those
            the detectors actually recommended.
        column_evidence: Per-target evidence dicts for accepted-target
            detection.  Keyed by dimension_path -> target -> evidence dict.

    Returns:
        List of contract violations found.
    """
    if not thresholds or not scores:
        return []

    ev = column_evidence or {}

    issues: list[ExitCheckIssue] = []
    for dimension_path, score in scores.items():
        threshold = match_threshold(dimension_path, thresholds)
        if threshold is not None and score > threshold:
            col_scores = column_details.get(dimension_path, {})
            dim_evidence = ev.get(dimension_path, {})

            # Filter: only non-accepted targets above threshold are violations
            above_threshold = [t for t, s in col_scores.items() if s > threshold]
            affected = [
                t for t in above_threshold if not dim_evidence.get(t, {}).get("accepted", False)
            ]

            # Contract overrule: if all above-threshold targets are accepted,
            # the dimension is acknowledged — don't block the gate.
            # Only applies when we have column-level detail; if no column
            # details exist, the dimension score alone triggers the violation.
            if above_threshold and not affected:
                continue

            actions = resolution_actions.get(dimension_path, set()) if resolution_actions else set()
            issues.append(
                ExitCheckIssue(
                    dimension_path=dimension_path,
                    score=score,
                    threshold=threshold,
                    producing_phase=producing_phase,
                    affected_targets=affected,
                    available_actions=sorted(actions),
                )
            )
    return issues


def persist_gate_result(
    session: Session,
    source_id: str,
    gate_result: GateResult,
    *,
    phase_name: str = "quality_review",
    run_id: str | None = None,
) -> None:
    """Persist gate scores to a PhaseLog record.

    Shared by the scheduler (scoped by run_id) and the standalone fix API
    (scoped by source_id + phase_name).

    Args:
        session: SQLAlchemy session.
        source_id: Source identifier.
        gate_result: Gate measurement result to persist.
        phase_name: Phase to update (default: quality_review).
        run_id: If provided, scope lookup to this run (scheduler path).
            Otherwise scope by source_id + phase_name (API path).
    """
    from sqlalchemy import select as sa_select

    from dataraum.entropy.detectors.base import get_default_registry
    from dataraum.pipeline.db_models import PhaseLog

    if run_id is not None:
        log = session.execute(
            sa_select(PhaseLog)
            .where(
                PhaseLog.run_id == run_id,
                PhaseLog.phase_name == phase_name,
            )
            .order_by(PhaseLog.completed_at.desc())
            .limit(1)
        ).scalar_one_or_none()
    else:
        log = session.execute(
            sa_select(PhaseLog)
            .where(
                PhaseLog.source_id == source_id,
                PhaseLog.phase_name == phase_name,
            )
            .order_by(PhaseLog.completed_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    if log is None:
        return

    registry = get_default_registry()
    detector_id_map = {d.dimension_path: d.detector_id for d in registry.get_all_detectors()}

    log.entropy_scores = dict(gate_result.scores)
    existing_outputs = dict(log.outputs) if log.outputs else {}
    existing_outputs["gate_column_details"] = dict(gate_result.column_details)
    existing_outputs["gate_table_details"] = dict(gate_result.table_details)
    existing_outputs["gate_view_details"] = dict(gate_result.view_details)
    existing_outputs["detector_id_map"] = detector_id_map
    if gate_result.column_evidence:
        existing_outputs["gate_column_evidence"] = dict(gate_result.column_evidence)
    log.outputs = existing_outputs
    session.commit()
