"""Quality gate measurement and contract assessment.

Provides the measurement and assessment logic that runs at quality gate phases:
- ``aggregate_at_gate``: read persisted detector records (no re-execution)
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

logger = get_logger(__name__)


@dataclass
class GateResult:
    """Result of measuring entropy at a quality gate."""

    scores: dict[str, float] = field(default_factory=dict)
    column_details: dict[str, dict[str, float]] = field(default_factory=dict)
    table_details: dict[str, dict[str, float]] = field(default_factory=dict)
    view_details: dict[str, dict[str, float]] = field(default_factory=dict)
    # dim_path -> {action_name, ...} — resolution options from all entropy objects
    resolution_actions: dict[str, set[str]] = field(default_factory=dict)
    # Per-target component evidence: dim_path -> target -> {component_key: value}
    # Enables smart context: LLM sees which component drives each column's score
    column_evidence: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    # dim_path -> {target, ...} — targets accepted via DataFix records
    accepted_targets: dict[str, set[str]] = field(default_factory=dict)
    # target -> filter_confidence — records discounted by business pattern filter
    filter_applied: dict[str, float] = field(default_factory=dict)


@dataclass
class ExitCheckIssue:
    """A contract violation detected at a quality gate."""

    dimension_path: str  # e.g. "structural.types.type_fidelity"
    score: float
    threshold: float
    producing_phase: str  # phase after which this was measured
    affected_targets: list[str] = field(default_factory=list)
    available_actions: list[str] = field(default_factory=list)


def _get_accepted_targets(
    session: Session,
    source_id: str,
) -> dict[str, set[str]]:
    """Query DataFix for accepted targets grouped by dimension.

    Returns a dict mapping dimension_path to a set of target strings
    (e.g. ``"column:orders.amount"``, ``"table:orders"``).
    Only considers applied ``document_accepted_*`` DataFix records.
    """
    from dataraum.pipeline.fixes.models import DataFix

    fixes = (
        session.execute(
            select(DataFix).where(
                DataFix.source_id == source_id,
                DataFix.action.like("document_accepted_%"),
                DataFix.status == "applied",
            )
        )
        .scalars()
        .all()
    )

    result: dict[str, set[str]] = defaultdict(set)
    for fix in fixes:
        if fix.column_name:
            target = f"column:{fix.table_name}.{fix.column_name}"
        else:
            target = f"table:{fix.table_name}"
        result[fix.dimension].add(target)
    return dict(result)


def _collect_evidence(
    obj: Any,
    target: str,
    evidence_by_dim: dict[str, dict[str, dict[str, Any]]],
) -> None:
    """Extract component evidence from an EntropyObject into the evidence dict.

    Passes all evidence keys through. Each detector decides what to put
    in its evidence dict; the gate doesn't filter.
    """
    if not obj.evidence:
        return
    ev = obj.evidence[0] if isinstance(obj.evidence, list) else obj.evidence
    if not isinstance(ev, dict):
        return
    if ev:
        evidence_by_dim[str(obj.sub_dimension)][target] = dict(ev)


def aggregate_at_gate(
    session: Session,
    source_id: str,
    detector_ids: list[str],
) -> GateResult:
    """Aggregate persisted detector records at a quality gate.

    Reads ``EntropyObjectRecord`` rows instead of re-running detectors.
    Records are produced by ``run_detector_post_step()`` after each phase.

    Args:
        session: SQLAlchemy session.
        source_id: The source being processed.
        detector_ids: Detector IDs to include (from completed phases).

    Returns:
        GateResult with dimension scores and per-column/table/view details.
    """
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.entropy.detectors.base import get_default_registry

    if not detector_ids:
        return GateResult()

    registry = get_default_registry()

    # Build sub_dimension -> dimension_path mapping from detector registry
    sub_dim_to_path: dict[str, str] = {}
    scope_by_detector: dict[str, str] = {}
    for d in registry.get_all_detectors():
        if d.detector_id in detector_ids:
            sub_dim_to_path[str(d.sub_dimension)] = d.dimension_path
            scope_by_detector[d.detector_id] = d.scope

    # Load all records for these detectors
    records = list(
        session.execute(
            select(EntropyObjectRecord).where(
                EntropyObjectRecord.source_id == source_id,
                EntropyObjectRecord.detector_id.in_(detector_ids),
            )
        )
        .scalars()
        .all()
    )

    if not records:
        return GateResult()

    # Apply business pattern filter before aggregation
    from dataraum.entropy.pattern_filter import CONFIDENCE_THRESHOLD, apply_pattern_filter

    records = apply_pattern_filter(session, source_id, records)

    scores_by_dim: dict[str, list[float]] = defaultdict(list)
    column_scores: dict[str, dict[str, float]] = defaultdict(dict)
    table_scores: dict[str, dict[str, float]] = defaultdict(dict)
    view_scores: dict[str, dict[str, float]] = defaultdict(dict)
    actions_by_dim: dict[str, set[str]] = defaultdict(set)
    evidence_by_dim: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    filter_applied: dict[str, float] = {}

    for record in records:
        sub_dim = record.sub_dimension
        target = record.target
        scope = scope_by_detector.get(record.detector_id, "column")

        scores_by_dim[sub_dim].append(record.score)

        # Track filtered records
        if (
            record.filter_confidence is not None
            and record.filter_confidence >= CONFIDENCE_THRESHOLD
        ):
            filter_applied[target] = record.filter_confidence

        # Route to column/table/view details by scope
        if scope == "table":
            table_scores[sub_dim][target] = record.score
        elif scope == "view":
            view_scores[sub_dim][target] = record.score
        else:
            column_scores[sub_dim][target] = record.score

        # Reconstruct resolution_actions from record
        if record.resolution_options:
            for opt in record.resolution_options:
                action = opt.get("action")
                if action:
                    actions_by_dim[sub_dim].add(action)

        # Reconstruct column_evidence from record
        _collect_evidence(record, target, evidence_by_dim)

    # Aggregate per dimension: max(mean, max²)
    result_scores: dict[str, float] = {}
    for sub_dim, score_list in scores_by_dim.items():
        mean_score = sum(score_list) / len(score_list)
        max_score = max(score_list)
        path = sub_dim_to_path.get(sub_dim, sub_dim)
        result_scores[path] = round(max(mean_score, max_score**2), 4)

    # Build details keyed by dimension_path
    result_column_details: dict[str, dict[str, float]] = {}
    for sd, targets in column_scores.items():
        path = sub_dim_to_path.get(sd, sd)
        result_column_details[path] = targets

    result_table_details: dict[str, dict[str, float]] = {}
    for sd, targets in table_scores.items():
        path = sub_dim_to_path.get(sd, sd)
        result_table_details[path] = targets

    result_view_details: dict[str, dict[str, float]] = {}
    for sd, targets in view_scores.items():
        path = sub_dim_to_path.get(sd, sd)
        result_view_details[path] = targets

    result_actions: dict[str, set[str]] = {}
    for sd, acts in actions_by_dim.items():
        path = sub_dim_to_path.get(sd, sd)
        result_actions.setdefault(path, set()).update(acts)

    result_evidence: dict[str, dict[str, dict[str, Any]]] = {}
    for sd, targets_ev in evidence_by_dim.items():
        path = sub_dim_to_path.get(sd, sd)
        result_evidence[path] = targets_ev

    # Query DataFix for accepted targets
    accepted = _get_accepted_targets(session, source_id)

    return GateResult(
        scores=result_scores,
        column_details=result_column_details,
        table_details=result_table_details,
        view_details=result_view_details,
        resolution_actions=result_actions,
        column_evidence=result_evidence,
        accepted_targets=accepted,
        filter_applied=filter_applied,
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
    accepted_targets: dict[str, set[str]] | None = None,
) -> list[ExitCheckIssue]:
    """Check scores against contract thresholds.

    Accepted targets (from DataFix records) are excluded from violation
    assessment.  The dimension score is still computed from all targets,
    but only non-accepted targets above threshold count as affected.
    If every above-threshold target is accepted, the dimension is not
    reported as a violation (contract overrule).

    Args:
        scores: Dimension path -> score mapping.
        thresholds: Contract thresholds to check against.
        column_details: Per-column scores for affected target identification.
        producing_phase: Phase name where measurement occurred.
        resolution_actions: Dimension path -> action names from detector
            resolution options. Used to filter fix schemas to only those
            the detectors actually recommended.
        accepted_targets: Dimension path -> set of accepted target strings
            from DataFix records.

    Returns:
        List of contract violations found.
    """
    if not thresholds or not scores:
        return []

    accepted = accepted_targets or {}

    issues: list[ExitCheckIssue] = []
    for dimension_path, score in scores.items():
        threshold = match_threshold(dimension_path, thresholds)
        if threshold is not None and score > threshold:
            col_scores = column_details.get(dimension_path, {})
            dim_accepted = accepted.get(dimension_path, set())

            # Filter: only non-accepted targets above threshold are violations
            above_threshold = [t for t, s in col_scores.items() if s > threshold]
            affected = [t for t in above_threshold if t not in dim_accepted]

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
    if gate_result.accepted_targets:
        # Convert sets to lists for JSON serialization
        existing_outputs["accepted_targets"] = {
            k: sorted(v) for k, v in gate_result.accepted_targets.items()
        }
    if gate_result.filter_applied:
        existing_outputs["filter_applied"] = gate_result.filter_applied
    log.outputs = existing_outputs
    session.commit()
