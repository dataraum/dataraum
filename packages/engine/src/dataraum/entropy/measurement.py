"""Entropy measurement.

Aggregates persisted detector records:
- ``measure_entropy``: read persisted detector records (no re-execution)
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
class MeasurementResult:
    """Result of measuring entropy across detector records."""

    scores: dict[str, float] = field(default_factory=dict)
    column_details: dict[str, dict[str, float]] = field(default_factory=dict)
    table_details: dict[str, dict[str, float]] = field(default_factory=dict)
    view_details: dict[str, dict[str, float]] = field(default_factory=dict)
    # Per-target component evidence: dim_path -> target -> {component_key: value}
    # Enables smart context: LLM sees which component drives each column's score
    column_evidence: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)


def _collect_evidence(
    obj: Any,
    target: str,
    evidence_by_dim: dict[str, dict[str, dict[str, Any]]],
) -> None:
    """Extract component evidence from an EntropyObject into the evidence dict.

    Passes all evidence keys through. Each detector decides what to put
    in its evidence dict; measurement doesn't filter.
    """
    if not obj.evidence:
        return
    ev = obj.evidence[0] if isinstance(obj.evidence, list) else obj.evidence
    if not isinstance(ev, dict):
        return
    if ev:
        evidence_by_dim[str(obj.sub_dimension)][target] = dict(ev)


def measure_entropy(
    session: Session,
    source_id: str,
    detector_ids: list[str],
) -> MeasurementResult:
    """Aggregate persisted detector records into dimension scores.

    Reads ``EntropyObjectRecord`` rows instead of re-running detectors.
    Records are produced by ``run_detector_post_step()`` after each phase.

    Args:
        session: SQLAlchemy session.
        source_id: The source being processed.
        detector_ids: Detector IDs to include.

    Returns:
        MeasurementResult with dimension scores and per-column/table/view details.
    """
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.entropy.detectors.base import get_default_registry

    if not detector_ids:
        return MeasurementResult()

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
        return MeasurementResult()

    scores_by_dim: dict[str, list[float]] = defaultdict(list)
    column_scores: dict[str, dict[str, float]] = defaultdict(dict)
    table_scores: dict[str, dict[str, float]] = defaultdict(dict)
    view_scores: dict[str, dict[str, float]] = defaultdict(dict)
    evidence_by_dim: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    for record in records:
        sub_dim = record.sub_dimension
        target = record.target
        scope = scope_by_detector.get(record.detector_id, "column")

        scores_by_dim[sub_dim].append(record.score)

        # Route to column/table/view details by scope
        if scope == "table":
            table_scores[sub_dim][target] = record.score
        elif scope == "view":
            view_scores[sub_dim][target] = record.score
        else:
            column_scores[sub_dim][target] = record.score

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

    result_evidence: dict[str, dict[str, dict[str, Any]]] = {}
    for sd, targets_ev in evidence_by_dim.items():
        path = sub_dim_to_path.get(sd, sd)
        result_evidence[path] = targets_ev

    return MeasurementResult(
        scores=result_scores,
        column_details=result_column_details,
        table_details=result_table_details,
        view_details=result_view_details,
        column_evidence=result_evidence,
    )


def match_threshold(
    dimension_path: str,
    thresholds: dict[str, float],
) -> float | None:
    """Find contract threshold by prefix match.

    Single source of truth for threshold resolution, used by contract
    evaluation and CLI display.

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
