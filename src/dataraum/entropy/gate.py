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
    skipped_detectors: list[SkippedDetector] = field(default_factory=list)


@dataclass
class ExitCheckIssue:
    """A contract violation detected at a quality gate."""

    dimension_path: str  # e.g. "structural.types.type_fidelity"
    score: float
    threshold: float
    producing_phase: str  # phase after which this was measured
    affected_targets: list[str] = field(default_factory=list)


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
        session.execute(
            select(Table).where(
                Table.source_id == source_id, Table.layer == "typed"
            )
        )
        .scalars()
        .all()
    )

    scores_by_dim: dict[str, list[float]] = defaultdict(list)
    column_scores: dict[str, dict[str, float]] = defaultdict(dict)

    # Column-scoped pass
    if col_dims:
        for table in typed_tables:
            columns = (
                session.execute(
                    select(ColumnModel).where(ColumnModel.table_id == table.table_id)
                )
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

    # View-scoped pass
    if view_dims:
        from dataraum.analysis.views.db_models import EnrichedView

        enriched_views = (
            session.execute(
                select(EnrichedView).where(
                    EnrichedView.fact_table_id.in_(
                        [t.table_id for t in typed_tables]
                    )
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

    # Build sub_dimension -> dimension_path mapping
    sub_dim_to_path: dict[str, str] = {
        str(d.sub_dimension): d.dimension_path for d in runnable
    }

    # Average per dimension
    result_scores: dict[str, float] = {}
    for sub_dim, score_list in scores_by_dim.items():
        mean_score = sum(score_list) / len(score_list)
        path = sub_dim_to_path.get(sub_dim, sub_dim)
        result_scores[path] = mean_score

    # Build column details keyed by dimension_path
    result_column_details: dict[str, dict[str, float]] = {}
    for sd, targets in column_scores.items():
        path = sub_dim_to_path.get(sd, sd)
        result_column_details[path] = targets

    return GateResult(
        scores=result_scores,
        column_details=result_column_details,
        skipped_detectors=skipped,
    )


def match_threshold(
    dimension_path: str,
    thresholds: dict[str, float],
) -> float | None:
    """Find contract threshold by prefix match.

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
) -> list[ExitCheckIssue]:
    """Check scores against contract thresholds.

    Args:
        scores: Dimension path -> score mapping.
        thresholds: Contract thresholds to check against.
        column_details: Per-column scores for affected target identification.
        producing_phase: Phase name where measurement occurred.

    Returns:
        List of contract violations found.
    """
    if not thresholds or not scores:
        return []

    issues: list[ExitCheckIssue] = []
    for dimension_path, score in scores.items():
        threshold = match_threshold(dimension_path, thresholds)
        if threshold is not None and score > threshold:
            col_scores = column_details.get(dimension_path, {})
            affected = [t for t, s in col_scores.items() if s > threshold]
            issues.append(
                ExitCheckIssue(
                    dimension_path=dimension_path,
                    score=score,
                    threshold=threshold,
                    producing_phase=producing_phase,
                    affected_targets=affected,
                )
            )
    return issues
