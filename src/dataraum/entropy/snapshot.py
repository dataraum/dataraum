"""Snapshot — run detectors on a target and return canonical scores.

Provides the core measurement mechanism for entropy gates:
- `take_snapshot()`: measure entropy for a column/table
- `load_column_analysis()`: load analysis results for a single column
- `Snapshot`: immutable result with scores, detectors run, timestamp
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import (
    DetectorContext,
    get_default_registry,
)
from dataraum.entropy.models import EntropyObject

logger = get_logger(__name__)


@dataclass(frozen=True)
class Snapshot:
    """Immutable snapshot of detector scores for a target."""

    scores: dict[str, float]  # sub_dimension -> score
    detectors_run: list[str]  # detector_ids that were executed
    objects: tuple[EntropyObject, ...] = ()  # full EntropyObject instances
    measured_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def score_for(self, sub_dimension: str) -> float | None:
        """Look up a score by sub_dimension name."""
        return self.scores.get(sub_dimension)


def load_column_analysis(
    session: Session,
    table_id: str,
    column_id: str,
    table_name: str = "",
) -> dict[str, Any]:
    """Load analysis results for a single column from the DB.

    Returns a dict matching the structure expected by DetectorContext:
    - "typing": type decision + candidate info
    - "statistics": statistical profile + quality metrics
    - "semantic": semantic annotation
    - "relationships": LLM-confirmed relationships
    - "correlation": derived column info
    - "drift_summaries": temporal drift data
    """
    from dataraum.analysis.correlation.db_models import DerivedColumn
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.statistics.db_models import StatisticalProfile
    from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics
    from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
    from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
    from dataraum.storage import Column, Table

    analysis_results: dict[str, Any] = {}

    # Typing
    td = session.execute(
        select(TypeDecision).where(TypeDecision.column_id == column_id)
    ).scalar_one_or_none()
    tc = session.execute(
        select(TypeCandidate)
        .where(TypeCandidate.column_id == column_id)
        .order_by(TypeCandidate.confidence.desc())
        .limit(1)
    ).scalar_one_or_none()

    if td:
        typing_dict: dict[str, Any] = {
            "resolved_type": td.decided_type,
            "data_type": td.decided_type,
            "detected_type": td.decided_type,
            "decision_source": td.decision_source,
            "decision_reason": td.decision_reason,
        }
        if tc:
            typing_dict["confidence"] = tc.confidence
            typing_dict["parse_success_rate"] = tc.parse_success_rate or 1.0
            typing_dict["failed_examples"] = tc.failed_examples or []
            typing_dict["detected_pattern"] = tc.detected_pattern
            typing_dict["pattern_match_rate"] = tc.pattern_match_rate
            typing_dict["detected_unit"] = tc.detected_unit
            typing_dict["unit_confidence"] = tc.unit_confidence
        analysis_results["typing"] = typing_dict
    elif tc:
        analysis_results["typing"] = {
            "data_type": tc.data_type,
            "detected_type": tc.data_type,
            "confidence": tc.confidence,
            "parse_success_rate": tc.parse_success_rate or 1.0,
            "failed_examples": tc.failed_examples or [],
            "detected_pattern": tc.detected_pattern,
            "pattern_match_rate": tc.pattern_match_rate,
            "detected_unit": tc.detected_unit,
            "unit_confidence": tc.unit_confidence,
        }

    # Statistics
    sp = session.execute(
        select(StatisticalProfile).where(StatisticalProfile.column_id == column_id)
    ).scalar_one_or_none()

    if sp:
        stats_dict: dict[str, Any] = {
            "null_count": sp.null_count,
            "null_ratio": sp.null_count / sp.total_count if sp.total_count else 0,
            "distinct_count": sp.distinct_count,
            "cardinality_ratio": sp.cardinality_ratio,
            "total_count": sp.total_count,
            "profile_data": sp.profile_data,
        }
        qm = session.execute(
            select(StatisticalQualityMetrics).where(
                StatisticalQualityMetrics.column_id == column_id
            )
        ).scalar_one_or_none()
        if qm:
            qd = qm.quality_data or {}
            outlier_data = qd.get("outlier_detection", {})
            stats_dict["quality"] = {
                "outlier_detection": {
                    "iqr_outlier_ratio": qm.iqr_outlier_ratio or 0.0,
                    "iqr_outlier_count": outlier_data.get("iqr_outlier_count", 0),
                    "iqr_lower_fence": outlier_data.get("iqr_lower_fence"),
                    "iqr_upper_fence": outlier_data.get("iqr_upper_fence"),
                    "zscore_outlier_ratio": qm.zscore_outlier_ratio,
                    "has_outliers": bool(qm.has_outliers),
                },
                "benford_compliant": bool(qm.benford_compliant)
                if qm.benford_compliant is not None
                else None,
                "benford_analysis": qd.get("benford_analysis"),
                "quality_data": qm.quality_data,
            }
        analysis_results["statistics"] = stats_dict

    # Semantic
    sa = session.execute(
        select(SemanticAnnotation).where(SemanticAnnotation.column_id == column_id)
    ).scalar_one_or_none()
    if sa:
        semantic_dict: dict[str, Any] = {
            "semantic_role": sa.semantic_role,
            "entity_type": sa.entity_type,
            "business_name": sa.business_name,
            "business_description": sa.business_description,
            "confidence": sa.confidence,
            "business_concept": sa.business_concept,
        }
        if sa.unit_source_column:
            semantic_dict["unit_source_column"] = sa.unit_source_column
        analysis_results["semantic"] = semantic_dict

    # Relationships (LLM-confirmed only)
    col = session.execute(select(Column).where(Column.column_id == column_id)).scalar_one_or_none()
    if col:
        rels_stmt = select(Relationship).where(
            ((Relationship.from_column_id == column_id) | (Relationship.to_column_id == column_id))
            & (Relationship.detection_method == "llm")
        )
        rels = session.execute(rels_stmt).scalars().all()
        if rels:
            # Resolve table names for relationship context
            table_ids_needed = {r.from_table_id for r in rels} | {r.to_table_id for r in rels}
            table_names_map: dict[str, str] = {}
            if table_ids_needed:
                tables = (
                    session.execute(select(Table).where(Table.table_id.in_(table_ids_needed)))
                    .scalars()
                    .all()
                )
                table_names_map = {t.table_id: t.table_name for t in tables}

            rel_dicts = []
            for rel in rels:
                rel_dicts.append(
                    {
                        "relationship_type": rel.relationship_type,
                        "confidence": rel.confidence,
                        "detection_method": rel.detection_method,
                        "from_table": table_names_map.get(rel.from_table_id, "unknown"),
                        "to_table": table_names_map.get(rel.to_table_id, "unknown"),
                        "cardinality": rel.cardinality,
                        "is_confirmed": rel.is_confirmed,
                        "evidence": rel.evidence,
                    }
                )
            analysis_results["relationships"] = rel_dicts

    # Correlation (derived columns) — a column may have multiple DerivedColumn rows
    dcs = (
        session.execute(select(DerivedColumn).where(DerivedColumn.derived_column_id == column_id))
        .scalars()
        .all()
    )
    if dcs and col:
        analysis_results["correlation"] = {
            "derived_columns": [
                {
                    "derived_column_name": col.column_name,
                    "formula": dc.formula,
                    "match_rate": dc.match_rate,
                    "derivation_type": dc.derivation_type,
                    "source_column_ids": dc.source_column_ids or [],
                }
                for dc in dcs
            ]
        }

    # Drift summaries — load via slice tables scoped to this table
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.slicing.slice_runner import _get_slice_table_name

    if col:
        col_name = col.column_name
        # Find slice tables for this table
        slice_defs = (
            session.execute(select(SliceDefinition).where(SliceDefinition.table_id == table_id))
            .scalars()
            .all()
        )
        # Load all columns in the table for name resolution
        all_cols = (
            session.execute(select(Column).where(Column.table_id == table_id)).scalars().all()
        )
        col_name_map = {c.column_id: c.column_name for c in all_cols}

        # Resolve source table name for namespaced slice table names
        source_table_name = table_name
        if not source_table_name:
            source_table = session.get(Table, table_id)
            source_table_name = source_table.table_name if source_table else ""

        slice_table_names: list[str] = []
        for sd in slice_defs:
            # Prefer sd.column_name (enriched FK-prefixed name used by slice_runner
            # to derive table names), fall back to column_id lookup for older records.
            sd_col_name = sd.column_name or col_name_map.get(sd.column_id)
            if sd_col_name and sd.distinct_values and source_table_name:
                for value in sd.distinct_values:
                    slice_table_names.append(
                        _get_slice_table_name(source_table_name, sd_col_name, value)
                    )

        if slice_table_names:
            drift_stmt = select(ColumnDriftSummary).where(
                ColumnDriftSummary.slice_table_name.in_(slice_table_names),
                ColumnDriftSummary.column_name == col_name,
            )
            drift_summaries = session.execute(drift_stmt).scalars().all()
            if drift_summaries:
                analysis_results["drift_summaries"] = list(drift_summaries)

    return analysis_results


def _resolve_table_target(
    session: Session,
    target: str,
) -> tuple[str, str] | None:
    """Parse table target string and resolve to (table_id, table_name).

    Supports format: "table:table_name"

    Returns None if target cannot be resolved.
    """
    from dataraum.storage import Table

    ref = target.split(":", 1)[1] if ":" in target else target

    table = session.execute(
        select(Table).where(Table.table_name == ref, Table.layer == "typed")
    ).scalar_one_or_none()
    if not table:
        return None

    return table.table_id, ref


def load_table_analysis(
    session: Session,
    table_id: str,
    table_name: str,
) -> dict[str, Any]:
    """Load table-scoped analysis results for cross-column detectors.

    Builds the analysis_results dict expected by DimensionalEntropyDetector:
    - "slice_variance": {columns, slice_data, temporal_columns, temporal_drift}
    - "drift_summaries": list of ColumnDriftSummary objects

    This is a simplified extraction from entropy_phase._run_dimensional_entropy().
    It only builds the analysis dict — it does NOT create per-column EntropyObjects.

    Args:
        session: SQLAlchemy session
        table_id: ID of the typed table
        table_name: Name of the typed table

    Returns:
        Dict with slice_variance and drift_summaries data.
    """
    from dataraum.analysis.quality_summary.db_models import ColumnSliceProfile
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.slicing.slice_runner import _get_slice_table_name
    from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
    from dataraum.storage import Column, Table

    # Get columns for this typed table
    table_columns = list(
        session.execute(select(Column).where(Column.table_id == table_id)).scalars().all()
    )
    table_column_ids = [c.column_id for c in table_columns]

    # Check for slicing_view table (FK-based scoping)
    sv_table = session.execute(
        select(Table).where(
            Table.table_name == f"slicing_{table_name}",
            Table.layer == "slicing_view",
        )
    ).scalar_one_or_none()

    if sv_table:
        sv_cols = (
            session.execute(select(Column).where(Column.table_id == sv_table.table_id))
            .scalars()
            .all()
        )
        lookup_column_ids = [c.column_id for c in sv_cols]
    else:
        lookup_column_ids = table_column_ids

    # Load column slice profiles
    profiles = list(
        session.execute(
            select(ColumnSliceProfile).where(
                ColumnSliceProfile.source_column_id.in_(lookup_column_ids)
            )
        )
        .scalars()
        .all()
    )

    if not profiles:
        return {}

    # Build slice_data: slice_value -> column_name -> metrics
    slice_data: dict[str, dict[str, dict[str, Any]]] = {}
    columns_data: dict[str, dict[str, Any]] = {}

    for profile in profiles:
        slice_val = profile.slice_value
        col_name = profile.column_name

        if slice_val not in slice_data:
            slice_data[slice_val] = {}

        slice_data[slice_val][col_name] = {
            "null_ratio": profile.null_ratio,
            "distinct_count": profile.distinct_count,
            "row_count": profile.row_count,
            "quality_score": profile.quality_score,
            "has_issues": profile.has_issues,
        }

        if col_name not in columns_data:
            columns_data[col_name] = {
                "classification": profile.variance_classification or "stable",
                "null_ratios": [],
                "distinct_counts": [],
                "exceeded_thresholds": [],
            }
        if profile.null_ratio is not None:
            columns_data[col_name]["null_ratios"].append(profile.null_ratio)
        if profile.distinct_count is not None:
            columns_data[col_name]["distinct_counts"].append(profile.distinct_count)

    # Calculate variance metrics per column
    for col_metrics in columns_data.values():
        null_ratios = col_metrics.get("null_ratios", [])
        distinct_counts = col_metrics.get("distinct_counts", [])

        if null_ratios and len(null_ratios) > 1:
            col_metrics["null_spread"] = max(null_ratios) - min(null_ratios)
        else:
            col_metrics["null_spread"] = 0.0

        if distinct_counts and len(distinct_counts) > 1 and min(distinct_counts) > 0:
            col_metrics["distinct_ratio"] = max(distinct_counts) / min(distinct_counts)
        else:
            col_metrics["distinct_ratio"] = 1.0

        if col_metrics["null_spread"] > 0.1 or col_metrics["distinct_ratio"] > 2.0:
            col_metrics["classification"] = "interesting"
            if col_metrics["null_spread"] > 0.1:
                col_metrics["exceeded_thresholds"].append("null_spread")
            if col_metrics["distinct_ratio"] > 2.0:
                col_metrics["exceeded_thresholds"].append("distinct_ratio")

    # Load drift summaries for slice tables
    col_name_by_id = {c.column_id: c.column_name for c in table_columns}
    slice_defs = list(
        session.execute(
            select(SliceDefinition).where(SliceDefinition.table_id == table_id)
        )
        .scalars()
        .all()
    )

    slice_table_names: list[str] = []
    for sd in slice_defs:
        sd_col_name = sd.column_name or col_name_by_id.get(sd.column_id)
        if sd_col_name and sd.distinct_values:
            for value in sd.distinct_values:
                slice_table_names.append(
                    _get_slice_table_name(table_name, sd_col_name, value)
                )

    drift_summaries: list[Any] = []
    if slice_table_names:
        drift_summaries = list(
            session.execute(
                select(ColumnDriftSummary).where(
                    ColumnDriftSummary.slice_table_name.in_(slice_table_names)
                )
            )
            .scalars()
            .all()
        )

    # Build temporal_drift from drift summaries
    temporal_drift: list[dict[str, Any]] = []
    for ds in drift_summaries:
        if ds.max_js_divergence > 0:
            evidence = ds.drift_evidence_json or {}
            change_points = evidence.get("change_points", [])
            temporal_drift.append(
                {
                    "column_name": ds.column_name,
                    "js_divergence": ds.max_js_divergence,
                    "has_significant_drift": ds.periods_with_drift > 0,
                    "has_category_changes": bool(
                        evidence.get("emerged_categories")
                        or evidence.get("vanished_categories")
                    ),
                    "change_points": change_points,
                }
            )

    # Load temporal analyses
    temporal_columns: dict[str, dict[str, Any]] = {}
    if slice_table_names:
        from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis

        period_analyses = list(
            session.execute(
                select(TemporalSliceAnalysis).where(
                    TemporalSliceAnalysis.slice_table_name.in_(slice_table_names)
                )
            )
            .scalars()
            .all()
        )

        for ta in period_analyses:
            col_name = ta.time_column
            if col_name not in temporal_columns:
                temporal_columns[col_name] = {
                    "is_interesting": False,
                    "reasons": [],
                    "coverage_ratio": ta.coverage_ratio,
                    "last_day_ratio": ta.last_day_ratio,
                    "is_volume_anomaly": bool(ta.is_volume_anomaly),
                }
            if (
                (ta.coverage_ratio is not None and ta.coverage_ratio < 0.5)
                or (ta.last_day_ratio is not None and ta.last_day_ratio > 1.5)
                or ta.is_volume_anomaly
            ):
                temporal_columns[col_name]["is_interesting"] = True
                if ta.coverage_ratio is not None and ta.coverage_ratio < 0.5:
                    temporal_columns[col_name]["reasons"].append("low_coverage")
                if ta.last_day_ratio is not None and ta.last_day_ratio > 1.5:
                    temporal_columns[col_name]["reasons"].append("period_end_spike")
                if ta.is_volume_anomaly:
                    temporal_columns[col_name]["reasons"].append("volume_anomaly")

    return {
        "slice_variance": {
            "columns": columns_data,
            "slice_data": slice_data,
            "temporal_columns": temporal_columns,
            "temporal_drift": temporal_drift,
        },
        "drift_summaries": drift_summaries,
    }


def _resolve_column_target(
    session: Session,
    target: str,
) -> tuple[str, str, str, str] | None:
    """Parse target string and resolve to (table_id, column_id, table_name, column_name).

    Supports formats:
    - "column:table_name.column_name"
    - "table_name.column_name"

    Returns None if target cannot be resolved.
    """
    from dataraum.storage import Column, Table

    # Parse target
    ref = target.split(":", 1)[1] if ":" in target else target
    parts = ref.split(".", 1)
    if len(parts) != 2:
        return None

    table_name, column_name = parts

    table = session.execute(
        select(Table).where(Table.table_name == table_name, Table.layer == "typed")
    ).scalar_one_or_none()
    if not table:
        return None

    column = session.execute(
        select(Column).where(
            Column.table_id == table.table_id,
            Column.column_name == column_name,
        )
    ).scalar_one_or_none()
    if not column:
        return None

    return table.table_id, column.column_id, table_name, column_name


def _run_detectors(
    target: str,
    context: DetectorContext,
    detectors: list[Any],
) -> Snapshot:
    """Run a list of detectors against a context and return a Snapshot."""
    scores: dict[str, float] = {}
    detectors_run: list[str] = []
    all_objects: list[EntropyObject] = []

    for detector in detectors:
        if not detector.can_run(context):
            continue
        try:
            objects: list[EntropyObject] = detector.detect(context)
            detectors_run.append(detector.detector_id)
            all_objects.extend(objects)
            for obj in objects:
                scores[obj.sub_dimension] = obj.score
        except Exception:
            logger.warning(
                f"Hard detector {detector.detector_id} failed on {target}",
                exc_info=True,
            )

    return Snapshot(scores=scores, detectors_run=detectors_run, objects=tuple(all_objects))


def take_snapshot(
    target: str,
    session: Session,
    duckdb_conn: Any = None,
    dimensions: list[str] | None = None,
) -> Snapshot:
    """Run detectors on a target and return canonical scores.

    Dispatches on target prefix:
    - "table:" → table-scoped detectors (scope="table")
    - "column:" or default → column-scoped detectors (scope="column")

    Args:
        target: Target reference (e.g., "column:orders.amount" or "table:orders")
        session: SQLAlchemy session for loading analysis data
        duckdb_conn: DuckDB connection (unused currently, reserved for future)
        dimensions: Optional filter — only run detectors for these sub_dimensions

    Returns:
        Snapshot with scores from all applicable detectors
    """
    registry = get_default_registry()
    is_table_target = target.startswith("table:")

    if is_table_target:
        resolved = _resolve_table_target(session, target)
        if resolved is None:
            logger.warning(f"Cannot resolve table target for snapshot: {target}")
            return Snapshot(scores={}, detectors_run=[])

        table_id, table_name = resolved
        analysis_results = load_table_analysis(session, table_id, table_name)

        context = DetectorContext(
            table_id=table_id,
            table_name=table_name,
            analysis_results=analysis_results,
        )

        detectors = [d for d in registry.get_all_detectors() if d.scope == "table"]
    else:
        resolved_col = _resolve_column_target(session, target)
        if resolved_col is None:
            logger.warning(f"Cannot resolve target for snapshot: {target}")
            return Snapshot(scores={}, detectors_run=[])

        table_id, column_id, table_name, column_name = resolved_col
        analysis_results = load_column_analysis(session, table_id, column_id, table_name)

        context = DetectorContext(
            table_id=table_id,
            column_id=column_id,
            table_name=table_name,
            column_name=column_name,
            analysis_results=analysis_results,
        )

        detectors = [d for d in registry.get_all_detectors() if d.scope == "column"]

    # Filter by dimensions if specified
    if dimensions:
        dim_set = set(dimensions)
        detectors = [d for d in detectors if d.sub_dimension in dim_set]

    return _run_detectors(target, context, detectors)
