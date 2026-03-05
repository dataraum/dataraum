"""Loader helpers for detector data loading.

Each helper extracts data from the DB for a specific analysis domain,
returning dict structures matching what detectors expect in
context.analysis_results. Extracted 1:1 from snapshot.load_column_analysis()
and snapshot.load_table_analysis().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def load_typing(session: Session, column_id: str) -> dict[str, Any] | None:
    """Load type decision and candidate info for a column.

    Returns dict with resolved_type, confidence, parse_success_rate, etc.
    or None if no typing data exists.
    """
    from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision

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
        return typing_dict
    elif tc:
        return {
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
    return None


def load_statistics(session: Session, column_id: str) -> dict[str, Any] | None:
    """Load statistical profile and quality metrics for a column.

    Returns dict with null_count, null_ratio, distinct_count, quality, etc.
    or None if no statistics exist.
    """
    from dataraum.analysis.statistics.db_models import StatisticalProfile
    from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics

    sp = session.execute(
        select(StatisticalProfile).where(StatisticalProfile.column_id == column_id)
    ).scalar_one_or_none()

    if not sp:
        return None

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
    return stats_dict


def load_semantic(session: Session, column_id: str) -> dict[str, Any] | None:
    """Load semantic annotation for a column.

    Returns dict with semantic_role, entity_type, business_name, etc.
    or None if no annotation exists.
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    sa = session.execute(
        select(SemanticAnnotation).where(SemanticAnnotation.column_id == column_id)
    ).scalar_one_or_none()
    if not sa:
        return None

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
    return semantic_dict


def load_relationships(
    session: Session, column_id: str, table_id: str
) -> list[dict[str, Any]] | None:
    """Load LLM-confirmed relationships involving a column.

    Returns list of relationship dicts or None if no relationships found.
    """
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Column, Table

    col = session.execute(select(Column).where(Column.column_id == column_id)).scalar_one_or_none()
    if not col:
        return None

    rels_stmt = select(Relationship).where(
        ((Relationship.from_column_id == column_id) | (Relationship.to_column_id == column_id))
        & (Relationship.detection_method == "llm")
    )
    rels = session.execute(rels_stmt).scalars().all()
    if not rels:
        return None

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

    return [
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
        for rel in rels
    ]


def load_correlation(
    session: Session, column_id: str, column_name: str
) -> dict[str, Any] | None:
    """Load derived column info for a column.

    Returns dict with derived_columns list or None if no derivations found.
    """
    from dataraum.analysis.correlation.db_models import DerivedColumn

    dcs = (
        session.execute(select(DerivedColumn).where(DerivedColumn.derived_column_id == column_id))
        .scalars()
        .all()
    )
    if not dcs:
        return None

    return {
        "derived_columns": [
            {
                "derived_column_name": column_name,
                "formula": dc.formula,
                "match_rate": dc.match_rate,
                "derivation_type": dc.derivation_type,
                "source_column_ids": dc.source_column_ids or [],
            }
            for dc in dcs
        ]
    }


def load_drift_summaries(
    session: Session,
    column_id: str,
    table_id: str,
    table_name: str,
) -> list[Any] | None:
    """Load temporal drift summaries for a column across slice tables.

    Returns list of ColumnDriftSummary ORM objects or None if none found.
    """
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.slicing.slice_runner import _get_slice_table_name
    from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
    from dataraum.storage import Column

    col = session.execute(select(Column).where(Column.column_id == column_id)).scalar_one_or_none()
    if not col:
        return None

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
        from dataraum.storage import Table

        source_table = session.get(Table, table_id)
        source_table_name = source_table.table_name if source_table else ""

    slice_table_names: list[str] = []
    for sd in slice_defs:
        sd_col_name = sd.column_name or col_name_map.get(sd.column_id)
        if sd_col_name and sd.distinct_values and source_table_name:
            for value in sd.distinct_values:
                slice_table_names.append(
                    _get_slice_table_name(source_table_name, sd_col_name, value)
                )

    if not slice_table_names:
        return None

    drift_stmt = select(ColumnDriftSummary).where(
        ColumnDriftSummary.slice_table_name.in_(slice_table_names),
        ColumnDriftSummary.column_name == col_name,
    )
    drift_summaries = session.execute(drift_stmt).scalars().all()
    return list(drift_summaries) if drift_summaries else None


def load_slice_variance(
    session: Session,
    table_id: str,
    table_name: str,
) -> dict[str, Any] | None:
    """Load table-scoped slice variance data for DimensionalEntropyDetector.

    Returns dict with slice_variance and drift_summaries keys,
    or None if no slice profiles exist.
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
        return None

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
