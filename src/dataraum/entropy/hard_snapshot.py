"""Hard snapshot — run hard detectors on a target and return canonical scores.

Provides the core trust mechanism for entropy gates:
- `take_hard_snapshot()`: measure hard entropy for a column/table
- `load_column_analysis()`: load analysis results for a single column
- `HardSnapshot`: immutable result with scores, detectors run, timestamp
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
class HardSnapshot:
    """Immutable snapshot of hard detector scores for a target."""

    scores: dict[str, float]  # sub_dimension -> score
    detectors_run: list[str]  # detector_ids that were executed
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
    col = session.execute(
        select(Column).where(Column.column_id == column_id)
    ).scalar_one_or_none()
    if col:
        rels_stmt = select(Relationship).where(
            (
                (Relationship.from_column_id == column_id)
                | (Relationship.to_column_id == column_id)
            )
            & (Relationship.detection_method == "llm")
        )
        rels = session.execute(rels_stmt).scalars().all()
        if rels:
            # Resolve table names for relationship context
            table_ids_needed = {r.from_table_id for r in rels} | {r.to_table_id for r in rels}
            table_names_map: dict[str, str] = {}
            if table_ids_needed:
                tables = session.execute(
                    select(Table).where(Table.table_id.in_(table_ids_needed))
                ).scalars().all()
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

    # Correlation (derived columns)
    dc = session.execute(
        select(DerivedColumn).where(DerivedColumn.derived_column_id == column_id)
    ).scalar_one_or_none()
    if dc and col:
        analysis_results["correlation"] = {
            "derived_columns": [
                {
                    "derived_column_name": col.column_name,
                    "formula": dc.formula,
                    "match_rate": dc.match_rate,
                    "derivation_type": dc.derivation_type,
                    "source_column_ids": dc.source_column_ids or [],
                }
            ]
        }

    # Drift summaries — load via slice tables scoped to this table
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.slicing.slice_runner import _get_slice_table_name

    if col:
        col_name = col.column_name
        # Find slice tables for this table
        slice_defs = (
            session.execute(
                select(SliceDefinition).where(SliceDefinition.table_id == table_id)
            )
            .scalars()
            .all()
        )
        col_name_map = {c.column_id: c.column_name for c in [col]}
        # Load all columns in the table for name resolution
        all_cols = (
            session.execute(select(Column).where(Column.table_id == table_id))
            .scalars()
            .all()
        )
        col_name_map = {c.column_id: c.column_name for c in all_cols}

        slice_table_names: list[str] = []
        for sd in slice_defs:
            sd_col_name = col_name_map.get(sd.column_id)
            if sd_col_name and sd.distinct_values:
                for value in sd.distinct_values:
                    slice_table_names.append(_get_slice_table_name(sd_col_name, value))

        if slice_table_names:
            drift_stmt = select(ColumnDriftSummary).where(
                ColumnDriftSummary.slice_table_name.in_(slice_table_names),
                ColumnDriftSummary.column_name == col_name,
            )
            drift_summaries = session.execute(drift_stmt).scalars().all()
            if drift_summaries:
                analysis_results["drift_summaries"] = list(drift_summaries)

    return analysis_results


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


def take_hard_snapshot(
    target: str,
    session: Session,
    duckdb_conn: Any = None,
    dimensions: list[str] | None = None,
) -> HardSnapshot:
    """Run hard detectors on a target and return canonical scores.

    Args:
        target: Target reference (e.g., "column:orders.amount")
        session: SQLAlchemy session for loading analysis data
        duckdb_conn: DuckDB connection (unused currently, reserved for future)
        dimensions: Optional filter — only run detectors for these sub_dimensions

    Returns:
        HardSnapshot with scores from all applicable hard detectors
    """
    resolved = _resolve_column_target(session, target)
    if resolved is None:
        logger.warning(f"Cannot resolve target for hard snapshot: {target}")
        return HardSnapshot(scores={}, detectors_run=[])

    table_id, column_id, table_name, column_name = resolved

    # Load analysis data
    analysis_results = load_column_analysis(session, table_id, column_id, table_name)

    # Build detector context
    context = DetectorContext(
        table_id=table_id,
        column_id=column_id,
        table_name=table_name,
        column_name=column_name,
        analysis_results=analysis_results,
    )

    # Get hard detectors
    registry = get_default_registry()
    hard_detectors = registry.get_hard_detectors()

    # Filter by dimensions if specified
    if dimensions:
        dim_set = set(dimensions)
        hard_detectors = [d for d in hard_detectors if d.sub_dimension in dim_set]

    # Run each detector
    scores: dict[str, float] = {}
    detectors_run: list[str] = []

    for detector in hard_detectors:
        if not detector.can_run(context):
            continue
        try:
            objects: list[EntropyObject] = detector.detect(context)
            detectors_run.append(detector.detector_id)
            for obj in objects:
                scores[obj.sub_dimension] = obj.score
        except Exception:
            logger.warning(
                f"Hard detector {detector.detector_id} failed on {target}",
                exc_info=True,
            )

    return HardSnapshot(
        scores=scores,
        detectors_run=detectors_run,
    )
