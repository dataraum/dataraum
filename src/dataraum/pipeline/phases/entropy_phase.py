"""Entropy phase implementation.

Non-LLM entropy detection across all dimensions (structural, semantic, value, computational).
Runs detectors to quantify uncertainty in each column and table.
"""

from __future__ import annotations

from collections.abc import Sequence
from types import ModuleType
from typing import Any

from sqlalchemy import func, select

from dataraum.analysis.correlation.db_models import DerivedColumn
from dataraum.analysis.quality_summary.db_models import ColumnQualityReport, ColumnSliceProfile
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.slice_runner import _get_slice_table_name
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics
from dataraum.analysis.temporal_slicing.db_models import ColumnDriftSummary
from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import (
    EntropyObjectRecord,
    EntropySnapshotRecord,
)
from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic import (
    DimensionalEntropyDetector,
)
from dataraum.entropy.processor import EntropyProcessor
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

logger = get_logger(__name__)


# TODO: focus prioritization on actions that impact downstream context generation and LLM performance - e.g. structural issues that cause RI failures, semantic issues that cause misinterpretation, value issues that cause parsing failures, etc.
@analysis_phase
class EntropyPhase(BasePhase):
    """Entropy detection phase.

    Runs entropy detectors across all dimensions to quantify uncertainty
    in data. Produces entropy profiles for each column and table.

    Requires: statistics, semantic, relationships, correlations, quality_summary phases.
    """

    @property
    def name(self) -> str:
        return "entropy"

    @property
    def description(self) -> str:
        return "Entropy detection across all dimensions"

    @property
    def dependencies(self) -> list[str]:
        return [
            "typing",
            "column_eligibility",
            "semantic",
            "relationships",
            "correlations",
            "quality_summary",
            "temporal_slice_analysis",
        ]

    @property
    def outputs(self) -> list[str]:
        return ["entropy_profiles"]

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.entropy import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if all columns already have entropy profiles."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Count columns in these tables
        col_count_stmt = select(func.count(Column.column_id)).where(Column.table_id.in_(table_ids))
        total_columns = (ctx.session.execute(col_count_stmt)).scalar() or 0

        if total_columns == 0:
            return "No columns found in typed tables"

        # Count distinct columns with entropy records
        # (each column has multiple EntropyObjectRecords - one per detector/dimension)
        entropy_stmt = select(func.count(func.distinct(EntropyObjectRecord.column_id))).where(
            EntropyObjectRecord.column_id.in_(
                select(Column.column_id).where(Column.table_id.in_(table_ids))
            )
        )
        columns_with_entropy = (ctx.session.execute(entropy_stmt)).scalar() or 0

        if columns_with_entropy >= total_columns:
            return "All columns already have entropy profiles"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run entropy detection on all columns."""
        # Verify detectors are registered
        from dataraum.entropy.detectors.base import get_default_registry

        registry = get_default_registry()
        all_detectors = registry.get_all_detectors()
        if not all_detectors:
            return PhaseResult.failed(
                "No entropy detectors registered. Cannot run entropy detection."
            )

        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Load all metadata needed for entropy detection
        columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = (ctx.session.execute(columns_stmt)).scalars().all()

        if not all_columns:
            return PhaseResult.failed("No columns found in typed tables.")

        column_ids = [c.column_id for c in all_columns]

        # Load statistical profiles
        stat_profiles: dict[str, StatisticalProfile] = {}
        stat_stmt = select(StatisticalProfile).where(StatisticalProfile.column_id.in_(column_ids))
        for profile in (ctx.session.execute(stat_stmt)).scalars().all():
            stat_profiles[profile.column_id] = profile

        # Load statistical quality metrics (outlier detection, Benford's Law)
        quality_metrics: dict[str, StatisticalQualityMetrics] = {}
        quality_stmt = select(StatisticalQualityMetrics).where(
            StatisticalQualityMetrics.column_id.in_(column_ids)
        )
        for qm in (ctx.session.execute(quality_stmt)).scalars().all():
            quality_metrics[qm.column_id] = qm

        # Load type decisions (copied to typed columns during resolve_types)
        type_decisions: dict[str, TypeDecision] = {}
        td_stmt = select(TypeDecision).where(TypeDecision.column_id.in_(column_ids))
        for td in (ctx.session.execute(td_stmt)).scalars().all():
            type_decisions[td.column_id] = td

        # Load type candidates (best per column by confidence)
        type_candidates: dict[str, TypeCandidate] = {}
        tc_stmt = (
            select(TypeCandidate)
            .where(TypeCandidate.column_id.in_(column_ids))
            .order_by(TypeCandidate.confidence.desc())
        )
        for tc in (ctx.session.execute(tc_stmt)).scalars().all():
            if tc.column_id not in type_candidates:
                type_candidates[tc.column_id] = tc

        # Load semantic annotations
        semantic_annotations: dict[str, SemanticAnnotation] = {}
        sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids))
        for ann in (ctx.session.execute(sem_stmt)).scalars().all():
            semantic_annotations[ann.column_id] = ann

        # Load LLM-confirmed relationships (for structural entropy)
        # Only use relationships finalized by the semantic agent, not raw candidates
        relationships_stmt = select(Relationship).where(
            (
                (Relationship.from_column_id.in_(column_ids))
                | (Relationship.to_column_id.in_(column_ids))
            )
            & (Relationship.detection_method == "llm")
        )
        relationships = (ctx.session.execute(relationships_stmt)).scalars().all()

        # Build table_id -> table_name mapping for relationship context
        table_names: dict[str, str] = {t.table_id: t.table_name for t in typed_tables}

        # Build relationships by column with table name context
        relationships_by_column: dict[str, list[dict[str, Any]]] = {}
        for rel in relationships:
            # Resolve table names
            from_table = table_names.get(rel.from_table_id, "unknown")
            to_table = table_names.get(rel.to_table_id, "unknown")

            rel_dict: dict[str, Any] = {
                "relationship_type": rel.relationship_type,
                "confidence": rel.confidence,
                "detection_method": rel.detection_method,
                "from_table": from_table,
                "to_table": to_table,
                "cardinality": rel.cardinality,
                "is_confirmed": rel.is_confirmed,
                "evidence": rel.evidence,  # Contains RI metrics, orphan count, etc.
            }

            if rel.from_column_id not in relationships_by_column:
                relationships_by_column[rel.from_column_id] = []
            relationships_by_column[rel.from_column_id].append(rel_dict)
            if rel.to_column_id not in relationships_by_column:
                relationships_by_column[rel.to_column_id] = []
            relationships_by_column[rel.to_column_id].append(rel_dict)

        # Load derived columns (for computational entropy)
        derived_stmt = select(DerivedColumn).where(DerivedColumn.derived_column_id.in_(column_ids))
        derived_columns: dict[str, DerivedColumn] = {}
        for dc in (ctx.session.execute(derived_stmt)).scalars().all():
            derived_columns[dc.derived_column_id] = dc

        # Load drift summaries for TemporalDriftDetector, scoped per typed table.
        # Key by (table_id, column_name) to prevent cross-table leakage when
        # different tables share column names (e.g. "amount", "date").
        drift_summaries_by_table_column: dict[tuple[str, str], list[ColumnDriftSummary]] = {}
        col_name_by_id = {c.column_id: c.column_name for c in all_columns}
        # Map slice_table_name -> owning typed table_id
        slice_table_to_typed: dict[str, str] = {}
        for table in typed_tables:
            sd_stmt = select(SliceDefinition).where(SliceDefinition.table_id == table.table_id)
            for sd in ctx.session.execute(sd_stmt).scalars().all():
                col_name = col_name_by_id.get(sd.column_id)
                if col_name and sd.distinct_values:
                    for value in sd.distinct_values:
                        stn = _get_slice_table_name(col_name, value)
                        slice_table_to_typed[stn] = table.table_id
        if slice_table_to_typed:
            drift_stmt = select(ColumnDriftSummary).where(
                ColumnDriftSummary.slice_table_name.in_(slice_table_to_typed.keys())
            )
            for ds in ctx.session.execute(drift_stmt).scalars().all():
                owning_table_id = slice_table_to_typed.get(ds.slice_table_name)
                if owning_table_id:
                    key = (owning_table_id, ds.column_name)
                    drift_summaries_by_table_column.setdefault(key, []).append(ds)

        # Initialize processor
        processor = EntropyProcessor()

        # Group columns by table
        columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            if col.table_id not in columns_by_table:
                columns_by_table[col.table_id] = []
            columns_by_table[col.table_id].append(col)

        # Process each table
        total_entropy_objects = 0
        tables_processed = 0
        all_domain_objects: list[Any] = []  # Collect EntropyObject for network inference
        all_records: list[EntropyObjectRecord] = []  # Batch for session.add_all()

        for table in typed_tables:
            table_columns = columns_by_table.get(table.table_id, [])
            if not table_columns:
                continue

            for col in table_columns:
                analysis_results: dict[str, Any] = {}

                # Add typing info - TypeDecision is the authoritative source,
                # TypeCandidate provides additional detail (pattern, unit info)
                if col.column_id in type_decisions:
                    td = type_decisions[col.column_id]
                    typing_dict: dict[str, Any] = {
                        "resolved_type": td.decided_type,
                        "data_type": td.decided_type,  # For backward compatibility
                        "detected_type": td.decided_type,  # Alias for type_fidelity detector
                        "decision_source": td.decision_source,
                        "decision_reason": td.decision_reason,
                    }
                    # Add detail from TypeCandidate if available (pattern, unit, etc.)
                    if col.column_id in type_candidates:
                        tc = type_candidates[col.column_id]
                        typing_dict["confidence"] = tc.confidence
                        typing_dict["parse_success_rate"] = tc.parse_success_rate or 1.0
                        typing_dict["failed_examples"] = tc.failed_examples or []
                        typing_dict["detected_pattern"] = tc.detected_pattern
                        typing_dict["pattern_match_rate"] = tc.pattern_match_rate
                        typing_dict["detected_unit"] = tc.detected_unit
                        typing_dict["unit_confidence"] = tc.unit_confidence
                    analysis_results["typing"] = typing_dict
                elif col.column_id in type_candidates:
                    # Fallback to TypeCandidate if no TypeDecision exists (shouldn't happen normally)
                    tc = type_candidates[col.column_id]
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

                # Add statistics
                if col.column_id in stat_profiles:
                    sp = stat_profiles[col.column_id]
                    stats_dict: dict[str, Any] = {
                        "null_count": sp.null_count,
                        "null_ratio": sp.null_count / sp.total_count if sp.total_count else 0,
                        "distinct_count": sp.distinct_count,
                        "cardinality_ratio": sp.cardinality_ratio,
                        "total_count": sp.total_count,
                        "profile_data": sp.profile_data,
                    }

                    # Add quality metrics (outlier detection, Benford's Law)
                    if col.column_id in quality_metrics:
                        qm = quality_metrics[col.column_id]
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

                # Add semantic info
                if col.column_id in semantic_annotations:
                    sa = semantic_annotations[col.column_id]
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

                # Add relationship info (already formatted as dicts with table names)
                if col.column_id in relationships_by_column:
                    analysis_results["relationships"] = relationships_by_column[col.column_id]

                # Add derived column info (for DerivedValueDetector)
                if col.column_id in derived_columns:
                    dc = derived_columns[col.column_id]
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

                # Add drift summaries for this column (for TemporalDriftDetector)
                col_drift = drift_summaries_by_table_column.get((table.table_id, col.column_name))
                if col_drift:
                    analysis_results["drift_summaries"] = col_drift

                # Process the column - returns list[EntropyObject] directly
                entropy_objects = processor.process_column(
                    table_name=table.table_name,
                    column_name=col.column_name,
                    analysis_results=analysis_results,
                    source_id=ctx.source_id,
                    table_id=table.table_id,
                    column_id=col.column_id,
                )

                # Keep domain objects for in-memory network inference
                all_domain_objects.extend(entropy_objects)

                # Persist each EntropyObject with full evidence
                for entropy_obj in entropy_objects:
                    resolution_dicts = [
                        {
                            "action": opt.action,
                            "parameters": opt.parameters,
                            "effort": opt.effort,
                            "description": opt.description,
                        }
                        for opt in entropy_obj.resolution_options
                    ]

                    record = EntropyObjectRecord(
                        source_id=ctx.source_id,
                        table_id=table.table_id,
                        column_id=col.column_id,
                        target=entropy_obj.target,
                        layer=entropy_obj.layer,
                        dimension=entropy_obj.dimension,
                        sub_dimension=entropy_obj.sub_dimension,
                        score=entropy_obj.score,
                        evidence=entropy_obj.evidence,
                        resolution_options=resolution_dicts if resolution_dicts else None,
                        detector_id=entropy_obj.detector_id,
                    )
                    all_records.append(record)
                    total_entropy_objects += 1

            tables_processed += 1

        # Run table-level dimensional entropy detection
        # This detects cross-column patterns from quality_summary data
        dimensional_objects = _run_dimensional_entropy(
            ctx=ctx,
            typed_tables=typed_tables,
        )
        all_domain_objects.extend(dimensional_objects)
        logger.info(
            "dimensional_entropy_results",
            objects_count=len(dimensional_objects),
        )
        for entropy_obj in dimensional_objects:
            resolution_dicts = [
                {
                    "action": opt.action,
                    "parameters": opt.parameters,
                    "effort": opt.effort,
                    "description": opt.description,
                }
                for opt in entropy_obj.resolution_options
            ]

            # Determine table_id for the record
            # For dimensional_entropy detector, extract table name and look up the ID
            record_table_id: str | None = None
            if entropy_obj.detector_id.startswith("dimensional_entropy"):
                # Target is like "table:kontobuchungen" - look up actual table_id
                if ":" in entropy_obj.target:
                    target_table_name = entropy_obj.target.split(":")[1].split(".")[0]
                    # Find matching table from typed_tables
                    for t in typed_tables:
                        if t.table_name == target_table_name:
                            record_table_id = t.table_id
                            break
            else:
                # For other detectors, the target might contain the table_id directly
                # Keep existing logic as fallback but be safe
                record_table_id = None

            record = EntropyObjectRecord(
                source_id=ctx.source_id,
                table_id=record_table_id,
                column_id=None,  # Table-level, no specific column
                target=entropy_obj.target,
                layer=entropy_obj.layer,
                dimension=entropy_obj.dimension,
                sub_dimension=entropy_obj.sub_dimension,
                score=entropy_obj.score,
                evidence=entropy_obj.evidence,
                resolution_options=resolution_dicts if resolution_dicts else None,
                detector_id=entropy_obj.detector_id,
            )
            all_records.append(record)
            total_entropy_objects += 1
            logger.debug(
                "dimensional_entropy_object_saved",
                detector_id=entropy_obj.detector_id,
                target=entropy_obj.target,
                score=entropy_obj.score,
            )

        # Batch insert all entropy records at once
        ctx.session.add_all(all_records)

        # Compute summary statistics from in-memory domain objects.
        # No DB round-trip needed — the session hasn't committed yet and
        # uses autoflush=False, so re-querying the DB would see nothing.
        from dataraum.entropy.network.model import EntropyNetwork
        from dataraum.entropy.views.network_context import _assemble_network_context

        network = EntropyNetwork()
        network_ctx = _assemble_network_context(all_domain_objects, network)

        high_entropy_count = network_ctx.columns_blocked + network_ctx.columns_investigate
        critical_entropy_count = network_ctx.columns_blocked
        overall_readiness = network_ctx.overall_readiness

        # Average entropy score: per-target max, then mean across targets.
        # This prevents table-level dimensional entropy object counts from
        # dominating the average (each target contributes its worst score).
        target_max: dict[str, float] = {}
        for obj in all_domain_objects:
            if obj.target not in target_max or obj.score > target_max[obj.target]:
                target_max[obj.target] = obj.score
        avg_entropy = (
            sum(target_max.values()) / len(target_max) if target_max else 0.0
        )

        # Serialize Bayesian network state for downstream consumers
        snapshot_data: dict[str, Any] = {
            "node_states": {
                intent.intent_name: {
                    "worst_p_high": intent.worst_p_high,
                    "mean_p_high": intent.mean_p_high,
                    "columns_blocked": intent.columns_blocked,
                    "columns_investigate": intent.columns_investigate,
                    "columns_ready": intent.columns_ready,
                    "overall_readiness": intent.overall_readiness,
                }
                for intent in network_ctx.intents
            },
            "total_columns": network_ctx.total_columns,
            "columns_blocked": network_ctx.columns_blocked,
            "columns_investigate": network_ctx.columns_investigate,
            "columns_ready": network_ctx.columns_ready,
        }

        # Create snapshot record
        snapshot = EntropySnapshotRecord(
            source_id=ctx.source_id,
            total_entropy_objects=total_entropy_objects,
            high_entropy_count=high_entropy_count,
            critical_entropy_count=critical_entropy_count,
            overall_readiness=overall_readiness,
            avg_entropy_score=avg_entropy,
            snapshot_data=snapshot_data,
        )
        ctx.session.add(snapshot)

        # Note: commit handled by session_scope() in orchestrator

        return PhaseResult.success(
            outputs={
                "entropy_profiles": tables_processed,
                "entropy_objects": total_entropy_objects,
                "overall_readiness": overall_readiness,
                "high_entropy_columns": high_entropy_count,
                "critical_entropy_columns": critical_entropy_count,
            },
            records_processed=len(all_columns),
            records_created=total_entropy_objects + 1,
        )


def _run_dimensional_entropy(
    ctx: PhaseContext,
    typed_tables: Sequence[Table],
) -> list[Any]:
    """Run dimensional entropy detection for cross-column patterns.

    Loads slice variance data from quality_summary tables and runs
    the DimensionalEntropyDetector to calculate entropy scores.

    Args:
        ctx: Phase context with session
        typed_tables: List of typed tables to analyze

    Returns:
        List of EntropyObject instances from detection
    """
    from dataraum.entropy.models import EntropyObject

    all_entropy_objects: list[EntropyObject] = []
    detector = DimensionalEntropyDetector()

    logger.info("dimensional_entropy_start", tables=len(typed_tables))

    for table in typed_tables:
        # Get column IDs for this typed table (FK-based scoping)
        table_cols_stmt = select(Column).where(Column.table_id == table.table_id)
        table_columns = list(ctx.session.execute(table_cols_stmt).scalars().all())
        table_column_ids = [c.column_id for c in table_columns]

        # Load column slice profiles by FK to typed table's columns
        profiles_stmt = select(ColumnSliceProfile).where(
            ColumnSliceProfile.source_column_id.in_(table_column_ids)
        )
        profiles = list(ctx.session.execute(profiles_stmt).scalars().all())

        logger.info(
            "dimensional_entropy_profiles_loaded",
            table=table.table_name,
            profile_count=len(profiles),
        )

        if not profiles:
            continue

        # Build slice_data structure: slice_value -> column_name -> metrics
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

            # Aggregate column-level variance metrics
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
        for _col_name, col_metrics in columns_data.items():
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

            # Mark as interesting if high variance
            if col_metrics["null_spread"] > 0.1 or col_metrics["distinct_ratio"] > 2.0:
                col_metrics["classification"] = "interesting"
                if col_metrics["null_spread"] > 0.1:
                    col_metrics["exceeded_thresholds"].append("null_spread")
                if col_metrics["distinct_ratio"] > 2.0:
                    col_metrics["exceeded_thresholds"].append("distinct_ratio")

        # Load drift summaries for slice tables belonging to this typed table
        drift_summaries: list[Any] = []
        slice_table_names: list[str] = []
        slice_def_stmt = select(SliceDefinition).where(SliceDefinition.table_id == table.table_id)
        slice_defs = list(ctx.session.execute(slice_def_stmt).scalars().all())

        if slice_defs:
            # Derive slice table names from this table's slice definitions
            col_name_by_id = {c.column_id: c.column_name for c in table_columns}
            for sd in slice_defs:
                sd_col_name = col_name_by_id.get(sd.column_id)
                if sd_col_name and sd.distinct_values:
                    for value in sd.distinct_values:
                        slice_table_names.append(_get_slice_table_name(sd_col_name, value))

            if slice_table_names:
                drift_stmt = select(ColumnDriftSummary).where(
                    ColumnDriftSummary.slice_table_name.in_(slice_table_names)
                )
                drift_summaries = list(ctx.session.execute(drift_stmt).scalars().all())

        # Build temporal_drift list from drift summaries for backward compatibility
        # with DimensionalEntropyDetector
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

        # Load period analyses (completeness + volume anomalies) for slice tables
        # and aggregate into temporal_columns for the dimensional detector
        temporal_columns: dict[str, dict[str, Any]] = {}
        if slice_table_names:
            from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis

            period_stmt = select(TemporalSliceAnalysis).where(
                TemporalSliceAnalysis.slice_table_name.in_(slice_table_names)
            )
            period_analyses = list(ctx.session.execute(period_stmt).scalars().all())

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

        # Build detector context
        context = DetectorContext(
            source_id=ctx.source_id,
            table_id=table.table_id,
            table_name=table.table_name,
            analysis_results={
                "slice_variance": {
                    "columns": columns_data,
                    "slice_data": slice_data,
                    "temporal_columns": temporal_columns,
                    "temporal_drift": temporal_drift,
                },
                "drift_summaries": drift_summaries,
            },
        )

        # Load ColumnQualityReports for this table (LLM-generated quality assessments)
        quality_reports_stmt = select(ColumnQualityReport).where(
            ColumnQualityReport.source_column_id.in_(table_column_ids)
        )
        quality_reports = list(ctx.session.execute(quality_reports_stmt).scalars().all())

        # Group reports by column
        reports_by_column: dict[str, list[Any]] = {}
        for report in quality_reports:
            col_name = report.column_name
            if col_name not in reports_by_column:
                reports_by_column[col_name] = []
            reports_by_column[col_name].append(report)

        # Build column_id lookup for this table (reuse table_columns from above)
        column_id_lookup = {c.column_name: c.column_id for c in table_columns}

        # Create EntropyObjects for each column's quality assessment
        from dataraum.entropy.models import ResolutionOption

        for col_name, reports in reports_by_column.items():
            # Calculate average quality score and entropy
            avg_quality_score = sum(r.overall_quality_score for r in reports) / len(reports)
            entropy_score_val = 1.0 - avg_quality_score  # Higher quality = lower entropy

            # Collect grades
            grades = [r.quality_grade for r in reports]

            # Aggregate findings from report_data
            all_key_findings: list[str] = []
            all_quality_issues: list[dict[str, Any]] = []
            all_recommendations: list[str] = []

            for report in reports:
                data = report.report_data or {}
                all_key_findings.extend(data.get("key_findings", []))
                all_quality_issues.extend(data.get("quality_issues", []))
                all_recommendations.extend(data.get("recommendations", []))

            # Get column_id for this column — skip if not in this typed table
            col_id = column_id_lookup.get(col_name)
            if col_id is None:
                continue

            # Create EntropyObject for this column's quality assessment
            column_entropy_obj = EntropyObject(
                layer="semantic",
                dimension="dimensional",
                sub_dimension="column_quality",
                target=f"column:{table.table_name}.{col_name}",
                score=entropy_score_val,
                evidence=[
                    {
                        "source": "column_quality_report",
                        "column_id": col_id,
                        "table_id": table.table_id,
                        "slices_analyzed": len(reports),
                        "avg_quality_score": avg_quality_score,
                        "grades": grades,
                        "key_findings": all_key_findings[:5],  # Top 5 findings
                        "quality_issues_count": len(all_quality_issues),
                        "recommendations_count": len(all_recommendations),
                    }
                ],
                resolution_options=[
                    ResolutionOption(
                        action="investigate_quality_issues",
                        parameters={
                            "column_name": col_name,
                            "key_findings": all_key_findings,
                            "quality_issues": all_quality_issues,
                            "recommendations": all_recommendations,
                        },
                        effort="medium",
                        description=f"Review {len(all_quality_issues)} quality issues and {len(all_recommendations)} recommendations for {col_name}",
                    ),
                ],
                detector_id="dimensional_entropy_column_quality",
                source_analysis_ids=[],
            )
            all_entropy_objects.append(column_entropy_obj)

        logger.info(
            "column_quality_reports_processed",
            table=table.table_name,
            reports_count=len(quality_reports),
            columns_with_findings=len(reports_by_column),
        )

        logger.info(
            "dimensional_entropy_context_built",
            table=table.table_name,
            columns_count=len(columns_data),
            slice_count=len(slice_data),
            temporal_columns=0,
        )

        # Run detector
        entropy_objects = detector.detect(context)
        all_entropy_objects.extend(entropy_objects)

        logger.info(
            "dimensional_entropy_detected",
            table=table.table_name,
            entropy_objects=len(entropy_objects),
        )

    logger.info(
        "dimensional_entropy_complete",
        total_objects=len(all_entropy_objects),
    )

    return all_entropy_objects
