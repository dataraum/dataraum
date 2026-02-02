"""Entropy phase implementation.

Non-LLM entropy detection across all dimensions (structural, semantic, value, computational).
Runs detectors to quantify uncertainty in each column and table.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select

from dataraum.analysis.correlation.db_models import DerivedColumn
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.statistics.db_models import StatisticalProfile, StatisticalQualityMetrics
from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.db_models import (
    CompoundRiskRecord,
    EntropyObjectRecord,
    EntropySnapshotRecord,
)
from dataraum.entropy.processor import EntropyProcessor
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Column, Table


class EntropyPhase(BasePhase):
    """Entropy detection phase.

    Runs entropy detectors across all dimensions to quantify uncertainty
    in data. Produces entropy profiles for each column and table.

    Requires: statistics, semantic, relationships, correlations phases.
    """

    @property
    def name(self) -> str:
        return "entropy"

    @property
    def description(self) -> str:
        return "Entropy detection across all dimensions"

    @property
    def dependencies(self) -> list[str]:
        return ["typing", "statistics", "semantic", "relationships", "correlations"]

    @property
    def outputs(self) -> list[str]:
        return ["entropy_profiles", "compound_risks"]

    @property
    def is_llm_phase(self) -> bool:
        return False

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

        # Load typing info from RAW table columns
        # TypeDecision and TypeCandidate are linked to raw columns, not typed columns
        # We map them to typed columns by (table_name, column_name)
        raw_tables_stmt = select(Table).where(
            Table.layer == "raw", Table.source_id == ctx.source_id
        )
        raw_tables = (ctx.session.execute(raw_tables_stmt)).scalars().all()
        raw_table_ids = [t.table_id for t in raw_tables]

        # Build mappings from (table_name, column_name) to TypeDecision and TypeCandidate
        type_decisions_by_name: dict[tuple[str, str], TypeDecision] = {}
        type_candidates_by_name: dict[tuple[str, str], TypeCandidate] = {}

        if raw_table_ids:
            # Get raw columns
            raw_cols_stmt = select(Column).where(Column.table_id.in_(raw_table_ids))
            raw_columns = (ctx.session.execute(raw_cols_stmt)).scalars().all()
            raw_column_ids = [c.column_id for c in raw_columns]

            # Build mapping from raw column_id to (table_name, column_name)
            raw_col_to_name: dict[str, tuple[str, str]] = {}
            raw_table_names = {t.table_id: t.table_name for t in raw_tables}
            for col in raw_columns:
                table_name = raw_table_names.get(col.table_id, "")
                raw_col_to_name[col.column_id] = (table_name, col.column_name)

            if raw_column_ids:
                # Load TypeDecisions for raw columns (primary source - the final decision)
                td_stmt = select(TypeDecision).where(TypeDecision.column_id.in_(raw_column_ids))
                for td in (ctx.session.execute(td_stmt)).scalars().all():
                    name_key = raw_col_to_name.get(td.column_id)
                    if name_key:
                        type_decisions_by_name[name_key] = td

                # Load TypeCandidates for raw columns (for additional detail: pattern, unit info)
                tc_stmt = (
                    select(TypeCandidate)
                    .where(TypeCandidate.column_id.in_(raw_column_ids))
                    .order_by(TypeCandidate.confidence.desc())
                )
                for tc in (ctx.session.execute(tc_stmt)).scalars().all():
                    name_key = raw_col_to_name.get(tc.column_id)
                    if name_key and name_key not in type_candidates_by_name:
                        type_candidates_by_name[name_key] = tc

        # Create lookups from typed column_id to TypeDecision and TypeCandidate
        type_decisions: dict[str, TypeDecision] = {}
        type_candidates: dict[str, TypeCandidate] = {}
        typed_table_names = {t.table_id: t.table_name for t in typed_tables}
        for col in all_columns:
            table_name = typed_table_names.get(col.table_id, "")
            name_key = (table_name, col.column_name)
            if name_key in type_decisions_by_name:
                type_decisions[col.column_id] = type_decisions_by_name[name_key]
            if name_key in type_candidates_by_name:
                type_candidates[col.column_id] = type_candidates_by_name[name_key]

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
        total_compound_risks = 0
        table_profiles = []

        for table in typed_tables:
            table_columns = columns_by_table.get(table.table_id, [])
            if not table_columns:
                continue

            # Build column specs with analysis_results
            columns_data: list[dict[str, Any]] = []
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
                        stats_dict["quality"] = {
                            "outlier_detection": {
                                "iqr_outlier_ratio": qm.iqr_outlier_ratio or 0.0,
                                "isolation_forest_anomaly_ratio": qm.isolation_forest_anomaly_ratio,
                                "has_outliers": bool(qm.has_outliers),
                            },
                            "benford_compliant": bool(qm.benford_compliant)
                            if qm.benford_compliant is not None
                            else None,
                            "quality_data": qm.quality_data,
                        }

                    analysis_results["statistics"] = stats_dict

                # Add semantic info
                if col.column_id in semantic_annotations:
                    sa = semantic_annotations[col.column_id]
                    analysis_results["semantic"] = {
                        "semantic_role": sa.semantic_role,
                        "entity_type": sa.entity_type,
                        "business_name": sa.business_name,
                        "business_description": sa.business_description,
                    }

                # Add relationship info (already formatted as dicts with table names)
                if col.column_id in relationships_by_column:
                    analysis_results["relationships"] = relationships_by_column[col.column_id]

                # Add derived column info (for computational entropy)
                if col.column_id in derived_columns:
                    dc = derived_columns[col.column_id]
                    # Format expected by DerivedValueDetector
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

                columns_data.append(
                    {
                        "name": col.column_name,
                        "column_id": col.column_id,
                        "analysis_results": analysis_results,
                    }
                )

            # Process the table
            table_profile = processor.process_table(
                table_name=table.table_name,
                columns=columns_data,
                source_id=ctx.source_id,
                table_id=table.table_id,
            )
            table_profiles.append(table_profile)

            # Persist entropy records
            for col_profile in table_profile.columns:
                # Persist each EntropyObject with full evidence
                for entropy_obj in col_profile.entropy_objects:
                    # Serialize resolution options to dicts
                    resolution_dicts = [
                        {
                            "action": opt.action,
                            "parameters": opt.parameters,
                            "expected_entropy_reduction": opt.expected_entropy_reduction,
                            "effort": opt.effort,
                            "description": opt.description,
                            "cascade_dimensions": opt.cascade_dimensions,
                        }
                        for opt in entropy_obj.resolution_options
                    ]

                    record = EntropyObjectRecord(
                        source_id=ctx.source_id,
                        table_id=table.table_id,
                        column_id=col_profile.column_id,
                        target=entropy_obj.target,
                        layer=entropy_obj.layer,
                        dimension=entropy_obj.dimension,
                        sub_dimension=entropy_obj.sub_dimension,
                        score=entropy_obj.score,
                        confidence=entropy_obj.confidence,
                        evidence=entropy_obj.evidence,
                        resolution_options=resolution_dicts if resolution_dicts else None,
                        detector_id=entropy_obj.detector_id,
                    )
                    ctx.session.add(record)
                    total_entropy_objects += 1

                # Persist compound risks
                for risk in col_profile.compound_risks:
                    risk_record = CompoundRiskRecord(
                        source_id=ctx.source_id,
                        table_id=table.table_id,
                        target=risk.target,
                        dimensions=risk.dimensions,
                        dimension_scores=risk.dimension_scores,
                        risk_level=risk.risk_level,
                        impact=risk.impact,
                        multiplier=risk.multiplier,
                        combined_score=risk.combined_score,
                    )
                    ctx.session.add(risk_record)
                    total_compound_risks += 1

        # Compute summary statistics from table profiles
        config = get_entropy_config()
        high_threshold = config.high_entropy_threshold
        critical_threshold = config.critical_entropy_threshold

        high_entropy_count = 0
        critical_entropy_count = 0
        all_compound_risks: list[Any] = []
        all_composite_scores: list[float] = []
        all_layer_scores: dict[str, list[float]] = {
            "structural": [],
            "semantic": [],
            "value": [],
            "computational": [],
        }

        for table_profile in table_profiles:
            for col_summary in table_profile.columns:
                if col_summary.composite_score >= critical_threshold:
                    critical_entropy_count += 1
                    high_entropy_count += 1
                elif col_summary.composite_score >= high_threshold:
                    high_entropy_count += 1
                all_compound_risks.extend(col_summary.compound_risks)

                # Collect scores for averaging
                all_composite_scores.append(col_summary.composite_score)
                for layer, score in col_summary.layer_scores.items():
                    if layer in all_layer_scores:
                        all_layer_scores[layer].append(score)

        # Calculate average scores
        avg_composite = (
            sum(all_composite_scores) / len(all_composite_scores) if all_composite_scores else 0.0
        )
        avg_structural = (
            sum(all_layer_scores["structural"]) / len(all_layer_scores["structural"])
            if all_layer_scores["structural"]
            else 0.0
        )
        avg_semantic = (
            sum(all_layer_scores["semantic"]) / len(all_layer_scores["semantic"])
            if all_layer_scores["semantic"]
            else 0.0
        )
        avg_value = (
            sum(all_layer_scores["value"]) / len(all_layer_scores["value"])
            if all_layer_scores["value"]
            else 0.0
        )
        avg_computational = (
            sum(all_layer_scores["computational"]) / len(all_layer_scores["computational"])
            if all_layer_scores["computational"]
            else 0.0
        )

        # Determine overall readiness
        if critical_entropy_count > 0:
            overall_readiness = "blocked"
        elif high_entropy_count > 0:
            overall_readiness = "investigate"
        else:
            overall_readiness = "ready"

        # Create snapshot record with all averages
        snapshot = EntropySnapshotRecord(
            source_id=ctx.source_id,
            total_entropy_objects=total_entropy_objects,
            high_entropy_count=high_entropy_count,
            critical_entropy_count=critical_entropy_count,
            compound_risk_count=len(all_compound_risks),
            overall_readiness=overall_readiness,
            avg_composite_score=avg_composite,
            avg_structural_entropy=avg_structural,
            avg_semantic_entropy=avg_semantic,
            avg_value_entropy=avg_value,
            avg_computational_entropy=avg_computational,
        )
        ctx.session.add(snapshot)

        # Note: commit handled by session_scope() in orchestrator

        return PhaseResult.success(
            outputs={
                "entropy_profiles": len(table_profiles),
                "compound_risks": total_compound_risks,
                "entropy_objects": total_entropy_objects,
                "overall_readiness": overall_readiness,
                "high_entropy_columns": high_entropy_count,
                "critical_entropy_columns": critical_entropy_count,
            },
            records_processed=len(all_columns),
            records_created=total_entropy_objects + total_compound_risks + 1,
        )
