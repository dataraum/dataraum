"""Entropy phase implementation.

Non-LLM entropy detection across all dimensions (structural, semantic, value, computational).
Runs detectors to quantify uncertainty in each column and table.
"""

from __future__ import annotations

from collections.abc import Sequence
from types import ModuleType
from typing import Any

from sqlalchemy import func, select

from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import (
    EntropyObjectRecord,
    EntropySnapshotRecord,
)
from dataraum.entropy.snapshot import take_snapshot
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

        # Load all columns for counting and grouping
        columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = list(ctx.session.execute(columns_stmt).scalars().all())

        if not all_columns:
            return PhaseResult.failed("No columns found in typed tables.")

        # Group columns by table
        columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            columns_by_table.setdefault(col.table_id, []).append(col)

        # Process each table's columns via take_snapshot
        total_entropy_objects = 0
        tables_processed = 0
        all_domain_objects: list[Any] = []  # Collect EntropyObject for network inference
        all_records: list[EntropyObjectRecord] = []  # Batch for session.add_all()

        for table in typed_tables:
            table_columns = columns_by_table.get(table.table_id, [])
            if not table_columns:
                continue

            for col in table_columns:
                target = f"column:{table.table_name}.{col.column_name}"
                snapshot = take_snapshot(target=target, session=ctx.session)

                all_domain_objects.extend(snapshot.objects)

                # Persist each EntropyObject with full evidence
                for entropy_obj in snapshot.objects:
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
        logger.debug(
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
        avg_entropy = sum(target_max.values()) / len(target_max) if target_max else 0.0

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
        snapshot_record = EntropySnapshotRecord(
            source_id=ctx.source_id,
            total_entropy_objects=total_entropy_objects,
            high_entropy_count=high_entropy_count,
            critical_entropy_count=critical_entropy_count,
            overall_readiness=overall_readiness,
            avg_entropy_score=avg_entropy,
            snapshot_data=snapshot_data,
        )
        ctx.session.add(snapshot_record)

        # Note: commit handled by session_scope() in scheduler

        # Compute aggregated detector scores for gate checking and display.
        # Keys use full dimension paths (layer.dimension.sub_dimension) so they
        # match contract threshold prefix matching in the scheduler.
        scores_by_dim: dict[str, list[float]] = {}
        for obj in all_domain_objects:
            path = f"{obj.layer}.{obj.dimension}.{obj.sub_dimension}"
            scores_by_dim.setdefault(path, []).append(obj.score)

        entropy_scores = {
            dim: sum(scores) / len(scores) for dim, scores in scores_by_dim.items() if scores
        }

        return PhaseResult.success(
            outputs={
                "entropy_profiles": tables_processed,
                "entropy_objects": total_entropy_objects,
                "overall_readiness": overall_readiness,
                "high_entropy_columns": high_entropy_count,
                "critical_entropy_columns": critical_entropy_count,
                "entropy_scores": entropy_scores,
            },
            records_processed=len(all_columns),
            records_created=total_entropy_objects + 1,
            summary=f"{overall_readiness} readiness, {critical_entropy_count} critical columns",
        )


def _run_dimensional_entropy(
    ctx: PhaseContext,
    typed_tables: Sequence[Table],
) -> list[Any]:
    """Run dimensional entropy detection for cross-column patterns.

    Uses take_snapshot("table:...") for table-scoped detectors, then builds
    ColumnQualityReport-based EntropyObjects (phase-specific, not a detector).

    Args:
        ctx: Phase context with session
        typed_tables: List of typed tables to analyze

    Returns:
        List of EntropyObject instances from detection
    """
    from dataraum.entropy.models import EntropyObject, ResolutionOption

    all_entropy_objects: list[EntropyObject] = []

    logger.debug("dimensional_entropy_start", tables=len(typed_tables))

    for table in typed_tables:
        # Get column IDs for this typed table (FK-based scoping)
        table_cols_stmt = select(Column).where(Column.table_id == table.table_id)
        table_columns = list(ctx.session.execute(table_cols_stmt).scalars().all())
        table_column_ids = [c.column_id for c in table_columns]

        # If a slicing_view was registered for this fact table, profiles and reports
        # reference its columns (not the typed table's) — include those IDs too.
        sv_table = ctx.session.execute(
            select(Table).where(
                Table.source_id == ctx.source_id,
                Table.table_name == f"slicing_{table.table_name}",
                Table.layer == "slicing_view",
            )
        ).scalar_one_or_none()

        if sv_table:
            sv_cols = (
                ctx.session.execute(select(Column).where(Column.table_id == sv_table.table_id))
                .scalars()
                .all()
            )
            lookup_column_ids = [c.column_id for c in sv_cols]
            sv_col_name_to_typed_id = {c.column_name: c.column_id for c in table_columns}
        else:
            lookup_column_ids = table_column_ids
            sv_col_name_to_typed_id = None

        # Run table-scoped detectors via take_snapshot
        table_snapshot = take_snapshot(f"table:{table.table_name}", session=ctx.session)
        all_entropy_objects.extend(table_snapshot.objects)

        logger.debug(
            "dimensional_entropy_detected",
            table=table.table_name,
            entropy_objects=len(table_snapshot.objects),
        )

        # ColumnQualityReport → EntropyObject (phase-specific, not a detector)
        quality_reports_stmt = select(ColumnQualityReport).where(
            ColumnQualityReport.source_column_id.in_(lookup_column_ids)
        )
        quality_reports = list(ctx.session.execute(quality_reports_stmt).scalars().all())

        # Group reports by column
        reports_by_column: dict[str, list[Any]] = {}
        for report in quality_reports:
            col_name = report.column_name
            reports_by_column.setdefault(col_name, []).append(report)

        column_id_lookup = {c.column_name: c.column_id for c in table_columns}

        for col_name, reports in reports_by_column.items():
            avg_quality_score = sum(r.overall_quality_score for r in reports) / len(reports)
            entropy_score_val = 1.0 - avg_quality_score

            grades = [r.quality_grade for r in reports]

            all_key_findings: list[str] = []
            all_quality_issues: list[dict[str, Any]] = []
            all_recommendations: list[str] = []

            for report in reports:
                data = report.report_data or {}
                all_key_findings.extend(data.get("key_findings", []))
                all_quality_issues.extend(data.get("quality_issues", []))
                all_recommendations.extend(data.get("recommendations", []))

            # Resolve column_id: prefer typed table column, fall back to slicing_view column
            col_id = column_id_lookup.get(col_name)
            effective_table_id = table.table_id
            effective_table_name = table.table_name
            if col_id is None and sv_table and sv_col_name_to_typed_id is not None:
                col_id = reports[0].source_column_id if reports else None
                effective_table_id = sv_table.table_id
                effective_table_name = sv_table.table_name
            if col_id is None:
                continue

            column_entropy_obj = EntropyObject(
                layer="semantic",
                dimension="dimensional",
                sub_dimension="column_quality",
                target=f"column:{effective_table_name}.{col_name}",
                score=entropy_score_val,
                evidence=[
                    {
                        "source": "column_quality_report",
                        "column_id": col_id,
                        "table_id": effective_table_id,
                        "slices_analyzed": len(reports),
                        "avg_quality_score": avg_quality_score,
                        "grades": grades,
                        "key_findings": all_key_findings[:5],
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

        logger.debug(
            "column_quality_reports_processed",
            table=table.table_name,
            reports_count=len(quality_reports),
            columns_with_findings=len(reports_by_column),
        )

    logger.debug(
        "dimensional_entropy_complete",
        total_objects=len(all_entropy_objects),
    )

    return all_entropy_objects
