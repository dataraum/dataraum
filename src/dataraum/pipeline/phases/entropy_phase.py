"""Entropy phase implementation.

Non-LLM entropy detection across all dimensions (structural, semantic, value, computational).
Runs detectors to quantify uncertainty in each column and table.

Delegates to the entropy engine library for detector execution, network
inference, and persistence.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import (
    EntropyObjectRecord,
    EntropySnapshotRecord,
)
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.cleanup import exec_delete
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


# TODO: focus prioritization on actions that impact downstream context generation and LLM performance - e.g. structural issues that cause RI failures, semantic issues that cause misinterpretation, value issues that cause parsing failures, etc.
@analysis_phase
class EntropyPhase(BasePhase):
    """Entropy detection phase.

    Runs entropy detectors across all dimensions to quantify uncertainty
    in data. Produces entropy profiles for each column and table.

    Requires: typing, column_eligibility, semantic, relationships, slice_analysis.
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
            "slice_analysis",
        ]

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        count = exec_delete(
            session,
            delete(EntropyObjectRecord).where(EntropyObjectRecord.source_id == source_id),
        )
        count += exec_delete(
            session,
            delete(EntropySnapshotRecord).where(EntropySnapshotRecord.source_id == source_id),
        )
        return count

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
        """Run entropy detection on all columns and tables."""
        from dataraum.entropy.detectors.base import get_default_registry
        from dataraum.entropy.engine import (
            build_network_context,
            compute_dimension_scores,
            create_snapshot,
            persist_records,
            run_detectors,
        )

        registry = get_default_registry()
        all_detectors = registry.get_all_detectors()
        if not all_detectors:
            return PhaseResult.failed(
                "No entropy detectors registered. Cannot run entropy detection."
            )

        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = list(result.scalars().all())

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Load all columns
        columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = list(ctx.session.execute(columns_stmt).scalars().all())

        if not all_columns:
            return PhaseResult.failed("No columns found in typed tables.")

        # Run detectors via engine library
        detector_results = run_detectors(
            session=ctx.session,
            source_id=ctx.source_id,
            typed_tables=typed_tables,
            columns=all_columns,
        )

        # Persist records
        persist_records(ctx.session, detector_results.records)

        # Build network context from in-memory domain objects
        network_ctx = build_network_context(detector_results.domain_objects)

        # Create and persist snapshot
        snapshot_record = create_snapshot(
            source_id=ctx.source_id,
            domain_objects=detector_results.domain_objects,
            network_ctx=network_ctx,
        )
        ctx.session.add(snapshot_record)

        # Compute dimension scores for gate checking
        entropy_scores = compute_dimension_scores(detector_results.domain_objects)

        total_entropy_objects = len(detector_results.records)
        high_entropy_count = network_ctx.columns_blocked + network_ctx.columns_investigate
        critical_entropy_count = network_ctx.columns_blocked

        return PhaseResult.success(
            outputs={
                "entropy_profiles": detector_results.tables_processed,
                "entropy_objects": total_entropy_objects,
                "overall_readiness": network_ctx.overall_readiness,
                "high_entropy_columns": high_entropy_count,
                "critical_entropy_columns": critical_entropy_count,
                "entropy_scores": entropy_scores,
            },
            records_processed=len(all_columns),
            records_created=total_entropy_objects + 1,
            summary=(
                f"{network_ctx.overall_readiness} readiness, "
                f"{critical_entropy_count} critical columns"
            ),
        )
