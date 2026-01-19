"""Relationships phase implementation.

Detects relationships between typed tables:
- Value overlap analysis (Jaccard/containment similarity)
- Join column detection
- Cardinality analysis (one-to-one, one-to-many, etc.)
- Graph topology analysis
"""

from __future__ import annotations

from sqlalchemy import func, select

from dataraum_context.analysis.relationships import detect_relationships
from dataraum_context.analysis.relationships.db_models import Relationship
from dataraum_context.pipeline.base import PhaseContext, PhaseResult
from dataraum_context.pipeline.phases.base import BasePhase
from dataraum_context.storage import Table


class RelationshipsPhase(BasePhase):
    """Relationship detection phase.

    Detects relationships between typed tables using value overlap
    and structural analysis.
    """

    @property
    def name(self) -> str:
        return "relationships"

    @property
    def description(self) -> str:
        return "Cross-table relationship detection"

    @property
    def dependencies(self) -> list[str]:
        return ["statistics"]

    @property
    def outputs(self) -> list[str]:
        return ["relationship_candidates"]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if relationships already detected for this source."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        if len(typed_tables) < 2:
            return "Need at least 2 tables to detect relationships"

        # Check if relationships already detected
        table_ids = [t.table_id for t in typed_tables]
        existing_count = (
            ctx.session.execute(
                select(func.count(Relationship.relationship_id)).where(
                    Relationship.from_table_id.in_(table_ids),
                    Relationship.detection_method == "candidate",
                )
            )
        ).scalar() or 0

        if existing_count > 0:
            return f"Already detected {existing_count} relationship candidates"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run relationship detection on typed tables."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        if len(typed_tables) < 2:
            return PhaseResult.success(
                outputs={"relationship_candidates": [], "message": "Need at least 2 tables"},
                records_processed=0,
                records_created=0,
            )

        table_ids = [t.table_id for t in typed_tables]

        # Configuration from context
        min_confidence = ctx.config.get("min_confidence", 0.3)
        sample_percent = ctx.config.get("sample_percent", 10.0)

        # Run relationship detection
        detection_result = detect_relationships(
            table_ids=table_ids,
            duckdb_conn=ctx.duckdb_conn,
            session=ctx.session,
            min_confidence=min_confidence,
            sample_percent=sample_percent,
            evaluate=True,
        )

        if not detection_result.success:
            return PhaseResult.failed(f"Relationship detection failed: {detection_result.error}")

        result_data = detection_result.unwrap()

        # Summarize findings
        candidates = result_data.candidates
        high_confidence = [
            c for c in candidates if any(jc.join_confidence >= 0.7 for jc in c.join_candidates)
        ]

        return PhaseResult.success(
            outputs={
                "relationship_candidates": [f"{c.table1} <-> {c.table2}" for c in candidates],
                "total_candidates": len(candidates),
                "high_confidence_count": len(high_confidence),
                "duration_seconds": result_data.duration_seconds,
            },
            records_processed=len(table_ids) * (len(table_ids) - 1) // 2,  # pairs analyzed
            records_created=len(candidates),
        )
