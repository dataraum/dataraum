"""Cross-table quality phase implementation.

Analyzes data quality issues across confirmed relationships:
- Cross-table correlations (unexpected relationships between columns)
- Redundant columns (r ≈ 1.0 within same table)
- Derived columns (one column computed from another)
- Multicollinearity (VDP-based dependency groups)

Note: Despite the plan labeling this as LLM, the actual analysis is
statistical (correlation + VDP). It runs AFTER semantic phase because
it requires confirmed relationships.
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum.analysis.correlation.processor import analyze_cross_table_quality
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Table


class CrossTableQualityPhase(BasePhase):
    """Cross-table correlation and quality analysis phase.

    Analyzes confirmed relationships for cross-table correlations,
    redundant columns, derived columns, and multicollinearity.

    Requires: semantic phase (for confirmed relationships).
    """

    @property
    def name(self) -> str:
        return "cross_table_quality"

    @property
    def description(self) -> str:
        return "Cross-table correlation analysis"

    @property
    def dependencies(self) -> list[str]:
        return ["semantic"]

    @property
    def outputs(self) -> list[str]:
        return ["cross_table_correlations", "multicollinearity_groups"]

    @property
    def is_llm_phase(self) -> bool:
        # This is actually a non-LLM phase - it's statistical analysis
        return False

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no confirmed relationships exist."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for confirmed relationships (LLM-confirmed or high confidence)
        rel_stmt = select(Relationship).where(
            (Relationship.from_table_id.in_(table_ids))
            & (Relationship.to_table_id.in_(table_ids))
            & ((Relationship.detection_method == "llm") | (Relationship.confidence > 0.7))
        )
        relationships = ctx.session.execute(rel_stmt).scalars().all()

        if not relationships:
            return "No confirmed relationships to analyze"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run cross-table quality analysis on confirmed relationships."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Get confirmed relationships
        rel_stmt = select(Relationship).where(
            (Relationship.from_table_id.in_(table_ids))
            & (Relationship.to_table_id.in_(table_ids))
            & ((Relationship.detection_method == "llm") | (Relationship.confidence > 0.7))
        )
        relationships = ctx.session.execute(rel_stmt).scalars().all()

        if not relationships:
            return PhaseResult.success(
                outputs={
                    "relationships_analyzed": 0,
                    "cross_table_correlations": 0,
                    "redundant_pairs": 0,
                    "derived_candidates": 0,
                    "multicollinearity_groups": 0,
                    "message": "No confirmed relationships to analyze",
                },
                records_processed=0,
                records_created=0,
            )

        # Get config parameters
        min_correlation = ctx.config.get("min_correlation", 0.3)
        redundancy_threshold = ctx.config.get("redundancy_threshold", 0.99)

        # Analyze each relationship
        total_correlations = 0
        total_redundant = 0
        total_derived = 0
        total_multicollinearity = 0
        analyzed_count = 0
        errors = []

        for rel in relationships:
            quality_result = analyze_cross_table_quality(
                relationship=rel,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                min_correlation=min_correlation,
                redundancy_threshold=redundancy_threshold,
            )

            if not quality_result.success:
                errors.append(f"Relationship {rel.relationship_id}: {quality_result.error}")
                continue

            result_data = quality_result.unwrap()
            analyzed_count += 1
            total_correlations += len(result_data.cross_table_correlations)
            total_redundant += len(result_data.redundant_columns)
            total_derived += len(result_data.derived_columns)
            total_multicollinearity += len(result_data.dependency_groups)

        outputs: dict[str, int | list[str]] = {
            "relationships_analyzed": analyzed_count,
            "cross_table_correlations": total_correlations,
            "redundant_pairs": total_redundant,
            "derived_candidates": total_derived,
            "multicollinearity_groups": total_multicollinearity,
        }

        if errors:
            outputs["errors"] = errors

        return PhaseResult.success(
            outputs=outputs,
            records_processed=len(relationships),
            records_created=total_correlations
            + total_redundant
            + total_derived
            + total_multicollinearity,
        )
