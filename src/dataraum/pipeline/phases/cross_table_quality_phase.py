"""Cross-table quality phase implementation.

Analyzes data quality issues across confirmed relationships:
- Cross-table correlations (unexpected relationships between columns)
- Multicollinearity (VDP-based dependency groups, optional)

Within-table redundant/derived columns are handled by the correlations phase.
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum.analysis.correlation.processor import analyze_cross_table_quality
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.core.config import load_phase_config
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table

logger = get_logger(__name__)


@analysis_phase
class CrossTableQualityPhase(BasePhase):
    """Cross-table correlation and quality analysis phase.

    Analyzes confirmed relationships for cross-table correlations
    and optionally multicollinearity.

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
                    "multicollinearity_groups": 0,
                    "message": "No confirmed relationships to analyze",
                },
                records_processed=0,
                records_created=0,
            )

        # Configuration from phase config (fallback to file for standalone usage)
        if "min_correlation" in ctx.config:
            config = ctx.config
        else:
            config = load_phase_config("cross_table_quality")
        min_correlation = config["min_correlation"]
        redundancy_threshold = config["redundancy_threshold"]
        compute_vdp = config.get("compute_vdp", False)

        # Analyze each relationship
        total_correlations = 0
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
                compute_vdp=compute_vdp,
            )

            if not quality_result.success:
                errors.append(f"Relationship {rel.relationship_id}: {quality_result.error}")
                continue

            result_data = quality_result.unwrap()
            analyzed_count += 1
            total_correlations += len(result_data.cross_table_correlations)
            total_multicollinearity += len(result_data.dependency_groups)

        outputs: dict[str, int | list[str]] = {
            "relationships_analyzed": analyzed_count,
            "cross_table_correlations": total_correlations,
            "multicollinearity_groups": total_multicollinearity,
        }

        if errors:
            outputs["errors"] = errors

        return PhaseResult.success(
            outputs=outputs,
            records_processed=len(relationships),
            records_created=total_correlations,
            summary=f"{analyzed_count} relationships analyzed, {total_correlations} cross-table correlations",
        )
