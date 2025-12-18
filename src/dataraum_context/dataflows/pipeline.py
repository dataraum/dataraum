"""Complete data processing pipeline from CSV to enriched metadata with quality assessment.

Consolidates staging, profiling, enrichment, and quality assessment into one unified pipeline.
All stages are mandatory and run sequentially.

Pipeline stages:
1. Staging: Load CSV files as VARCHAR (preserve raw values)
2. Schema Profiling: Pattern detection, type candidates
3. Type Resolution: Create typed tables, quarantine failed casts
4. Statistics Profiling: Column stats, correlations
5. Semantic Enrichment: LLM analysis (CRITICAL - fails pipeline if fails)
6. Topology Enrichment: TDA-based FK detection
7. Temporal Enrichment: Time series analysis (basic - gaps, completeness)
8. Cross-table Analysis: Multicollinearity (multi-table datasets only)
9. Quality Assessment:
   - Statistical quality (Benford, outliers)
   - Topological quality (cycles, Betti numbers)
   - Temporal quality (seasonality, trends - advanced)
   - Domain quality (financial, if detected)
   - Quality context assembly (issues, flags, metrics for LLM/API consumption)

All metadata is stored in the database. Results contain only health information.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import duckdb
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation import analyze_correlations

# NOTE: Cross-table multicollinearity disabled - being replaced with per-relationship evaluation
# from dataraum_context.analysis.correlation import compute_cross_table_multicollinearity
from dataraum_context.analysis.statistics import assess_statistical_quality, profile_statistics
from dataraum_context.analysis.temporal import (
    profile_temporal,
)
from dataraum_context.analysis.typing import infer_type_candidates, resolve_types
from dataraum_context.core.models import SourceConfig
from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.agent import SemanticAgent
from dataraum_context.enrichment.db_models import (
    Relationship,
    SemanticAnnotation,
)
from dataraum_context.enrichment.semantic import enrich_semantic
from dataraum_context.enrichment.topology import enrich_topology
from dataraum_context.quality.context import format_dataset_quality_context
from dataraum_context.quality.domains.financial import analyze_financial_quality
from dataraum_context.quality.topological import analyze_topological_quality
from dataraum_context.sources.csv import CSVLoader
from dataraum_context.storage import Column, Table

# =============================================================================
# Result Models (Simplified - Health Info Only)
# =============================================================================


@dataclass
class TablePipelineHealth:
    """Health information for a single table pipeline execution.

    Contains only summary statistics and completion flags.
    Full metadata is stored in the database.
    """

    table_id: str
    table_name: str
    raw_table_name: str
    typed_table_name: str | None = None
    quarantine_table_name: str | None = None

    # Stage completion flags
    staging_completed: bool = False
    schema_profiling_completed: bool = False
    type_resolution_completed: bool = False
    statistics_profiling_completed: bool = False
    semantic_enrichment_completed: bool = False
    topology_enrichment_completed: bool = False
    temporal_enrichment_completed: bool = False

    # Quality assessment completion flags (Phase 4 - NEW)
    statistical_quality_completed: bool = False
    topological_quality_completed: bool = False
    temporal_quality_completed: bool = False
    domain_quality_completed: bool = False
    quality_synthesis_completed: bool = False

    # Counts (summary stats)
    row_count: int = 0
    column_count: int = 0
    semantic_annotation_count: int = 0
    relationship_count: int = 0

    # Quality counts (Phase 4 - context-based)
    quality_issue_count: int = 0

    # Error tracking
    error: str | None = None


@dataclass
class PipelineResult:
    """Result from complete pipeline.

    Contains only health information and IDs.
    Full metadata is stored in the database and can be retrieved via API.
    """

    source_id: str
    source_name: str
    table_health: list[TablePipelineHealth] = field(default_factory=list)

    # Cross-table analysis health
    cross_table_completed: bool = False
    cross_table_column_count: int = 0
    cross_table_relationship_count: int = 0

    # Overall health
    success: bool = True
    warnings: list[str] = field(default_factory=list)

    @property
    def table_count(self) -> int:
        """Total number of tables processed."""
        return len(self.table_health)

    @property
    def successful_tables(self) -> int:
        """Number of tables that completed without errors."""
        return sum(1 for t in self.table_health if t.error is None)


# =============================================================================
# Helper Functions
# =============================================================================


async def _get_typed_table_id(typed_table_name: str, session: AsyncSession) -> str | None:
    """Get the table ID for a typed table by DuckDB path.

    Args:
        typed_table_name: DuckDB path for the typed table
        session: Database session

    Returns:
        Table ID if found, None otherwise
    """
    stmt = select(Table).where(Table.duckdb_path == typed_table_name)
    result = await session.execute(stmt)
    table = result.scalar_one_or_none()
    return table.table_id if table else None


async def _count_annotations(session: AsyncSession, table_id: str) -> int:
    """Count semantic annotations for a table.

    Args:
        session: Database session
        table_id: Table ID

    Returns:
        Number of semantic annotations
    """
    stmt = (
        select(func.count())
        .select_from(SemanticAnnotation)
        .join(Column, SemanticAnnotation.column_id == Column.column_id)
        .where(Column.table_id == table_id)
    )
    result = await session.execute(stmt)
    return result.scalar() or 0


async def _count_relationships(session: AsyncSession, table_id: str) -> int:
    """Count relationships involving a table.

    Args:
        session: Database session
        table_id: Table ID

    Returns:
        Number of relationships
    """
    stmt = (
        select(func.count())
        .select_from(Relationship)
        .where((Relationship.from_table_id == table_id) | (Relationship.to_table_id == table_id))
    )
    result = await session.execute(stmt)
    return result.scalar() or 0


# =============================================================================
# Main Pipeline Function
# =============================================================================


async def run_pipeline(
    source: str | list[str],
    source_name: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    semantic_agent: SemanticAgent,
    min_confidence: float = 0.85,
    ontology: str = "financial_reporting",
) -> Result[PipelineResult]:
    """Run complete pipeline from CSV(s) through enrichment and quality assessment.

    All stages run sequentially. All metadata is stored in the database.
    Returns only health information and IDs.

    Pipeline stages:
    1. Staging: Load CSV files as VARCHAR
    2. Schema Profiling: Pattern detection, type candidates
    3. Type Resolution: Create typed tables, quarantine failed casts
    4. Statistics Profiling: Column stats, correlations
    5. Semantic Enrichment: LLM analysis (uses ontology concepts)
    6. Topology Enrichment: TDA-based FK detection
    7. Temporal Enrichment: Time series analysis (basic)
    8. Cross-table Analysis: Multicollinearity (if >1 table)
    9. Quality Assessment:
       - Statistical quality (Benford, outliers)
       - Topological quality (cycles, Betti numbers)
       - Temporal quality (seasonality, trends - advanced)
       - Domain quality (financial checks enabled by default)
       - Quality context assembly (issues, flags, metrics)

    Args:
        source: CSV file path or directory path
        source_name: Name for the data source
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        semantic_agent: Semantic agent for LLM analysis
        min_confidence: Minimum confidence for type resolution (default: 0.85)
        ontology: Ontology for semantic analysis (default: "financial_reporting")
                  Loads domain concepts from config/ontologies/{ontology}.yaml

    Returns:
        Result containing PipelineResult with health information
    """
    warnings: list[str] = []
    table_health_records: list[TablePipelineHealth] = []

    # =========================================================================
    # PHASE 1: STAGING
    # =========================================================================
    # Detect file vs directory
    loader = CSVLoader()

    if isinstance(source, str) and os.path.isfile(source):
        # Single file
        staging_result = await loader.load(
            source_config=SourceConfig(name=source_name, source_type="csv", path=source),
            duckdb_conn=duckdb_conn,
            session=session,
        )
    else:
        # Directory (or list of files - handle as directory)
        directory = source if isinstance(source, str) else source[0]
        staging_result = await loader.load_directory(
            directory_path=directory,
            source_name=source_name,
            duckdb_conn=duckdb_conn,
            session=session,
            file_pattern="*.csv",
        )

    # ✅ Metadata stored in database by CSVLoader (Source, Table, Column records)
    if not staging_result.success:
        return Result.fail(f"Staging failed: {staging_result.error}")

    staging = staging_result.unwrap()
    source_id = staging.source_id
    warnings.extend(staging_result.warnings)

    if not staging.tables:
        return Result.fail("No tables created during staging")

    # =========================================================================
    # PHASE 2: PROFILING
    # =========================================================================
    # Initialize health records
    for staged_table in staging.tables:
        health = TablePipelineHealth(
            table_id=staged_table.table_id,
            table_name=staged_table.table_name,
            raw_table_name=staged_table.raw_table_name,
            row_count=staged_table.row_count,
            column_count=staged_table.column_count,
            staging_completed=True,
        )
        table_health_records.append(health)

    # Track typed table IDs for statistics profiling
    typed_table_ids: dict[str, str] = {}

    # Stage 2.1: Type inference (type discovery)
    for health in table_health_records:
        if health.error:
            continue

        # Get the table object for type inference
        table = await session.get(Table, health.table_id)
        if not table:
            health.error = f"Table not found: {health.table_id}"
            continue

        type_inference_result = await infer_type_candidates(table, duckdb_conn, session)
        # ✅ TypeCandidate stored in database by infer_type_candidates()

        if type_inference_result.success:
            health.schema_profiling_completed = True
        else:
            health.error = f"Type inference failed: {type_inference_result.error}"
            warnings.append(
                f"Type inference failed for {health.table_name}: {type_inference_result.error}"
            )

    # Stage 2.2: Type resolution
    for health in table_health_records:
        if health.error:
            continue

        type_result = await resolve_types(health.table_id, duckdb_conn, session, min_confidence)
        # ✅ TypeDecision, typed/quarantine tables stored in database

        if type_result.success:
            health.type_resolution_completed = True
            resolution = type_result.unwrap()
            health.typed_table_name = resolution.typed_table_name
            health.quarantine_table_name = resolution.quarantine_table_name

            # Get typed table ID for statistics profiling
            typed_id = await _get_typed_table_id(resolution.typed_table_name, session)
            if typed_id:
                typed_table_ids[health.table_id] = typed_id
        else:
            health.error = f"Type resolution failed: {type_result.error}"
            warnings.append(f"Type resolution failed for {health.table_name}: {type_result.error}")

    # Stage 2.3: Statistics profiling (on typed tables)
    for raw_id, typed_id in typed_table_ids.items():
        # Find health record by raw table ID
        health = next(h for h in table_health_records if h.table_id == raw_id)
        if health.error:
            continue

        stats_result = await profile_statistics(typed_id, duckdb_conn, session)
        # ✅ StatisticalProfile stored in database by profile_statistics()

        if stats_result.success:
            health.statistics_profiling_completed = True
        else:
            # Non-critical - warn but don't fail table
            warnings.append(
                f"Statistics profiling failed for {health.table_name}: {stats_result.error}"
            )

    # Stage 2.4: Correlation analysis (on typed tables)
    for raw_id, typed_id in typed_table_ids.items():
        health = next(h for h in table_health_records if h.table_id == raw_id)
        if health.error:
            continue

        corr_result = await analyze_correlations(typed_id, duckdb_conn, session)
        # ✅ Correlations stored in database by analyze_correlations()

        if not corr_result.success:
            # Non-critical - warn but don't fail table
            warnings.append(
                f"Correlation analysis failed for {health.table_name}: {corr_result.error}"
            )

    # =========================================================================
    # PHASE 3: ENRICHMENT
    # =========================================================================
    # Only enrich tables that completed profiling successfully
    successful_table_ids = [h.table_id for h in table_health_records if h.error is None]

    if not successful_table_ids:
        return Result.fail("No tables completed profiling successfully - cannot run enrichment")

    # Stage 3.1: Semantic enrichment (CRITICAL)
    try:
        semantic_result = await enrich_semantic(
            session=session,
            agent=semantic_agent,
            table_ids=successful_table_ids,
            ontology=ontology,
        )
        # ✅ SemanticAnnotation, TableEntity, Relationship stored by enrich_semantic()

        if not semantic_result.success:
            return Result.fail(f"Semantic enrichment failed: {semantic_result.error}")

        warnings.extend(semantic_result.warnings)

        # Update health records with semantic counts
        for health in table_health_records:
            if health.table_id in successful_table_ids:
                health.semantic_enrichment_completed = True
                # Query counts from database
                health.semantic_annotation_count = await _count_annotations(
                    session, health.table_id
                )
                health.relationship_count = await _count_relationships(session, health.table_id)

    except Exception as e:
        return Result.fail(f"Semantic enrichment exception: {e}")

    # Stage 3.2: Topology enrichment (NON-CRITICAL)
    try:
        topology_result = await enrich_topology(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=successful_table_ids,
        )
        # ✅ Relationship, TopologyMetrics stored by enrich_topology()

        if topology_result.success:
            for health in table_health_records:
                if health.table_id in successful_table_ids:
                    health.topology_enrichment_completed = True
            warnings.extend(topology_result.warnings)
        else:
            warnings.append(f"Topology enrichment failed: {topology_result.error}")

    except Exception as e:
        warnings.append(f"Topology enrichment exception: {e}")

    # Stage 3.3: Temporal profiling (NON-CRITICAL)
    try:
        for table_id in successful_table_ids:
            temporal_result = await profile_temporal(
                table_id=table_id,
                duckdb_conn=duckdb_conn,
                session=session,
            )
            # ✅ TemporalColumnProfile stored by profile_temporal()

            if temporal_result.success:
                for health in table_health_records:
                    if health.table_id == table_id:
                        health.temporal_enrichment_completed = True
                warnings.extend(temporal_result.warnings)
            else:
                warnings.append(
                    f"Temporal profiling failed for {table_id}: {temporal_result.error}"
                )

    except Exception as e:
        warnings.append(f"Temporal profiling exception: {e}")

    # Stage 3.4: Cross-table multicollinearity (DISABLED)
    # NOTE: Being replaced with per-relationship evaluation
    # See analysis/relationships/evaluator.py
    cross_table_completed = False
    cross_table_column_count = 0
    cross_table_relationship_count = 0

    # =========================================================================
    # PHASE 4: QUALITY ASSESSMENT (NEW)
    # Pure measurement - NO rules evaluation, NO filtering
    # =========================================================================

    # Stage 4.1: Statistical Quality (Benford, outliers)
    for health in table_health_records:
        if health.error or not health.typed_table_name:
            continue

        # Get typed table ID
        typed_id = typed_table_ids.get(health.table_id)
        if not typed_id:
            continue

        try:
            stat_quality_result = await assess_statistical_quality(typed_id, duckdb_conn, session)
            # ✅ StatisticalQualityMetrics stored in database

            if stat_quality_result.success:
                health.statistical_quality_completed = True
            else:
                # Non-critical - warn but don't fail table
                warnings.append(
                    f"Statistical quality assessment failed for {health.table_name}: {stat_quality_result.error}"
                )

        except Exception as e:
            warnings.append(
                f"Statistical quality assessment exception for {health.table_name}: {e}"
            )

    # Stage 4.2: Topological Quality (cycles, Betti numbers)
    for health in table_health_records:
        if health.error or not health.typed_table_name:
            continue

        typed_id = typed_table_ids.get(health.table_id)
        if not typed_id:
            continue

        try:
            topo_quality_result = await analyze_topological_quality(typed_id, duckdb_conn, session)
            # ✅ TopologicalQualityMetrics stored in database

            if topo_quality_result.success:
                health.topological_quality_completed = True
            else:
                warnings.append(
                    f"Topological quality assessment failed for {health.table_name}: {topo_quality_result.error}"
                )

        except Exception as e:
            warnings.append(
                f"Topological quality assessment exception for {health.table_name}: {e}"
            )

    # Stage 4.3: Temporal Quality (seasonality, trends) - ADVANCED
    # Profile temporal columns for each typed table
    for health in table_health_records:
        if health.error or not health.typed_table_name:
            continue

        typed_id = typed_table_ids.get(health.table_id)
        if not typed_id:
            continue

        try:
            # Profile all temporal columns in the table at once
            temp_result = await profile_temporal(typed_id, duckdb_conn, session)

            if temp_result.success:
                profile_result = temp_result.unwrap()
                if profile_result.column_profiles:
                    health.temporal_quality_completed = True

        except Exception as e:
            warnings.append(f"Temporal quality assessment exception for {health.table_name}: {e}")

    # Stage 4.4: Domain Quality (financial) - CONDITIONAL
    # Only run if financial domain is detected (simple config check for now)
    if ontology == "financial_reporting":
        for health in table_health_records:
            if health.error or not health.typed_table_name:
                continue

            typed_id = typed_table_ids.get(health.table_id)
            if not typed_id:
                continue

            try:
                financial_result = await analyze_financial_quality(typed_id, duckdb_conn, session)
                # ✅ DomainQualityMetrics stored in database

                if financial_result.success:
                    health.domain_quality_completed = True
                else:
                    warnings.append(
                        f"Financial quality assessment failed for {health.table_name}: {financial_result.error}"
                    )

            except Exception as e:
                warnings.append(
                    f"Financial quality assessment exception for {health.table_name}: {e}"
                )

    # Stage 4.5: Quality Context Assembly
    # Aggregate all quality metrics into context-focused output
    if successful_table_ids:
        try:
            # Use typed table IDs for context assembly
            typed_ids_for_context = [
                typed_table_ids[raw_id]
                for raw_id in successful_table_ids
                if raw_id in typed_table_ids
            ]

            if typed_ids_for_context:
                quality_context = await format_dataset_quality_context(
                    typed_ids_for_context, session, duckdb_conn
                )
                # ✅ Quality context assembled from all pillars

                # Update health records with quality info
                for table_context in quality_context.tables:
                    # Find corresponding health record by typed table ID
                    for raw_id, typed_id in typed_table_ids.items():
                        if typed_id == table_context.table_id:
                            matching_health = next(
                                (h for h in table_health_records if h.table_id == raw_id),
                                None,
                            )
                            if matching_health:
                                matching_health.quality_synthesis_completed = True
                                # Count all issues: table-level + column-level
                                table_issue_count = len(table_context.issues)
                                column_issue_count = sum(
                                    len(col.issues) for col in table_context.columns
                                )
                                matching_health.quality_issue_count = (
                                    table_issue_count + column_issue_count
                                )
                            break

        except Exception as e:
            warnings.append(f"Quality context assembly exception: {e}")

    # =========================================================================
    # PHASE 5: RETURN HEALTH INFORMATION
    # =========================================================================
    return Result.ok(
        PipelineResult(
            source_id=source_id,
            source_name=source_name,
            table_health=table_health_records,
            cross_table_completed=cross_table_completed,
            cross_table_column_count=cross_table_column_count,
            cross_table_relationship_count=cross_table_relationship_count,
            success=True,
            warnings=warnings,
        ),
        warnings=warnings,
    )
