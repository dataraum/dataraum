"""Context assembly logic - aggregates all 5 pillars into ContextDocument.

This module is responsible for:
1. Fetching metadata from all pillar-specific storage tables
2. Converting SQLAlchemy models to Pydantic models using converters
3. Aggregating pillar-specific results into unified ContextDocument
4. Coordinating LLM-generated content (if enabled)
5. Producing ContextDocument for AI consumption

The assembly process queries data from:
- Statistical: StatisticalProfile, StatisticalQualityMetrics, CorrelationAnalysis
- Topological: TopologicalQualityMetrics, PersistentCycle
- Semantic: SemanticAnnotation, TableEntity
- Temporal: TemporalProfile, TemporalQualityMetrics
- Quality: DomainQualityMetrics, QualityRule, QualityScore
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dataraum_context.context.converters import (
    convert_statistical_profile,
    convert_statistical_quality_metrics,
    convert_topological_metrics,
)
from dataraum_context.context.models import ContextDocument
from dataraum_context.core.models.base import Result
from dataraum_context.core.models.correlation import CorrelationAnalysisResult
from dataraum_context.core.models.statistical import (
    StatisticalProfilingResult,
    StatisticalQualityResult,
)
from dataraum_context.core.models.topological import TopologicalSummary
from dataraum_context.storage.models_v2 import Source, Table
from dataraum_context.storage.models_v2.statistical_context import StatisticalProfile


async def assemble_context_document(
    source_id: str,
    ontology: str,
    session: AsyncSession,
) -> Result[ContextDocument]:
    """Assemble complete context document for a data source.

    This is the main entry point for context assembly. It:
    1. Loads source and tables from database
    2. Fetches and converts each pillar's metadata
    3. Aggregates into ContextDocument
    4. Returns ready for AI consumption

    Args:
        source_id: The source to assemble context for
        ontology: Ontology to apply (e.g., 'financial_reporting')
        session: Database session

    Returns:
        Result containing ContextDocument or error
    """
    start_time = datetime.now()

    # Load source
    result = await session.execute(
        select(Source).where(Source.source_id == source_id).options(selectinload(Source.tables))
    )
    source = result.scalar_one_or_none()

    if not source:
        return Result.fail(f"Source not found: {source_id}")

    # Filter to only typed tables (not raw or quarantine)
    typed_tables = [t for t in source.tables if t.layer == "typed"]

    if not typed_tables:
        return Result.fail(f"No typed tables found for source: {source_id}")

    # Assemble each pillar
    statistical_profiling = await _assemble_statistical_profiling(typed_tables, session)
    statistical_quality = await _assemble_statistical_quality(typed_tables, session)
    correlation_analysis = await _assemble_correlation_analysis(typed_tables, session)
    topology = None  # TODO: Implement topology assembly
    topological_summary = await _assemble_topological_summary(typed_tables, session)
    semantic = None  # TODO: Implement semantic assembly
    temporal_summary = None  # TODO: Implement temporal assembly
    quality = None  # TODO: Implement quality synthesis assembly

    # Compute duration
    duration = (datetime.now() - start_time).total_seconds()

    # Assemble final document
    document = ContextDocument(
        source_id=source_id,
        source_name=source.name,
        generated_at=datetime.now(),
        ontology=ontology,
        # Pillar 1: Statistical
        statistical_profiling=statistical_profiling,
        statistical_quality=statistical_quality,
        correlation_analysis=correlation_analysis,
        # Pillar 2: Topological
        topology=topology,
        topological_summary=topological_summary,
        # Pillar 3: Semantic
        semantic=semantic,
        # Pillar 4: Temporal
        temporal_summary=temporal_summary,
        # Pillar 5: Quality
        quality=quality,
        # Ontology content (TODO)
        relevant_metrics=[],
        domain_concepts=[],
        # LLM content (TODO)
        suggested_queries=[],
        ai_summary=None,
        key_facts=[],
        warnings=[],
        llm_features_used=[],
        assembly_duration_seconds=duration,
    )

    return Result.ok(document)


# ==================== Pillar 1: Statistical Assembly ====================


async def _assemble_statistical_profiling(
    tables: list[Table],
    session: AsyncSession,
) -> StatisticalProfilingResult | None:
    """Assemble statistical profiling results for all tables.

    Args:
        tables: List of tables to profile
        session: Database session

    Returns:
        Statistical profiling result or None if no data
    """
    all_profiles = []

    for table in tables:
        # Load columns with their statistical profiles
        await session.refresh(table, ["columns"])

        for column in table.columns:
            # Get most recent statistical profile
            result = await session.execute(
                select(StatisticalProfile)
                .where(StatisticalProfile.column_id == column.column_id)
                .order_by(StatisticalProfile.profiled_at.desc())
                .limit(1)
            )
            db_profile = result.scalar_one_or_none()

            if db_profile:
                # Convert to Pydantic
                pydantic_profile = convert_statistical_profile(db_profile)
                all_profiles.append(pydantic_profile)

    if not all_profiles:
        return None

    return StatisticalProfilingResult(
        profiles=all_profiles,
        duration_seconds=0.0,  # Not tracking profiling duration in assembly
    )


async def _assemble_statistical_quality(
    tables: list[Table],
    session: AsyncSession,
) -> StatisticalQualityResult | None:
    """Assemble statistical quality metrics for all tables.

    Args:
        tables: List of tables
        session: Database session

    Returns:
        Statistical quality result or None if no data
    """
    from dataraum_context.storage.models_v2.statistical_context import (
        StatisticalQualityMetrics,
    )

    all_quality_metrics = []

    for table in tables:
        await session.refresh(table, ["columns"])

        for column in table.columns:
            # Query quality metrics directly instead of using relationship
            # to avoid lazy loading issues
            result = await session.execute(
                select(StatisticalQualityMetrics)
                .where(StatisticalQualityMetrics.column_id == column.column_id)
                .order_by(StatisticalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            latest_metrics = result.scalar_one_or_none()

            if latest_metrics:
                # Convert to Pydantic
                pydantic_metrics = convert_statistical_quality_metrics(latest_metrics)
                all_quality_metrics.append(pydantic_metrics)

    if not all_quality_metrics:
        return None

    return StatisticalQualityResult(
        metrics=all_quality_metrics,
        duration_seconds=0.0,  # Not tracking assessment duration in assembly
    )


async def _assemble_correlation_analysis(
    tables: list[Table],
    session: AsyncSession,
) -> CorrelationAnalysisResult | None:
    """Assemble correlation analysis for all tables.

    Args:
        tables: List of tables
        session: Database session

    Returns:
        Correlation analysis result or None if no data
    """
    # TODO: Implement correlation assembly
    # This will query ColumnCorrelation, CategoricalAssociation, FunctionalDependency
    # from correlation_context.py storage models
    return None


# ==================== Pillar 2: Topological Assembly ====================


async def _assemble_topological_summary(
    tables: list[Table],
    session: AsyncSession,
) -> TopologicalSummary | None:
    """Assemble topological summary for all tables.

    Args:
        tables: List of tables
        session: Database session

    Returns:
        Topological summary or None if no data
    """
    from dataraum_context.storage.models_v2.topological_context import (
        TopologicalQualityMetrics,
    )

    topological_results = []

    for table in tables:
        # Get most recent topological metrics
        result = await session.execute(
            select(TopologicalQualityMetrics)
            .where(TopologicalQualityMetrics.table_id == table.table_id)
            .order_by(TopologicalQualityMetrics.computed_at.desc())
            .limit(1)
        )
        db_metrics = result.scalar_one_or_none()

        if db_metrics:
            # Convert to Pydantic
            pydantic_result = convert_topological_metrics(db_metrics)
            topological_results.append(pydantic_result)

    if not topological_results:
        return None

    # Aggregate into summary
    total_components = sum(r.betti_numbers.betti_0 for r in topological_results)
    total_cycles = sum(r.betti_numbers.betti_1 for r in topological_results)
    total_voids = sum(r.betti_numbers.betti_2 for r in topological_results)

    has_anomalies = any(r.anomalous_cycles for r in topological_results)
    total_orphaned = sum(r.orphaned_components for r in topological_results)

    return TopologicalSummary(
        total_components=total_components,
        total_cycles=total_cycles,
        total_voids=total_voids,
        has_structural_anomalies=has_anomalies,
        orphaned_components=total_orphaned,
        tables_analyzed=len(topological_results),
    )


# ==================== Pillar 3: Semantic Assembly ====================

# TODO: Implement semantic assembly
# This will query SemanticAnnotation, TableEntity from semantic_context.py

# ==================== Pillar 4: Temporal Assembly ====================

# TODO: Implement temporal assembly
# This will query TemporalProfile, TemporalQualityMetrics from temporal_context.py

# ==================== Pillar 5: Quality Assembly ====================

# TODO: Implement quality synthesis assembly
# This will aggregate quality from all pillars
