"""Quality Context Formatters.

Formats quality metrics and issues into context-focused output for LLM/API consumption.
Replaces scoring-based views with raw metrics and issues that consumers interpret.

Usage:
    from dataraum_context.quality.context import format_dataset_quality_context

    context = await format_dataset_quality_context(
        table_ids, session, duckdb_conn, llm_service
    )
"""

import logging
from datetime import UTC, datetime
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.db_models import (
    ColumnCorrelation,
    DerivedColumn,
    FunctionalDependency,
)

# NOTE: CrossTableMulticollinearityMetrics removed - being replaced with per-relationship evaluation
from dataraum_context.analysis.statistics.db_models import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum_context.analysis.temporal import TemporalColumnProfile as TemporalQualityMetrics
from dataraum_context.analysis.typing.db_models import (
    TypeCandidate,
    TypeDecision,
)
from dataraum_context.enrichment.db_models import (
    SemanticAnnotation,
    TableEntity,
    TopologicalQualityMetrics,
)
from dataraum_context.enrichment.relationships.gathering import gather_relationships
from dataraum_context.quality.domains.db_models import DomainQualityMetrics
from dataraum_context.quality.models import (
    ColumnQualityContext,
    DatasetQualityContext,
    QualitySynthesisIssue,
    RelationshipContext,
    TableQualityContext,
)
from dataraum_context.quality.synthesis import (
    aggregate_correlation_issues,
    aggregate_domain_issues,
    aggregate_statistical_issues,
    aggregate_temporal_issues,
    aggregate_topological_issues,
)
from dataraum_context.storage import Column, Table

logger = logging.getLogger(__name__)


# ============================================================================
# Flag Generation
# ============================================================================


def _generate_column_flags(
    null_ratio: float | None,
    outlier_ratio: float | None,
    benford_compliant: bool | None,
    is_stale: bool | None,
    cardinality_ratio: float | None,
) -> list[str]:
    """Generate actionable flags from column metrics."""
    flags = []

    if null_ratio is not None and null_ratio > 0.5:
        flags.append("high_nulls")
    elif null_ratio is not None and null_ratio > 0.1:
        flags.append("moderate_nulls")

    if outlier_ratio is not None and outlier_ratio > 0.1:
        flags.append("high_outliers")
    elif outlier_ratio is not None and outlier_ratio > 0.05:
        flags.append("moderate_outliers")

    if benford_compliant is False:
        flags.append("benford_violation")

    if is_stale is True:
        flags.append("stale_data")

    if cardinality_ratio is not None:
        if cardinality_ratio > 0.99:
            flags.append("near_unique")
        elif cardinality_ratio < 0.01:
            flags.append("low_cardinality")

    return flags


def _generate_table_flags(
    betti_0: int | None,
    orphaned_components: int | None,
    anomaly_count: int,
    issue_count: int,
) -> list[str]:
    """Generate actionable flags from table metrics."""
    flags = []

    if betti_0 is not None and betti_0 > 1:
        flags.append("fragmented")

    if orphaned_components is not None and orphaned_components > 0:
        flags.append("has_orphaned_components")

    if anomaly_count > 0:
        flags.append("has_anomalies")

    if issue_count > 5:
        flags.append("many_issues")

    return flags


# ============================================================================
# Column Context Formatter
# ============================================================================


async def format_column_quality_context(
    column: Column,
    table_name: str,
    session: AsyncSession,
) -> ColumnQualityContext:
    """Format quality context for a single column.

    Args:
        column: Column to format
        table_name: Parent table name
        session: Database session

    Returns:
        ColumnQualityContext with metrics, flags, and issues
    """
    # Fetch statistical profile
    stat_profile_stmt = (
        select(StatisticalProfile)
        .where(StatisticalProfile.column_id == column.column_id)
        .order_by(StatisticalProfile.profiled_at.desc())
        .limit(1)
    )
    stat_profile = (await session.execute(stat_profile_stmt)).scalar_one_or_none()

    # Fetch statistical quality
    stat_quality_stmt = (
        select(StatisticalQualityMetrics)
        .where(StatisticalQualityMetrics.column_id == column.column_id)
        .order_by(StatisticalQualityMetrics.computed_at.desc())
        .limit(1)
    )
    stat_quality = (await session.execute(stat_quality_stmt)).scalar_one_or_none()

    # Fetch temporal quality
    temp_quality_stmt = (
        select(TemporalQualityMetrics)
        .where(TemporalQualityMetrics.column_id == column.column_id)
        .order_by(TemporalQualityMetrics.profiled_at.desc())
        .limit(1)
    )
    temp_quality = (await session.execute(temp_quality_stmt)).scalar_one_or_none()

    # Fetch type decision for parse rate
    type_decision_stmt = select(TypeDecision).where(TypeDecision.column_id == column.column_id)
    type_decision = (await session.execute(type_decision_stmt)).scalar_one_or_none()

    parse_rate = None
    if type_decision:
        type_candidate_stmt = (
            select(TypeCandidate)
            .where(TypeCandidate.column_id == column.column_id)
            .where(TypeCandidate.data_type == type_decision.decided_type)
            .order_by(TypeCandidate.confidence.desc())
            .limit(1)
        )
        type_candidate = (await session.execute(type_candidate_stmt)).scalar_one_or_none()
        if type_candidate and type_candidate.parse_success_rate is not None:
            parse_rate = type_candidate.parse_success_rate

    # Fetch correlations for this column
    corr_stmt = select(ColumnCorrelation).where(
        (ColumnCorrelation.column1_id == column.column_id)
        | (ColumnCorrelation.column2_id == column.column_id)
    )
    correlations = (await session.execute(corr_stmt)).scalars().all()
    high_correlations = [
        corr
        for corr in correlations
        if (corr.pearson_r and abs(corr.pearson_r) > 0.9)
        or (corr.spearman_rho and abs(corr.spearman_rho) > 0.9)
    ]

    # Fetch FD violations
    fd_stmt = select(FunctionalDependency).where(
        FunctionalDependency.dependent_column_id == column.column_id
    )
    fd_results = (await session.execute(fd_stmt)).scalars().all()
    fd_violations = [fd for fd in fd_results if fd.violation_count and fd.violation_count > 0]

    # Fetch semantic annotation
    semantic_stmt = select(SemanticAnnotation).where(
        SemanticAnnotation.column_id == column.column_id
    )
    semantic_annotation = (await session.execute(semantic_stmt)).scalar_one_or_none()

    # Fetch derived column info (if this column is derived from others)
    derived_stmt = select(DerivedColumn).where(DerivedColumn.derived_column_id == column.column_id)
    derived_results = (await session.execute(derived_stmt)).scalars().all()

    derived_from: list[dict[str, Any]] | None = None
    if derived_results:
        derived_from = [
            {
                "derivation_type": d.derivation_type,
                "formula": d.formula,
                "match_rate": d.match_rate,
                "source_column_ids": d.source_column_ids,
            }
            for d in derived_results
        ]

    # Extract metrics
    null_ratio = stat_profile.null_ratio if stat_profile else None
    cardinality_ratio = stat_profile.cardinality_ratio if stat_profile else None
    outlier_ratio = None
    if stat_quality:
        outlier_ratio = (
            stat_quality.iqr_outlier_ratio or stat_quality.isolation_forest_anomaly_ratio
        )
    benford_compliant = stat_quality.benford_compliant if stat_quality else None

    # Temporal metrics
    is_stale = temp_quality.is_stale if temp_quality else None
    freshness_days = None
    has_seasonality = None
    has_trend = None
    detected_granularity = None
    completeness_ratio = None

    if temp_quality:
        # Extract from structured fields
        detected_granularity = temp_quality.detected_granularity
        completeness_ratio = temp_quality.completeness_ratio

        # Extract from JSONB profile_data
        if temp_quality.profile_data:
            freshness_days = temp_quality.profile_data.get("data_freshness_days")
            seasonality = temp_quality.profile_data.get("seasonality")
            if seasonality:
                has_seasonality = seasonality.get("has_seasonality")
            trend = temp_quality.profile_data.get("trend")
            if trend:
                has_trend = trend.get("has_trend")

    # Extract semantic context
    semantic_role = semantic_annotation.semantic_role if semantic_annotation else None
    column_entity_type = semantic_annotation.entity_type if semantic_annotation else None
    business_name = semantic_annotation.business_name if semantic_annotation else None

    # Generate flags
    flags = _generate_column_flags(
        null_ratio, outlier_ratio, benford_compliant, is_stale, cardinality_ratio
    )

    # Aggregate issues
    issues: list[QualitySynthesisIssue] = []
    issues.extend(aggregate_statistical_issues(stat_quality, column.column_id, column.column_name))
    issues.extend(aggregate_temporal_issues(temp_quality, column.column_id, column.column_name))
    issues.extend(
        aggregate_correlation_issues(
            column.column_id, column.column_name, high_correlations, fd_violations
        )
    )

    return ColumnQualityContext(
        column_id=column.column_id,
        column_name=column.column_name,
        table_id=column.table_id,
        table_name=table_name,
        null_ratio=null_ratio,
        cardinality_ratio=cardinality_ratio,
        outlier_ratio=outlier_ratio,
        parse_success_rate=parse_rate,
        is_stale=is_stale,
        data_freshness_days=freshness_days,
        has_seasonality=has_seasonality,
        has_trend=has_trend,
        detected_granularity=detected_granularity,
        completeness_ratio=completeness_ratio,
        benford_compliant=benford_compliant,
        semantic_role=semantic_role,
        entity_type=column_entity_type,
        business_name=business_name,
        derived_from=derived_from,
        flags=flags,
        issues=issues,
        filter_hints=[],  # Populated by LLM later
    )


# ============================================================================
# Table Context Formatter
# ============================================================================


async def format_table_quality_context(
    table_id: str,
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
) -> TableQualityContext | None:
    """Format quality context for a table.

    Args:
        table_id: Table ID
        session: Database session
        duckdb_conn: Optional DuckDB connection for row count

    Returns:
        TableQualityContext with column contexts and table-level issues
    """
    # Get table
    table = await session.get(Table, table_id)
    if not table:
        logger.warning(f"Table not found: {table_id}")
        return None

    # Get all columns
    columns_stmt = select(Column).where(Column.table_id == table_id)
    columns = (await session.execute(columns_stmt)).scalars().all()

    # Get row count from DuckDB if available
    row_count = None
    if duckdb_conn and table.duckdb_path:
        try:
            result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{table.duckdb_path}"').fetchone()
            if result:
                row_count = result[0]
        except Exception as e:
            logger.warning(f"Could not get row count for {table.duckdb_path}: {e}")

    # Format column contexts
    column_contexts = []
    for col in columns:
        col_context = await format_column_quality_context(col, table.table_name, session)
        column_contexts.append(col_context)

    # Fetch topological quality
    topo_stmt = (
        select(TopologicalQualityMetrics)
        .where(TopologicalQualityMetrics.table_id == table_id)
        .order_by(TopologicalQualityMetrics.computed_at.desc())
        .limit(1)
    )
    topo_quality = (await session.execute(topo_stmt)).scalar_one_or_none()

    # Fetch domain quality
    domain_stmt = (
        select(DomainQualityMetrics)
        .where(DomainQualityMetrics.table_id == table_id)
        .order_by(DomainQualityMetrics.computed_at.desc())
        .limit(1)
    )
    domain_quality = (await session.execute(domain_stmt)).scalar_one_or_none()

    # NOTE: Per-table multicollinearity has been removed.
    # Cross-table multicollinearity is computed in relationships module.

    # Fetch table entity (semantic classification)
    table_entity_stmt = select(TableEntity).where(TableEntity.table_id == table_id)
    table_entity = (await session.execute(table_entity_stmt)).scalar_one_or_none()

    detected_entity_type = table_entity.detected_entity_type if table_entity else None
    is_fact_table = table_entity.is_fact_table if table_entity else None
    is_dimension_table = table_entity.is_dimension_table if table_entity else None

    # Extract topological metrics
    betti_0 = None
    betti_1 = None
    orphaned_components = None
    if topo_quality:
        betti_0 = topo_quality.betti_0
        betti_1 = topo_quality.betti_1
        orphaned_components = topo_quality.orphaned_components

    # Aggregate table-level issues
    table_issues: list[QualitySynthesisIssue] = []
    table_issues.extend(aggregate_topological_issues(topo_quality, table_id, table.table_name))
    table_issues.extend(aggregate_domain_issues(domain_quality, table_id))

    # Count domain anomalies
    domain_anomaly_count = 0
    if domain_quality and domain_quality.violations:
        violations = domain_quality.violations
        if isinstance(violations, list):
            domain_anomaly_count = len(violations)
        elif isinstance(violations, dict):
            domain_anomaly_count = len(violations.get("violations", []))

    # Generate flags
    all_issues = table_issues + [issue for col in column_contexts for issue in col.issues]
    flags = _generate_table_flags(
        betti_0, orphaned_components, domain_anomaly_count, len(all_issues)
    )

    return TableQualityContext(
        table_id=table_id,
        table_name=table.table_name,
        row_count=row_count,
        column_count=len(columns),
        columns=column_contexts,
        issues=table_issues,
        betti_0=betti_0,
        betti_1=betti_1,
        orphaned_components=orphaned_components,
        detected_entity_type=detected_entity_type,
        is_fact_table=is_fact_table,
        is_dimension_table=is_dimension_table,
        domain_anomaly_count=domain_anomaly_count,
        fiscal_stability=None,  # Set by financial orchestrator if applicable
        multicollinearity=None,  # Per-table multicollinearity removed; use cross-table
        problematic_relationships=[],
        flags=flags,
    )


# ============================================================================
# Dataset Context Formatter
# ============================================================================


async def format_dataset_quality_context(
    table_ids: list[str],
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
) -> DatasetQualityContext:
    """Format quality context for a dataset (multiple tables).

    Args:
        table_ids: List of table IDs
        session: Database session
        duckdb_conn: Optional DuckDB connection for row counts

    Returns:
        DatasetQualityContext with all table contexts and cross-table issues
    """
    # Format table contexts
    table_contexts = []
    for table_id in table_ids:
        table_context = await format_table_quality_context(table_id, session, duckdb_conn)
        if table_context:
            table_contexts.append(table_context)

    # Gather relationships between tables
    relationships: list[RelationshipContext] = []
    if len(table_ids) > 1:
        enriched_rels = await gather_relationships(table_ids, session)
        for rel in enriched_rels:
            relationships.append(
                RelationshipContext(
                    from_table=rel.from_table,
                    from_column=rel.from_column,
                    to_table=rel.to_table,
                    to_column=rel.to_column,
                    relationship_type=rel.relationship_type.value,
                    cardinality=rel.cardinality.value if rel.cardinality else None,
                    confidence=rel.confidence,
                    detection_method=rel.detection_method,
                )
            )

    # Cross-table analysis disabled - being replaced with per-relationship evaluation
    # See analysis/relationships/evaluator.py (TODO: implement)
    cross_table_issues: list[QualitySynthesisIssue] = []
    cross_table_severity: str | None = None
    cross_table_correlation_count = 0

    # Aggregate all issues
    all_issues = (
        cross_table_issues
        + [issue for table in table_contexts for issue in table.issues]
        + [issue for table in table_contexts for col in table.columns for issue in col.issues]
    )

    # Count by severity
    issues_by_severity: dict[str, int] = {}
    for issue in all_issues:
        sev = issue.severity.value
        issues_by_severity[sev] = issues_by_severity.get(sev, 0) + 1

    # Count by dimension
    issues_by_dimension: dict[str, int] = {}
    for issue in all_issues:
        dim = issue.dimension.value
        issues_by_dimension[dim] = issues_by_dimension.get(dim, 0) + 1

    # Filter recommendations and summary (future feature)
    filter_recommendations: list[dict[str, Any]] = []
    summary: str | None = None

    return DatasetQualityContext(
        tables=table_contexts,
        relationships=relationships,
        cross_table_issues=cross_table_issues,
        cross_table_multicollinearity_severity=cross_table_severity,
        cross_table_correlation_count=cross_table_correlation_count,
        total_tables=len(table_contexts),
        total_columns=sum(t.column_count for t in table_contexts),
        total_issues=len(all_issues),
        critical_issue_count=issues_by_severity.get("critical", 0),
        issues_by_severity=issues_by_severity,
        issues_by_dimension=issues_by_dimension,
        summary=summary,
        filter_recommendations=filter_recommendations,
        computed_at=datetime.now(UTC),
    )
