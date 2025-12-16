"""Cross-table correlation and multicollinearity analysis.

Computes correlations and multicollinearity across joined tables using
the relationships infrastructure. Called 2x:
1. Before semantic agent: on relationship candidates → context for LLM
2. After semantic agent: on confirmed relationships → context for quality agents
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.algorithms.multicollinearity import (
    compute_multicollinearity,
)
from dataraum_context.analysis.relationships.db_models import (
    CrossTableMulticollinearityMetrics,
)
from dataraum_context.analysis.relationships.models import (
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    EnrichedRelationship,
    SingleRelationshipJoin,
)
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table


class ColumnMetadata:
    """Metadata for a numeric column in cross-table analysis."""

    def __init__(
        self,
        column_id: str,
        table_id: str,
        table_name: str,
        column_name: str,
    ):
        self.column_id = column_id
        self.table_id = table_id
        self.table_name = table_name
        self.column_name = column_name
        self.qualified_name = f"{table_name}.{column_name}"


async def analyze_cross_table_correlations(
    table_ids: list[str],
    relationships: list[EnrichedRelationship],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_correlation: float = 0.3,
    vdp_threshold: float = 0.5,
) -> Result[CrossTableMulticollinearityAnalysis]:
    """Compute cross-table correlations and multicollinearity.

    Args:
        table_ids: Tables to analyze
        relationships: Enriched relationships for building joins
        duckdb_conn: DuckDB connection
        session: Async database session
        min_correlation: Minimum |r| for correlation results
        vdp_threshold: VDP threshold for multicollinearity (0.5-0.8)

    Returns:
        Result containing CrossTableMulticollinearityAnalysis
    """
    if len(table_ids) < 2:
        return Result.fail("Cross-table analysis requires at least 2 tables")

    if not relationships:
        # No relationships - return empty analysis
        table_names = await _get_table_names(table_ids, session)
        return Result.ok(
            CrossTableMulticollinearityAnalysis(
                table_ids=table_ids,
                table_names=table_names,
                computed_at=datetime.now(UTC),
                total_columns_analyzed=0,
                total_relationships_used=0,
                overall_condition_index=0.0,
                overall_severity="none",
            )
        )

    # Step 1: Get numeric columns across tables
    numeric_columns = await _get_numeric_columns(table_ids, session)
    if len(numeric_columns) < 2:
        return Result.fail("Need at least 2 numeric columns for correlation analysis")

    # Step 2: Build and execute join query
    try:
        join_query = await _build_join_query(table_ids, relationships, numeric_columns, session)
        data = duckdb_conn.execute(join_query).fetchnumpy()
    except Exception as e:
        return Result.fail(f"Join query failed: {e}")

    # Step 3: Build data matrix
    if not isinstance(data, dict):
        return Result.fail("Unexpected data format from DuckDB")

    X = np.column_stack([data[col.qualified_name] for col in numeric_columns])
    X = X[~np.isnan(X).any(axis=1)]  # Remove rows with NULL

    if len(X) < 10:
        return Result.fail(f"Not enough data after join: {len(X)} rows")

    # Step 4: Compute correlation matrix
    corr_matrix = np.corrcoef(X, rowvar=False)

    # Step 5: Compute multicollinearity
    multi_result = compute_multicollinearity(corr_matrix, vdp_threshold)

    # Step 6: Enrich dependency groups with cross-table context
    enriched_groups = []
    cross_table_groups = []

    for group in multi_result.dependency_groups:
        # Map indices to (table, column) pairs
        involved_columns = [
            (numeric_columns[idx].table_name, numeric_columns[idx].column_name)
            for idx in group.involved_col_indices
        ]
        column_ids = [numeric_columns[idx].column_id for idx in group.involved_col_indices]

        # Find join paths connecting these columns
        join_paths = _find_join_paths(column_ids, relationships, numeric_columns)
        relationship_types = list({path.relationship_type for path in join_paths})

        enriched = CrossTableDependencyGroup(
            dimension=group.dimension,
            eigenvalue=group.eigenvalue,
            condition_index=group.condition_index,
            severity=group.severity,
            involved_columns=involved_columns,
            column_ids=column_ids,
            variance_proportions=group.variance_proportions,
            join_paths=join_paths,
            relationship_types=relationship_types,
        )

        enriched_groups.append(enriched)
        if enriched.num_tables > 1:
            cross_table_groups.append(enriched)

    # Step 7: Generate quality issues
    quality_issues = []
    if cross_table_groups:
        affected_tables = list(
            {table for g in cross_table_groups for table, _ in g.involved_columns}
        )
        quality_issues.append(
            {
                "issue_type": "cross_table_multicollinearity",
                "severity": "critical" if multi_result.overall_severity == "severe" else "warning",
                "description": (
                    f"{len(cross_table_groups)} dependency groups span multiple tables "
                    f"(overall CI={multi_result.overall_condition_index:.1f})"
                ),
                "affected_tables": affected_tables,
                "evidence": {
                    "condition_index": multi_result.overall_condition_index,
                    "num_groups": len(cross_table_groups),
                    "total_columns": len(numeric_columns),
                },
            }
        )

    # Build result
    table_names = await _get_table_names(table_ids, session)

    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=table_ids,
        table_names=table_names,
        computed_at=datetime.now(UTC),
        total_columns_analyzed=len(numeric_columns),
        total_relationships_used=len(relationships),
        overall_condition_index=multi_result.overall_condition_index,
        overall_severity=multi_result.overall_severity,
        dependency_groups=enriched_groups,
        cross_table_groups=cross_table_groups,
        quality_issues=quality_issues,
    )

    return Result.ok(analysis)


async def _get_table_names(table_ids: list[str], session: AsyncSession) -> list[str]:
    """Get table names for given IDs."""
    names = []
    for tid in table_ids:
        table = await session.get(Table, tid)
        if table:
            names.append(table.table_name)
    return names


async def _get_numeric_columns(
    table_ids: list[str],
    session: AsyncSession,
) -> list[ColumnMetadata]:
    """Get all numeric columns across tables."""
    stmt = (
        select(Column, Table)
        .join(Table, Column.table_id == Table.table_id)
        .where(
            Column.table_id.in_(table_ids),
            Column.resolved_type.in_(["INTEGER", "BIGINT", "DOUBLE", "DECIMAL", "FLOAT"]),
        )
    )

    result = await session.execute(stmt)
    rows = result.all()

    return [
        ColumnMetadata(
            column_id=col.column_id,
            table_id=table.table_id,
            table_name=table.table_name,
            column_name=col.column_name,
        )
        for col, table in rows
    ]


async def _build_join_query(
    table_ids: list[str],
    relationships: list[EnrichedRelationship],
    columns: list[ColumnMetadata],
    session: AsyncSession,
) -> str:
    """Build SQL query to join tables and select numeric columns."""
    # Select base table (one with most relationships)
    rel_counts = dict.fromkeys(table_ids, 0)
    for rel in relationships:
        if rel.from_table_id in rel_counts:
            rel_counts[rel.from_table_id] += 1
        if rel.to_table_id in rel_counts:
            rel_counts[rel.to_table_id] += 1

    base_table_id = max(rel_counts, key=lambda k: rel_counts[k])
    base_table = await session.get(Table, base_table_id)
    if not base_table:
        raise ValueError(f"Base table {base_table_id} not found")

    # Build aliases
    aliases = {base_table_id: "t0"}
    for idx, tid in enumerate(table_ids):
        if tid not in aliases:
            aliases[tid] = f"t{idx + 1}"

    # SELECT clause
    select_parts = [
        f'{aliases[col.table_id]}."{col.column_name}" AS "{col.qualified_name}"'
        for col in columns
        if col.table_id in aliases
    ]

    # FROM clause
    query = f"SELECT {', '.join(select_parts)} FROM {base_table.duckdb_path} AS t0"

    # JOIN clauses
    joined = {base_table_id}
    for rel in relationships:
        if rel.from_table_id in joined and rel.to_table_id not in joined:
            to_table = await session.get(Table, rel.to_table_id)
            if to_table:
                to_alias = aliases[rel.to_table_id]
                from_alias = aliases[rel.from_table_id]
                query += (
                    f" LEFT JOIN {to_table.duckdb_path} AS {to_alias} "
                    f'ON {from_alias}."{rel.from_column}" = {to_alias}."{rel.to_column}"'
                )
                joined.add(rel.to_table_id)

        elif rel.to_table_id in joined and rel.from_table_id not in joined:
            from_table = await session.get(Table, rel.from_table_id)
            if from_table:
                from_alias = aliases[rel.from_table_id]
                to_alias = aliases[rel.to_table_id]
                query += (
                    f" LEFT JOIN {from_table.duckdb_path} AS {from_alias} "
                    f'ON {from_alias}."{rel.from_column}" = {to_alias}."{rel.to_column}"'
                )
                joined.add(rel.from_table_id)

    return query


def _find_join_paths(
    column_ids: list[str],
    relationships: list[EnrichedRelationship],
    columns: list[ColumnMetadata],
) -> list[SingleRelationshipJoin]:
    """Find join paths connecting columns in a dependency group."""
    col_to_table = {col.column_id: col.table_id for col in columns}
    tables_in_group = {col_to_table.get(cid) for cid in column_ids if cid in col_to_table}

    join_paths = []
    for rel in relationships:
        if rel.from_table_id in tables_in_group and rel.to_table_id in tables_in_group:
            join_paths.append(
                SingleRelationshipJoin(
                    from_table=rel.from_table,
                    from_column=rel.from_column,
                    to_table=rel.to_table,
                    to_column=rel.to_column,
                    relationship_id=rel.relationship_id,
                    relationship_type=rel.relationship_type,
                    cardinality=rel.cardinality,
                    confidence=rel.confidence,
                    detection_method=rel.detection_method,
                )
            )

    return join_paths


async def compute_cross_table_multicollinearity(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    store_results: bool = True,
) -> Result[CrossTableMulticollinearityAnalysis]:
    """Compute cross-table multicollinearity (convenience wrapper).

    This is a convenience wrapper that:
    1. Gathers confirmed relationships from the database
    2. Calls analyze_cross_table_correlations()
    3. Optionally stores results

    Use this for the "second call" scenario (after semantic agent confirms relationships).
    For the "first call" scenario (before semantic agent), use analyze_cross_table_correlations()
    directly with relationship candidates.

    Args:
        table_ids: Tables to analyze
        duckdb_conn: DuckDB connection
        session: Async database session
        store_results: Whether to store results to DB (default True)

    Returns:
        Result containing CrossTableMulticollinearityAnalysis
    """
    from dataraum_context.enrichment.relationships.gathering import gather_relationships

    # Step 1: Gather confirmed relationships
    relationships = await gather_relationships(table_ids, session)

    # Step 2: Run analysis
    result = await analyze_cross_table_correlations(
        table_ids=table_ids,
        relationships=relationships,
        duckdb_conn=duckdb_conn,
        session=session,
    )

    if not result.success:
        return result

    # Step 3: Store results
    if store_results and result.value:
        storage_result = await store_cross_table_analysis(result.value, session)
        if not storage_result.success:
            return Result.ok(
                result.value, warnings=[f"Failed to store results: {storage_result.error}"]
            )

    return result


async def store_cross_table_analysis(
    analysis: CrossTableMulticollinearityAnalysis,
    session: AsyncSession,
) -> Result[str]:
    """Store cross-table multicollinearity analysis to database.

    Uses hybrid storage approach:
    - Structured fields: Queryable dimensions for filtering/sorting
    - JSONB field: Full CrossTableMulticollinearityAnalysis Pydantic model

    Args:
        analysis: Analysis result to store
        session: Database session

    Returns:
        Result containing metric_id
    """
    try:
        metric = CrossTableMulticollinearityMetrics(
            metric_id=str(uuid4()),
            computed_at=analysis.computed_at,
            table_ids={"table_ids": analysis.table_ids},
            overall_condition_index=analysis.overall_condition_index,
            num_cross_table_groups=len(analysis.cross_table_groups),
            num_total_groups=len(analysis.dependency_groups),
            has_severe_cross_table_dependencies=(analysis.overall_severity == "severe"),
            total_columns_analyzed=analysis.total_columns_analyzed,
            total_relationships_used=analysis.total_relationships_used,
            analysis_data=analysis.model_dump(mode="json"),
        )

        session.add(metric)
        await session.commit()

        return Result.ok(metric.metric_id)

    except Exception as e:
        await session.rollback()
        return Result.fail(f"Failed to store cross-table analysis: {e}")
