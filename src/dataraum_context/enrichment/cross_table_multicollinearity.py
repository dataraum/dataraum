"""Cross-table multicollinearity detection using unified correlation matrix.

This module implements Phase 2 of multicollinearity analysis:
- Gathers relationships from semantic + topology enrichment
- Builds unified correlation matrix across related tables
- Applies Belsley VDP methodology to detect cross-table dependencies
- Generates rich LLM context with join paths and recommendations
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import duckdb
import numpy as np
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import (
    Cardinality,
    RelationshipType,
    Result,
)
from dataraum_context.profiling.models import (
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    SingleRelationshipJoin,
)
from dataraum_context.storage.models_v2 import (
    Column,
    Table,
)
from dataraum_context.storage.models_v2 import (
    Relationship as RelationshipModel,
)

# === Helper Models ===


class EnrichedRelationship(BaseModel):
    """Relationship enriched with column and table metadata for join construction."""

    relationship_id: str
    from_table: str
    from_column: str
    from_column_id: str
    to_table: str
    to_column: str
    to_column_id: str
    from_table_id: str
    to_table_id: str
    relationship_type: RelationshipType
    cardinality: Cardinality | None
    confidence: float
    detection_method: str
    evidence: dict[str, Any]


class ColumnMetadata(BaseModel):
    """Metadata for a numeric column in correlation analysis."""

    column_id: str
    table_id: str
    table_name: str
    column_name: str
    column_name_qualified: str  # "table.column" format


class UnifiedMatrixResult(BaseModel):
    """Result of building unified correlation matrix."""

    correlation_matrix: Any  # numpy.ndarray (can't serialize directly)
    column_metadata: list[ColumnMetadata]
    num_rows: int
    num_columns: int
    join_query: str


# === Configuration ===

# Differentiated confidence thresholds by relationship type
CONFIDENCE_THRESHOLDS = {
    "foreign_key": 0.7,  # Stricter (high reliability expected)
    "semantic": 0.6,  # Medium (LLM-based, good but not perfect)
    "correlation": 0.5,  # More permissive (TDA-detected)
    "hierarchy": 0.6,  # Medium (similar to semantic)
}


# === Main Functions ===


async def gather_relationships(
    table_ids: list[str],
    session: AsyncSession,
) -> list[EnrichedRelationship]:
    """Gather and filter relationships from semantic + topology enrichment.

    Strategy:
    - Query relationships table for all combinations of input tables
    - Filter by differentiated confidence thresholds (FK: 0.7, Semantic: 0.6, Correlation: 0.5)
    - Merge/dedupe relationships from multiple sources
    - Resolve conflicts (prefer higher confidence, prefer FK over correlation)

    Args:
        table_ids: List of table IDs to analyze
        session: Async database session

    Returns:
        List of enriched relationships with metadata
    """
    # Build query for relationships between any pair of input tables
    # NOTE: We filter by confidence AFTER retrieval to apply differentiated thresholds
    stmt = (
        select(RelationshipModel)
        .where(
            (RelationshipModel.from_table_id.in_(table_ids))
            & (RelationshipModel.to_table_id.in_(table_ids))
        )
        .order_by(RelationshipModel.confidence.desc())
    )

    result = await session.execute(stmt)
    db_relationships = result.scalars().all()

    # Convert to enriched format with additional metadata
    enriched = []
    seen_pairs = set()  # (from_col_id, to_col_id) to dedupe

    for db_rel in db_relationships:
        # Apply differentiated confidence threshold
        threshold = CONFIDENCE_THRESHOLDS.get(db_rel.relationship_type, 0.5)
        if db_rel.confidence < threshold:
            continue  # Below threshold for this type

        pair = (db_rel.from_column_id, db_rel.to_column_id)
        if pair in seen_pairs:
            continue  # Skip duplicate (keep highest confidence)

        # Load column metadata for join construction
        from_col = await session.get(Column, db_rel.from_column_id)
        to_col = await session.get(Column, db_rel.to_column_id)
        from_table = await session.get(Table, db_rel.from_table_id)
        to_table = await session.get(Table, db_rel.to_table_id)

        if not all([from_col, to_col, from_table, to_table]):
            continue  # Skip if any metadata is missing

        enriched.append(
            EnrichedRelationship(
                relationship_id=db_rel.relationship_id,
                from_table=from_table.table_name,
                from_column=from_col.column_name,
                from_column_id=db_rel.from_column_id,
                from_table_id=db_rel.from_table_id,
                to_table=to_table.table_name,
                to_column=to_col.column_name,
                to_column_id=db_rel.to_column_id,
                to_table_id=db_rel.to_table_id,
                relationship_type=RelationshipType(db_rel.relationship_type),
                cardinality=Cardinality(db_rel.cardinality) if db_rel.cardinality else None,
                confidence=db_rel.confidence,
                detection_method=db_rel.detection_method,
                evidence=db_rel.evidence or {},
            )
        )

        seen_pairs.add(pair)

    return enriched


async def _get_numeric_columns_for_tables(
    table_ids: list[str], session: AsyncSession
) -> list[ColumnMetadata]:
    """Get all numeric columns across specified tables.

    Args:
        table_ids: List of table IDs
        session: Async database session

    Returns:
        List of column metadata for numeric columns
    """
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

    metadata = []
    for col, table in rows:
        metadata.append(
            ColumnMetadata(
                column_id=col.column_id,
                table_id=table.table_id,
                table_name=table.table_name,
                column_name=col.column_name,
                column_name_qualified=f"{table.table_name}.{col.column_name}",
            )
        )

    return metadata


def _needs_sampling(table: Table, threshold: int = 100000) -> bool:
    """Check if table needs sampling based on row count estimate."""
    # TODO: Add row_count tracking or query DuckDB for estimate
    # For now, conservatively don't sample
    return False


def _select_base_table(table_ids: list[str], relationships: list[EnrichedRelationship]) -> str:
    """Select base table for FROM clause (table with most relationships)."""
    rel_counts = {}
    for table_id in table_ids:
        count = sum(
            1
            for rel in relationships
            if rel.from_table_id == table_id or rel.to_table_id == table_id
        )
        rel_counts[table_id] = count

    if rel_counts:
        return max(rel_counts, key=rel_counts.get)  # type: ignore
    else:
        return table_ids[0]


async def build_unified_correlation_matrix(
    table_ids: list[str],
    relationships: list[EnrichedRelationship],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[UnifiedMatrixResult]:
    """Build normalized correlation matrix spanning all related tables."""
    # Step 1: Get all numeric columns
    numeric_columns = await _get_numeric_columns_for_tables(table_ids, session)

    if len(numeric_columns) < 2:
        return Result.fail("Need at least 2 numeric columns for correlation analysis")

    # Step 2: Build join query
    try:
        join_query = await _build_join_query(table_ids, relationships, numeric_columns, session)
    except Exception as e:
        return Result.fail(f"Failed to build join query: {e}")

    # Step 3: Execute query
    try:
        data = duckdb_conn.execute(join_query).fetchnumpy()
    except Exception as e:
        return Result.fail(f"Join query execution failed: {e}")

    # Step 4: Build matrix
    if isinstance(data, dict):
        X = np.column_stack([data[col.column_name_qualified] for col in numeric_columns])
    else:
        return Result.fail("Unexpected data format from DuckDB")

    # Remove rows with any NULL
    X = X[~np.isnan(X).any(axis=1)]

    if len(X) < 10:
        return Result.fail(f"Not enough data after removing NULLs: {len(X)} rows")

    # Step 5: Normalize (z-scores)
    from scipy.stats import zscore

    X_normalized = zscore(X, axis=0)

    # Step 6: Compute correlation matrix
    corr_matrix = np.corrcoef(X_normalized, rowvar=False)

    return Result.ok(
        UnifiedMatrixResult(
            correlation_matrix=corr_matrix,
            column_metadata=numeric_columns,
            num_rows=len(X),
            num_columns=len(numeric_columns),
            join_query=join_query,
        )
    )


async def _build_join_query(
    table_ids: list[str],
    relationships: list[EnrichedRelationship],
    numeric_columns: list[ColumnMetadata],
    session: AsyncSession,
) -> str:
    """Build DuckDB query to join tables and select numeric columns."""
    # Determine base table
    base_table_id = _select_base_table(table_ids, relationships)
    base_table = await session.get(Table, base_table_id)
    if not base_table:
        raise ValueError(f"Base table {base_table_id} not found")

    # Build table alias mapping
    table_aliases = {base_table_id: "t0"}
    for idx, tid in enumerate(table_ids):
        if tid not in table_aliases:
            table_aliases[tid] = f"t{idx + 1}"

    # Build SELECT clause
    select_parts = []
    for col in numeric_columns:
        alias = table_aliases.get(col.table_id, "t0")
        select_parts.append(f'{alias}."{col.column_name}" AS "{col.column_name_qualified}"')

    # Build FROM clause
    base_clause = (
        f"{base_table.duckdb_path} TABLESAMPLE SYSTEM(50000 ROWS)"
        if _needs_sampling(base_table)
        else base_table.duckdb_path
    )
    query = f"SELECT {', '.join(select_parts)} FROM {base_clause} AS t0"

    # Build JOIN clauses (1-hop only)
    joined_tables = {base_table_id}
    join_clauses = []

    for rel in relationships:
        if rel.from_table_id in joined_tables and rel.to_table_id not in joined_tables:
            # Join to_table
            from_alias = table_aliases[rel.from_table_id]
            to_table = await session.get(Table, rel.to_table_id)
            if not to_table:
                continue
            to_alias = table_aliases[rel.to_table_id]

            join_clauses.append(
                f"LEFT JOIN {to_table.duckdb_path} AS {to_alias} "
                f'ON {from_alias}."{rel.from_column}" = {to_alias}."{rel.to_column}"'
            )
            joined_tables.add(rel.to_table_id)

        elif rel.to_table_id in joined_tables and rel.from_table_id not in joined_tables:
            # Join from_table (reverse)
            to_alias = table_aliases[rel.to_table_id]
            from_table = await session.get(Table, rel.from_table_id)
            if not from_table:
                continue
            from_alias = table_aliases[rel.from_table_id]

            join_clauses.append(
                f"LEFT JOIN {from_table.duckdb_path} AS {from_alias} "
                f'ON {from_alias}."{rel.from_column}" = {to_alias}."{rel.to_column}"'
            )
            joined_tables.add(rel.from_table_id)

    if join_clauses:
        query += "\n" + "\n".join(join_clauses)

    return query


def _find_join_paths_for_group(
    column_ids: list[str],
    relationships: list[EnrichedRelationship],
    column_metadata: list[ColumnMetadata],
) -> list[SingleRelationshipJoin]:
    """Find join paths connecting columns in a dependency group.

    Args:
        column_ids: Column IDs in dependency group
        relationships: All relationships
        column_metadata: Metadata for all columns

    Returns:
        List of SingleRelationshipJoin objects showing how columns are connected
    """
    # Build mapping of column_id -> table_id
    col_to_table = {col.column_id: col.table_id for col in column_metadata}

    # Get tables involved in this group
    tables_in_group = {col_to_table.get(col_id) for col_id in column_ids if col_id in col_to_table}

    # Find relationships between tables in this group
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
) -> Result[CrossTableMulticollinearityAnalysis]:
    """Compute cross-table multicollinearity via unified correlation matrix.

    Main entry point for Phase 2 analysis.

    Args:
        table_ids: Tables to analyze
        duckdb_conn: DuckDB connection
        session: Async database session

    Returns:
        Result containing CrossTableMulticollinearityAnalysis
    """
    # Step 1: Gather relationships
    relationships = await gather_relationships(table_ids, session)

    if not relationships:
        # No relationships - return empty analysis
        return Result.ok(
            CrossTableMulticollinearityAnalysis(
                table_ids=table_ids,
                table_names=[],
                computed_at=datetime.now(UTC),
                total_columns_analyzed=0,
                total_relationships_used=0,
                overall_condition_index=0.0,
                overall_severity="none",
            )
        )

    # Step 2: Build unified correlation matrix
    matrix_result = await build_unified_correlation_matrix(
        table_ids, relationships, duckdb_conn, session
    )

    if not matrix_result.success:
        return Result.fail(matrix_result.error if matrix_result.error else "Matrix build failed")

    matrix_data = matrix_result.value

    # Step 3: Apply Belsley VDP (reuse existing function!)
    from dataraum_context.profiling.correlation import _compute_variance_decomposition

    # Eigenvalue decomposition
    eigenvalues, eigenvectors = np.linalg.eigh(matrix_data.correlation_matrix)

    # Sort descending
    idx = eigenvalues.argsort()[::-1]
    eigenvalues = eigenvalues[idx]
    eigenvectors = eigenvectors[:, idx]

    # Compute condition index
    max_eigenvalue = eigenvalues[0]
    min_eigenvalue = eigenvalues[-1]
    condition_index = (
        float(np.sqrt(max_eigenvalue / abs(min_eigenvalue)))
        if abs(min_eigenvalue) > 1e-10
        else 999.0
    )

    # Determine overall severity
    if condition_index < 10:
        overall_severity = "none"
    elif condition_index < 30:
        overall_severity = "moderate"
    else:
        overall_severity = "severe"

    # Compute VDP and identify dependency groups
    column_ids = [col.column_id for col in matrix_data.column_metadata]
    dependency_groups_pydantic = _compute_variance_decomposition(
        eigenvalues=eigenvalues,
        eigenvectors=eigenvectors,
        column_ids=column_ids,
        vdp_threshold=0.5,  # Belsley classical threshold
    )

    # Step 4: Enrich dependency groups with cross-table context
    enriched_groups = []
    cross_table_groups = []

    for group in dependency_groups_pydantic:
        # Map column IDs to (table, column) pairs
        involved_columns = []
        for col_id in group.involved_column_ids:
            col_meta = next((c for c in matrix_data.column_metadata if c.column_id == col_id), None)
            if col_meta:
                involved_columns.append((col_meta.table_name, col_meta.column_name))

        # Find join paths for this group
        join_paths = _find_join_paths_for_group(
            group.involved_column_ids, relationships, matrix_data.column_metadata
        )

        # Extract relationship types
        relationship_types = list({path.relationship_type for path in join_paths})

        # Create enriched group
        enriched = CrossTableDependencyGroup(
            dimension=group.dimension,
            eigenvalue=group.eigenvalue,
            condition_index=group.condition_index,
            severity=group.severity,
            involved_columns=involved_columns,
            column_ids=group.involved_column_ids,
            variance_proportions=group.variance_proportions,
            join_paths=join_paths,
            relationship_types=relationship_types,
        )

        enriched_groups.append(enriched)

        if enriched.num_tables > 1:
            cross_table_groups.append(enriched)

    # Generate quality issues
    quality_issues = []
    if len(cross_table_groups) > 0:
        affected_tables = list(
            {table for group in cross_table_groups for table, _ in group.involved_columns}
        )
        quality_issues.append(
            {
                "issue_type": "cross_table_multicollinearity",
                "severity": "critical" if overall_severity == "severe" else "warning",
                "description": (
                    f"{len(cross_table_groups)} dependency groups span multiple tables "
                    f"(overall CI={condition_index:.1f})"
                ),
                "affected_tables": affected_tables,
                "evidence": {
                    "condition_index": condition_index,
                    "num_groups": len(cross_table_groups),
                    "total_columns": matrix_data.num_columns,
                },
            }
        )

    # Build table names list
    table_names = []
    for tid in table_ids:
        t = await session.get(Table, tid)
        if t:
            table_names.append(t.table_name)

    return Result.ok(
        CrossTableMulticollinearityAnalysis(
            table_ids=table_ids,
            table_names=table_names,
            computed_at=datetime.now(UTC),
            total_columns_analyzed=matrix_data.num_columns,
            total_relationships_used=len(relationships),
            overall_condition_index=condition_index,
            overall_severity=overall_severity,
            dependency_groups=enriched_groups,
            cross_table_groups=cross_table_groups,
            quality_issues=quality_issues,
        )
    )
