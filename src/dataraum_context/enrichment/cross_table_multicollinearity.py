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
from uuid import uuid4

import duckdb
import numpy as np
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.relationships.models import (
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    DependencyGroup,
    EnrichedRelationship,
    SingleRelationshipJoin,
)
from dataraum_context.core.models.base import Result

# Re-export from relationships package for backward compatibility
from dataraum_context.enrichment.relationships.gathering import (
    CONFIDENCE_THRESHOLDS,
    gather_relationships,
)
from dataraum_context.storage import Column, Table

__all__ = [
    "EnrichedRelationship",
    "gather_relationships",
    "CONFIDENCE_THRESHOLDS",
    "compute_cross_table_multicollinearity",
]

# === Helper Models ===


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


# === Variance Decomposition (Belsley VDP) ===


def _compute_variance_decomposition(
    eigenvalues: np.ndarray,
    eigenvectors: np.ndarray,
    column_ids: list[str],
    vdp_threshold: float = 0.5,
) -> list[DependencyGroup]:
    """Compute Variance Decomposition Proportions per Belsley, Kuh, Welsch (1980).

    Correct implementation of Belsley diagnostics for detecting multicollinearity
    involving any number of variables (2, 3, 4+).

    Methodology:
    1. Compute phi_kj = V_kj² / D_j² for all variables k and dimensions j
    2. For each variable k: VDP_kj = phi_kj / Σ_j(phi_kj) [normalize across dimensions]
    3. For dimensions with high CI (>30): variables with VDP > threshold form a group

    This differs from naive eigenvector normalization - VDPs sum to 1 ACROSS dimensions
    for each variable, not across variables for a single dimension.

    Args:
        eigenvalues: Eigenvalues sorted descending (D² from SVD)
        eigenvectors: Corresponding eigenvectors (V from SVD)
        column_ids: Column IDs corresponding to data matrix columns
        vdp_threshold: VDP threshold (Belsley recommends 0.5-0.8)

    Returns:
        List of DependencyGroup objects

    Reference:
        Belsley, D.A., Kuh, E., Welsch, R.E. (1980). Regression Diagnostics:
        Identifying Influential Data and Sources of Collinearity. Wiley.
    """
    n_vars = len(column_ids)
    n_dims = len(eigenvalues)

    # Step 1: Compute phi matrix (n_vars x n_dims)
    # phi_kj = V_kj² / eigenvalue_j
    # Use abs(eigenvalue) to handle near-zero negative values
    phi_matrix = np.zeros((n_vars, n_dims))
    for j in range(n_dims):
        eigenvalue_abs = abs(eigenvalues[j]) if abs(eigenvalues[j]) > 1e-10 else 1e-10
        for k in range(n_vars):
            phi_matrix[k, j] = eigenvectors[k, j] ** 2 / eigenvalue_abs

    # Step 2: Compute VDP matrix by normalizing phi across dimensions (row-wise)
    # VDP_kj = phi_kj / Σ_j(phi_kj)
    phi_sums = phi_matrix.sum(axis=1, keepdims=True)  # Sum across dimensions for each var
    phi_sums = np.where(phi_sums < 1e-10, 1.0, phi_sums)  # Avoid division by zero
    vdp_matrix = phi_matrix / phi_sums

    # Step 3: For each high-CI dimension, find variables with high VDP
    dependency_groups = []
    max_eigenvalue = eigenvalues[0]

    for j, eigenvalue in enumerate(eigenvalues):
        # Skip if eigenvalue is not near-zero
        if abs(eigenvalue) >= 0.01:
            continue

        # Compute condition index for this dimension
        condition_index = float(np.sqrt(max_eigenvalue / abs(eigenvalue)))

        # Belsley recommends CI > 30 for severe multicollinearity
        if condition_index < 10:
            continue

        # Find variables with VDP > threshold on this dimension
        high_vdp_indices = np.where(vdp_matrix[:, j] > vdp_threshold)[0]

        # Need at least 2 variables for a dependency group
        if len(high_vdp_indices) < 2:
            continue

        # Determine severity
        severity = "severe" if condition_index > 30 else "moderate"

        dependency_groups.append(
            DependencyGroup(
                dimension=j,
                eigenvalue=float(eigenvalue),
                condition_index=condition_index,
                severity=severity,
                involved_column_ids=[column_ids[idx] for idx in high_vdp_indices],
                variance_proportions=vdp_matrix[high_vdp_indices, j].tolist(),
            )
        )

    return dependency_groups


# === Main Functions ===


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
    if matrix_data is None:
        return Result.fail("Matrix build succeeded but returned None value")

    # Step 3: Apply Belsley VDP
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

    analysis = CrossTableMulticollinearityAnalysis(
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

    # Store to database using hybrid storage approach
    storage_result = await store_cross_table_analysis(analysis, session)
    if not storage_result.success:
        # Log warning but don't fail the analysis
        return Result.ok(analysis, warnings=[f"Failed to store results: {storage_result.error}"])

    return Result.ok(analysis)


async def get_stored_cross_table_analysis(
    table_ids: list[str],
    session: AsyncSession,
) -> Result[CrossTableMulticollinearityAnalysis | None]:
    """Query most recent stored cross-table multicollinearity analysis for given tables.

    Returns the latest analysis (by computed_at) that matches the table set.

    Args:
        table_ids: List of table IDs to find analysis for
        session: Database session

    Returns:
        Result containing most recent analysis if found, None if not found
    """
    from dataraum_context.analysis.relationships.db_models import (
        CrossTableMulticollinearityMetrics,
    )

    try:
        # Sort table_ids for consistent comparison
        sorted_table_ids = sorted(table_ids)

        # Query for matching analysis (most recent first)
        stmt = select(CrossTableMulticollinearityMetrics).order_by(
            CrossTableMulticollinearityMetrics.computed_at.desc()
        )

        result = await session.execute(stmt)
        metrics = result.scalars().all()

        # Find matching table set (returns first match = most recent)
        for metric in metrics:
            stored_ids = sorted(metric.table_ids.get("table_ids", []))
            if stored_ids == sorted_table_ids:
                # Deserialize JSONB to Pydantic model
                analysis = CrossTableMulticollinearityAnalysis.model_validate(metric.analysis_data)
                return Result.ok(analysis)

        # No matching analysis found
        return Result.ok(None)

    except Exception as e:
        return Result.fail(f"Failed to query stored analysis: {e}")


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
    from dataraum_context.analysis.relationships.db_models import (
        CrossTableMulticollinearityMetrics,
    )

    try:
        # Create database record with hybrid storage
        metric = CrossTableMulticollinearityMetrics(
            metric_id=str(uuid4()),
            computed_at=analysis.computed_at,
            # Store table IDs as JSON array
            table_ids={"table_ids": analysis.table_ids},
            # Structured fields for querying
            overall_condition_index=analysis.overall_condition_index,
            num_cross_table_groups=len(analysis.cross_table_groups),
            num_total_groups=len(analysis.dependency_groups),
            has_severe_cross_table_dependencies=(analysis.overall_severity == "severe"),
            total_columns_analyzed=analysis.total_columns_analyzed,
            total_relationships_used=analysis.total_relationships_used,
            # Full Pydantic model as JSONB for flexibility
            analysis_data=analysis.model_dump(mode="json"),
        )

        session.add(metric)
        await session.commit()

        return Result.ok(metric.metric_id)

    except Exception as e:
        await session.rollback()
        return Result.fail(f"Failed to store cross-table analysis: {e}")
