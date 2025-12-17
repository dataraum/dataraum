"""Cross-table correlation and multicollinearity analysis.

Computes correlations and multicollinearity across joined tables using
the relationships infrastructure. Called 2x:
1. Before semantic agent: on relationship candidates → context for LLM
2. After semantic agent: on confirmed relationships → context for quality agents

Uses pure algorithms from algorithms/ folder:
- compute_pairwise_correlations: Pearson/Spearman correlations
- compute_cramers_v: Categorical associations
- compute_multicollinearity: Belsley VDP analysis
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import duckdb
import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.algorithms.categorical import (
    build_contingency_table,
    compute_cramers_v,
)
from dataraum_context.analysis.correlation.algorithms.multicollinearity import (
    compute_multicollinearity,
)
from dataraum_context.analysis.correlation.algorithms.numeric import (
    compute_pairwise_correlations,
)
from dataraum_context.analysis.relationships.db_models import (
    CrossTableMulticollinearityMetrics,
)
from dataraum_context.analysis.correlation.models import (
    CrossTableCategoricalAssociation,
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    CrossTableNumericCorrelation,
    EnrichedRelationship,
    SingleRelationshipJoin,
)
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table

# Default sample size for correlation analysis
DEFAULT_SAMPLE_SIZE = 10000
MIN_ROWS_FOR_ANALYSIS = 10


class ColumnMetadata:
    """Metadata for a column in cross-table analysis."""

    def __init__(
        self,
        column_id: str,
        table_id: str,
        table_name: str,
        column_name: str,
        column_type: str,
    ):
        self.column_id = column_id
        self.table_id = table_id
        self.table_name = table_name
        self.column_name = column_name
        self.column_type = column_type
        self.qualified_name = f"{table_name}.{column_name}"

    @property
    def is_numeric(self) -> bool:
        return self.column_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL", "FLOAT"]

    @property
    def is_categorical(self) -> bool:
        return self.column_type in ["VARCHAR", "BOOLEAN"]


async def analyze_cross_table_correlations(
    table_ids: list[str],
    relationships: list[EnrichedRelationship],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_correlation: float = 0.3,
    min_cramers_v: float = 0.1,
    vdp_threshold: float = 0.5,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    max_categorical_distinct: int = 50,
) -> Result[CrossTableMulticollinearityAnalysis]:
    """Compute cross-table correlations and multicollinearity.

    Uses pure algorithms from algorithms/ folder for all computations.

    Args:
        table_ids: Tables to analyze
        relationships: Enriched relationships for building joins
        duckdb_conn: DuckDB connection
        session: Async database session
        min_correlation: Minimum |r| for numeric correlation results
        min_cramers_v: Minimum Cramér's V for categorical associations
        vdp_threshold: VDP threshold for multicollinearity (0.5-0.8)
        sample_size: Number of rows to sample (uses DuckDB USING SAMPLE)
        max_categorical_distinct: Max distinct values for categorical analysis

    Returns:
        Result containing CrossTableMulticollinearityAnalysis
    """
    if len(table_ids) < 2:
        return Result.fail("Cross-table analysis requires at least 2 tables")

    table_names = await _get_table_names(table_ids, session)

    if not relationships:
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

    # Step 1: Get all columns across tables
    all_columns = await _get_all_columns(table_ids, session)
    numeric_columns = [c for c in all_columns if c.is_numeric]
    categorical_columns = [c for c in all_columns if c.is_categorical]

    if len(numeric_columns) < 2 and len(categorical_columns) < 2:
        return Result.fail("Need at least 2 numeric or 2 categorical columns for analysis")

    # Step 2: Build and execute join query with sampling
    try:
        join_query = await _build_join_query(
            table_ids, relationships, all_columns, session, sample_size
        )
        raw_data = duckdb_conn.execute(join_query).fetchnumpy()
    except Exception as e:
        return Result.fail(f"Join query failed: {e}")

    if not isinstance(raw_data, dict):
        return Result.fail("Unexpected data format from DuckDB")

    # Step 3: Compute numeric correlations using pure algorithm
    numeric_correlations = []
    cross_table_correlations = []

    if len(numeric_columns) >= 2:
        numeric_result = _compute_numeric_correlations(raw_data, numeric_columns, min_correlation)
        numeric_correlations = numeric_result["all"]
        cross_table_correlations = numeric_result["cross_table"]

    # Step 4: Compute categorical associations using pure algorithm
    categorical_associations = []
    cross_table_associations = []

    if len(categorical_columns) >= 2:
        cat_result = _compute_categorical_associations(
            raw_data, categorical_columns, min_cramers_v, max_categorical_distinct
        )
        categorical_associations = cat_result["all"]
        cross_table_associations = cat_result["cross_table"]

    # Step 5: Compute multicollinearity on numeric columns
    multi_result = None
    enriched_groups = []
    cross_table_groups = []
    overall_condition_index = 0.0
    overall_severity = "none"

    if len(numeric_columns) >= 2:
        multi_result = _compute_multicollinearity_analysis(
            raw_data, numeric_columns, vdp_threshold, relationships
        )
        if multi_result:
            enriched_groups = multi_result["dependency_groups"]
            cross_table_groups = multi_result["cross_table_groups"]
            overall_condition_index = multi_result["overall_condition_index"]
            overall_severity = multi_result["overall_severity"]

    # Step 6: Generate quality issues
    quality_issues = _generate_quality_issues(
        cross_table_correlations,
        cross_table_associations,
        cross_table_groups,
        overall_condition_index,
        overall_severity,
    )

    # Build result
    analysis = CrossTableMulticollinearityAnalysis(
        table_ids=table_ids,
        table_names=table_names,
        computed_at=datetime.now(UTC),
        total_columns_analyzed=len(all_columns),
        total_numeric_columns=len(numeric_columns),
        total_categorical_columns=len(categorical_columns),
        total_relationships_used=len(relationships),
        numeric_correlations=numeric_correlations,
        cross_table_correlations=cross_table_correlations,
        categorical_associations=categorical_associations,
        cross_table_associations=cross_table_associations,
        overall_condition_index=overall_condition_index,
        overall_severity=overall_severity,
        dependency_groups=enriched_groups,
        cross_table_groups=cross_table_groups,
        quality_issues=quality_issues,
    )

    return Result.ok(analysis)


def _compute_numeric_correlations(
    data: dict[str, Any],
    columns: list[ColumnMetadata],
    min_correlation: float,
) -> dict[str, list[CrossTableNumericCorrelation]]:
    """Compute numeric correlations using pure algorithm.

    Handles zero-variance columns and NaN values.
    """
    # Build data matrix, filtering zero-variance columns
    valid_columns = []
    column_data = []

    for col in columns:
        col_values = data.get(col.qualified_name)
        if col_values is None:
            continue

        # Convert to float array
        arr = np.asarray(col_values, dtype=np.float64)

        # Check for zero variance (would cause NaN in correlation)
        if np.nanstd(arr) < 1e-10:
            continue

        valid_columns.append(col)
        column_data.append(arr)

    if len(valid_columns) < 2:
        return {"all": [], "cross_table": []}

    # Stack into matrix
    X = np.column_stack(column_data)

    # Use pure algorithm
    corr_results = compute_pairwise_correlations(
        X, min_correlation=min_correlation, min_samples=MIN_ROWS_FOR_ANALYSIS
    )

    # Convert to CrossTableNumericCorrelation
    all_correlations = []
    cross_table = []

    for result in corr_results:
        col1 = valid_columns[result.col1_idx]
        col2 = valid_columns[result.col2_idx]
        is_cross = col1.table_id != col2.table_id

        corr = CrossTableNumericCorrelation(
            table1=col1.table_name,
            column1=col1.column_name,
            table2=col2.table_name,
            column2=col2.column_name,
            pearson_r=result.pearson_r,
            pearson_p=result.pearson_p,
            spearman_rho=result.spearman_rho,
            spearman_p=result.spearman_p,
            sample_size=result.sample_size,
            strength=result.strength,
            is_significant=result.is_significant,
            is_cross_table=is_cross,
        )
        all_correlations.append(corr)
        if is_cross:
            cross_table.append(corr)

    return {"all": all_correlations, "cross_table": cross_table}


def _compute_categorical_associations(
    data: dict[str, Any],
    columns: list[ColumnMetadata],
    min_cramers_v: float,
    max_distinct: int,
) -> dict[str, list[CrossTableCategoricalAssociation]]:
    """Compute categorical associations using pure algorithm."""
    # Filter columns with too many distinct values
    valid_columns = []
    column_values = []

    for col in columns:
        col_data = data.get(col.qualified_name)
        if col_data is None:
            continue

        # Convert to list and filter None/masked values
        values = []
        for v in col_data:
            if v is None:
                continue
            # Handle numpy masked values
            if hasattr(v, "mask") or isinstance(v, np.ma.core.MaskedConstant):
                continue
            values.append(v)

        if not values:
            continue

        # Convert to hashable types for distinct count
        try:
            distinct = len({str(v) for v in values})
        except Exception:
            continue

        if distinct < 2 or distinct > max_distinct:
            continue

        valid_columns.append(col)
        column_values.append(values)

    if len(valid_columns) < 2:
        return {"all": [], "cross_table": []}

    # Compute pairwise associations
    all_associations = []
    cross_table = []

    for i in range(len(valid_columns)):
        for j in range(i + 1, len(valid_columns)):
            col1 = valid_columns[i]
            col2 = valid_columns[j]
            vals1 = column_values[i]
            vals2 = column_values[j]

            # Align lengths (use min length)
            min_len = min(len(vals1), len(vals2))
            if min_len < MIN_ROWS_FOR_ANALYSIS:
                continue

            vals1 = vals1[:min_len]
            vals2 = vals2[:min_len]

            # Build contingency table
            try:
                contingency = build_contingency_table(vals1, vals2)
                result = compute_cramers_v(contingency, i, j)
            except Exception:
                continue

            if result is None or result.cramers_v < min_cramers_v:
                continue

            is_cross = col1.table_id != col2.table_id

            assoc = CrossTableCategoricalAssociation(
                table1=col1.table_name,
                column1=col1.column_name,
                table2=col2.table_name,
                column2=col2.column_name,
                cramers_v=result.cramers_v,
                chi_square=result.chi_square,
                p_value=result.p_value,
                sample_size=result.sample_size,
                strength=result.strength,
                is_significant=result.is_significant,
                is_cross_table=is_cross,
            )
            all_associations.append(assoc)
            if is_cross:
                cross_table.append(assoc)

    return {"all": all_associations, "cross_table": cross_table}


def _compute_multicollinearity_analysis(
    data: dict[str, Any],
    columns: list[ColumnMetadata],
    vdp_threshold: float,
    relationships: list[EnrichedRelationship],
) -> dict[str, Any] | None:
    """Compute multicollinearity using pure algorithm.

    Handles zero-variance columns and NaN values.
    """
    # Build data matrix, filtering zero-variance columns
    valid_columns = []
    column_data = []

    for col in columns:
        col_values = data.get(col.qualified_name)
        if col_values is None:
            continue

        arr = np.asarray(col_values, dtype=np.float64)

        # Check for zero variance
        if np.nanstd(arr) < 1e-10:
            continue

        valid_columns.append(col)
        column_data.append(arr)

    if len(valid_columns) < 2:
        return None

    # Stack and remove rows with NaN
    X = np.column_stack(column_data)
    X = X[~np.isnan(X).any(axis=1)]

    if len(X) < MIN_ROWS_FOR_ANALYSIS:
        return None

    # Compute correlation matrix
    try:
        corr_matrix = np.corrcoef(X, rowvar=False)

        # Check for NaN in correlation matrix
        if np.isnan(corr_matrix).any():
            return None

        # Use pure algorithm
        multi_result = compute_multicollinearity(corr_matrix, vdp_threshold)
    except Exception:
        return None

    # Enrich dependency groups with cross-table context
    enriched_groups = []
    cross_table_groups = []

    for group in multi_result.dependency_groups:
        involved_columns = [
            (valid_columns[idx].table_name, valid_columns[idx].column_name)
            for idx in group.involved_col_indices
        ]
        column_ids = [valid_columns[idx].column_id for idx in group.involved_col_indices]

        join_paths = _find_join_paths(column_ids, relationships, valid_columns)
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

    return {
        "dependency_groups": enriched_groups,
        "cross_table_groups": cross_table_groups,
        "overall_condition_index": multi_result.overall_condition_index,
        "overall_severity": multi_result.overall_severity,
    }


def _generate_quality_issues(
    cross_table_correlations: list[CrossTableNumericCorrelation],
    cross_table_associations: list[CrossTableCategoricalAssociation],
    cross_table_groups: list[CrossTableDependencyGroup],
    overall_condition_index: float,
    overall_severity: str,
) -> list[dict[str, Any]]:
    """Generate quality issues from analysis results."""
    quality_issues = []

    # Strong cross-table correlations
    strong_correlations = [
        c for c in cross_table_correlations if c.strength in ["strong", "very_strong"]
    ]
    if strong_correlations:
        quality_issues.append(
            {
                "issue_type": "strong_cross_table_correlation",
                "severity": "info",
                "description": (
                    f"{len(strong_correlations)} strong numeric correlations found across tables"
                ),
                "evidence": {
                    "count": len(strong_correlations),
                    "examples": [
                        f"{c.table1}.{c.column1} <-> {c.table2}.{c.column2} (r={c.pearson_r:.2f})"
                        for c in strong_correlations[:3]
                    ],
                },
            }
        )

    # Strong cross-table categorical associations
    strong_associations = [
        a for a in cross_table_associations if a.strength in ["strong", "moderate"]
    ]
    if strong_associations:
        quality_issues.append(
            {
                "issue_type": "strong_cross_table_association",
                "severity": "info",
                "description": (
                    f"{len(strong_associations)} categorical associations found across tables"
                ),
                "evidence": {
                    "count": len(strong_associations),
                    "examples": [
                        f"{a.table1}.{a.column1} <-> {a.table2}.{a.column2} (V={a.cramers_v:.2f})"
                        for a in strong_associations[:3]
                    ],
                },
            }
        )

    # Cross-table multicollinearity
    if cross_table_groups:
        affected_tables = list(
            {table for g in cross_table_groups for table, _ in g.involved_columns}
        )
        quality_issues.append(
            {
                "issue_type": "cross_table_multicollinearity",
                "severity": "critical" if overall_severity == "severe" else "warning",
                "description": (
                    f"{len(cross_table_groups)} dependency groups span multiple tables "
                    f"(overall CI={overall_condition_index:.1f})"
                ),
                "affected_tables": affected_tables,
                "evidence": {
                    "condition_index": overall_condition_index,
                    "num_groups": len(cross_table_groups),
                },
            }
        )

    return quality_issues


async def _get_table_names(table_ids: list[str], session: AsyncSession) -> list[str]:
    """Get table names for given IDs."""
    names = []
    for tid in table_ids:
        table = await session.get(Table, tid)
        if table:
            names.append(table.table_name)
    return names


async def _get_all_columns(
    table_ids: list[str],
    session: AsyncSession,
) -> list[ColumnMetadata]:
    """Get all columns (numeric and categorical) across tables."""
    stmt = (
        select(Column, Table)
        .join(Table, Column.table_id == Table.table_id)
        .where(
            Column.table_id.in_(table_ids),
            Column.resolved_type.in_(
                [
                    "INTEGER",
                    "BIGINT",
                    "DOUBLE",
                    "DECIMAL",
                    "FLOAT",  # Numeric
                    "VARCHAR",
                    "BOOLEAN",  # Categorical
                ]
            ),
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
            column_type=col.resolved_type,
        )
        for col, table in rows
    ]


async def _build_join_query(
    table_ids: list[str],
    relationships: list[EnrichedRelationship],
    columns: list[ColumnMetadata],
    session: AsyncSession,
    sample_size: int,
) -> str:
    """Build SQL query to join tables and select columns with sampling."""
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

    # FROM clause - sample base table FIRST, then join
    query = f"SELECT {', '.join(select_parts)} FROM (SELECT * FROM {base_table.duckdb_path} USING SAMPLE {sample_size} ROWS) AS t0"

    # JOIN clauses - track join columns for NULL filtering
    join_columns = []
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
                join_columns.append(f'{to_alias}."{rel.to_column}"')

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
                join_columns.append(f'{from_alias}."{rel.from_column}"')

    # Filter out rows where any join failed (NULL in join column)
    if join_columns:
        null_checks = " AND ".join(f"{col} IS NOT NULL" for col in join_columns)
        query += f" WHERE {null_checks}"

    # Limit final result
    query += f" LIMIT {sample_size}"

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
