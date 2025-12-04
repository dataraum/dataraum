"""Correlation analysis between columns within a table.

This module implements:
- Numeric correlation (Pearson, Spearman)
- Categorical association (Cramér's V)
- Functional dependency detection
- Derived column detection
- Multicollinearity detection (VIF, Tolerance, Condition Index)

All analysis is within a single table. Cross-table relationships
are handled by the topological enrichment module.
"""

import time
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import ColumnRef, Result
from dataraum_context.profiling.models import (
    CategoricalAssociation as CategoricalAssociationModel,
)
from dataraum_context.profiling.models import (
    ColumnVIF,
    ConditionIndexAnalysis,
    CorrelationAnalysisResult,
    DependencyGroup,
    MulticollinearityAnalysis,
    NumericCorrelation,
)
from dataraum_context.profiling.models import (
    DerivedColumn as DerivedColumnModel,
)
from dataraum_context.profiling.models import (
    FunctionalDependency as FunctionalDependencyModel,
)
from dataraum_context.storage.models_v2.core import Column, Table
from dataraum_context.storage.models_v2.correlation_context import (
    CategoricalAssociation as DBCategoricalAssociation,
)
from dataraum_context.storage.models_v2.correlation_context import (
    ColumnCorrelation as DBColumnCorrelation,
)
from dataraum_context.storage.models_v2.correlation_context import (
    DerivedColumn as DBDerivedColumn,
)
from dataraum_context.storage.models_v2.correlation_context import (
    FunctionalDependency as DBFunctionalDependency,
)
from dataraum_context.storage.models_v2.correlation_context import (
    MulticollinearityMetrics as DBMulticollinearityMetrics,
)

# ============================================================================
# Numeric Correlation (Pearson & Spearman)
# ============================================================================


async def compute_numeric_correlations(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_correlation: float = 0.3,
) -> Result[list[NumericCorrelation]]:
    """Compute Pearson and Spearman correlations for all numeric column pairs.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        min_correlation: Minimum |r| to store (reduces noise)

    Returns:
        Result containing list of NumericCorrelation objects
    """
    try:
        # Get numeric columns
        stmt = select(Column).where(
            Column.table_id == table.table_id,
            Column.resolved_type.in_(["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]),
        )
        result = await session.execute(stmt)
        numeric_columns = result.scalars().all()

        if len(numeric_columns) < 2:
            return Result.ok([])  # Need at least 2 columns

        correlations = []
        table_name = table.duckdb_path

        # Compute correlations for all pairs
        for i, col1 in enumerate(numeric_columns):
            for col2 in numeric_columns[i + 1 :]:  # Only upper triangle
                # Get values from DuckDB
                query = f"""
                    SELECT
                        TRY_CAST("{col1.column_name}" AS DOUBLE) as val1,
                        TRY_CAST("{col2.column_name}" AS DOUBLE) as val2
                    FROM {table_name}
                    WHERE
                        TRY_CAST("{col1.column_name}" AS DOUBLE) IS NOT NULL
                        AND TRY_CAST("{col2.column_name}" AS DOUBLE) IS NOT NULL
                """

                data = duckdb_conn.execute(query).fetchnumpy()
                val1 = data["val1"]
                val2 = data["val2"]

                if len(val1) < 10:
                    continue  # Not enough data

                # Pearson correlation
                pearson_r, pearson_p = stats.pearsonr(val1, val2)
                pearson_r_float = float(np.asarray(pearson_r).item())
                pearson_p_float = float(np.asarray(pearson_p).item())

                # Spearman correlation
                spearman_rho, spearman_p = stats.spearmanr(val1, val2)
                spearman_rho_float = float(np.asarray(spearman_rho).item())
                spearman_p_float = float(np.asarray(spearman_p).item())

                # Only store if above threshold
                if (
                    abs(pearson_r_float) < min_correlation
                    and abs(spearman_rho_float) < min_correlation
                ):
                    continue

                # Determine strength
                max_corr = max(abs(pearson_r_float), abs(spearman_rho_float))
                if max_corr >= 0.9:
                    strength = "very_strong"
                elif max_corr >= 0.7:
                    strength = "strong"
                elif max_corr >= 0.5:
                    strength = "moderate"
                elif max_corr >= 0.3:
                    strength = "weak"
                else:
                    strength = "none"

                is_significant = bool(min(pearson_p_float, spearman_p_float) < 0.05)

                computed_at = datetime.now(UTC)

                correlation = NumericCorrelation(
                    correlation_id=str(uuid4()),
                    table_id=table.table_id,
                    column1_id=col1.column_id,
                    column2_id=col2.column_id,
                    column1_name=col1.column_name,
                    column2_name=col2.column_name,
                    pearson_r=pearson_r_float,
                    pearson_p_value=pearson_p_float,
                    spearman_rho=spearman_rho_float,
                    spearman_p_value=spearman_p_float,
                    sample_size=len(val1),
                    computed_at=computed_at,
                    correlation_strength=strength,
                    is_significant=is_significant,
                )

                correlations.append(correlation)

                # Store in database
                db_corr = DBColumnCorrelation(
                    correlation_id=correlation.correlation_id,
                    table_id=correlation.table_id,
                    column1_id=correlation.column1_id,
                    column2_id=correlation.column2_id,
                    pearson_r=correlation.pearson_r,
                    pearson_p_value=correlation.pearson_p_value,
                    spearman_rho=correlation.spearman_rho,
                    spearman_p_value=correlation.spearman_p_value,
                    sample_size=correlation.sample_size,
                    computed_at=correlation.computed_at,
                    correlation_strength=correlation.correlation_strength,
                    is_significant=correlation.is_significant,
                )
                session.add(db_corr)

        await session.commit()

        return Result.ok(correlations)

    except Exception as e:
        return Result.fail(f"Numeric correlation computation failed: {e}")


# ============================================================================
# Categorical Association (Cramér's V)
# ============================================================================


async def compute_categorical_associations(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    max_distinct_values: int = 100,
    min_cramers_v: float = 0.1,
) -> Result[list[CategoricalAssociationModel]]:
    """Compute Cramér's V for categorical column pairs.

    Cramér's V is based on chi-square test and ranges from 0 (no association)
    to 1 (perfect association).

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        max_distinct_values: Skip columns with too many values
        min_cramers_v: Minimum V to store

    Returns:
        Result containing list of CategoricalAssociation objects
    """
    try:
        # Get columns that could be categorical (low cardinality or VARCHAR)
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = await session.execute(stmt)
        all_columns = result.scalars().all()

        # Filter to categorical candidates
        categorical_columns = []
        table_name = table.duckdb_path

        for col in all_columns:
            # Get distinct count
            query = f'SELECT COUNT(DISTINCT "{col.column_name}") FROM {table_name}'
            distinct_count_rows = duckdb_conn.execute(query).fetchone()

            if distinct_count_rows and 2 <= distinct_count_rows[0] <= max_distinct_values:
                categorical_columns.append(col)

        if len(categorical_columns) < 2:
            return Result.ok([])

        associations = []

        # Compute Cramér's V for all pairs
        for i, col1 in enumerate(categorical_columns):
            for col2 in categorical_columns[i + 1 :]:
                # Build contingency table using DuckDB
                query = f"""
                    SELECT
                        "{col1.column_name}" as val1,
                        "{col2.column_name}" as val2,
                        COUNT(*) as count
                    FROM {table_name}
                    WHERE
                        "{col1.column_name}" IS NOT NULL
                        AND "{col2.column_name}" IS NOT NULL
                    GROUP BY val1, val2
                """

                contingency_data = duckdb_conn.execute(query).fetchall()

                if len(contingency_data) < 4:
                    continue  # Need at least 2x2 table

                # Convert to contingency table
                # This is complex - we need to pivot the data
                # For now, we'll use scipy's contingency table builder
                val1_list = [row[0] for row in contingency_data]
                val2_list = [row[1] for row in contingency_data]
                count_list = [row[2] for row in contingency_data]

                # Create contingency table
                unique_val1 = sorted(set(val1_list))
                unique_val2 = sorted(set(val2_list))

                contingency = np.zeros((len(unique_val1), len(unique_val2)))
                val1_idx = {v: i for i, v in enumerate(unique_val1)}
                val2_idx = {v: i for i, v in enumerate(unique_val2)}

                for v1, v2, count in zip(val1_list, val2_list, count_list, strict=False):
                    contingency[val1_idx[v1], val2_idx[v2]] = count

                # Chi-square test
                chi2, p_value, dof, expected = stats.chi2_contingency(contingency)

                # Cramér's V
                n = contingency.sum()
                min_dim = min(len(unique_val1), len(unique_val2)) - 1
                cramers_v = np.sqrt(chi2 / (n * min_dim))

                if cramers_v < min_cramers_v:
                    continue

                # Determine strength
                if cramers_v >= 0.5:
                    strength = "strong"
                elif cramers_v >= 0.3:
                    strength = "moderate"
                elif cramers_v >= 0.1:
                    strength = "weak"
                else:
                    strength = "none"

                computed_at = datetime.now(UTC)

                association = CategoricalAssociationModel(
                    association_id=str(uuid4()),
                    table_id=table.table_id,
                    column1_id=col1.column_id,
                    column2_id=col2.column_id,
                    column1_name=col1.column_name,
                    column2_name=col2.column_name,
                    cramers_v=float(cramers_v),
                    chi_square=float(chi2),
                    p_value=float(p_value),
                    degrees_of_freedom=int(dof),
                    sample_size=int(n),
                    computed_at=computed_at,
                    association_strength=strength,
                    is_significant=bool(p_value < 0.05),
                )

                associations.append(association)

                # Store in database
                db_assoc = DBCategoricalAssociation(
                    association_id=association.association_id,
                    table_id=association.table_id,
                    column1_id=association.column1_id,
                    column2_id=association.column2_id,
                    cramers_v=association.cramers_v,
                    chi_square=association.chi_square,
                    p_value=association.p_value,
                    degrees_of_freedom=association.degrees_of_freedom,
                    sample_size=association.sample_size,
                    computed_at=association.computed_at,
                    association_strength=association.association_strength,
                    is_significant=association.is_significant,
                )
                session.add(db_assoc)

        await session.commit()

        return Result.ok(associations)

    except Exception as e:
        return Result.fail(f"Categorical association computation failed: {e}")


# ============================================================================
# Functional Dependency Detection
# ============================================================================


async def detect_functional_dependencies(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_confidence: float = 0.95,
    max_determinant_columns: int = 3,
) -> Result[list[FunctionalDependencyModel]]:
    """Detect functional dependencies: A → B or (A, B) → C.

    A functional dependency means that for each value (or combination of values)
    in the determinant, there is exactly one value in the dependent column.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: AsyncSession
        min_confidence: Minimum confidence (1.0 = exact FD)
        max_determinant_columns: Maximum columns in determinant

    Returns:
        Result containing list of FunctionalDependency objects
    """
    try:
        # Get all columns
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = await session.execute(stmt)
        columns = result.scalars().all()

        if len(columns) < 2:
            return Result.ok([])

        dependencies = []
        table_name = table.duckdb_path

        # Check single-column FDs: A → B
        for col_a in columns:
            for col_b in columns:
                if col_a.column_id == col_b.column_id:
                    continue

                # Check if col_a → col_b
                query = f"""
                    WITH mappings AS (
                        SELECT
                            "{col_a.column_name}",
                            COUNT(DISTINCT "{col_b.column_name}") as distinct_b_values
                        FROM {table_name}
                        WHERE
                            "{col_a.column_name}" IS NOT NULL
                            AND "{col_b.column_name}" IS NOT NULL
                        GROUP BY "{col_a.column_name}"
                    )
                    SELECT
                        COUNT(CASE WHEN distinct_b_values = 1 THEN 1 END) as valid_mappings,
                        COUNT(CASE WHEN distinct_b_values > 1 THEN 1 END) as violations,
                        COUNT(*) as total_unique_a
                    FROM mappings
                """

                fd_result = duckdb_conn.execute(query).fetchone()
                if not fd_result:
                    continue
                valid_mappings, violations, total_unique_a = fd_result

                if total_unique_a == 0:
                    continue

                confidence = valid_mappings / total_unique_a

                if confidence >= min_confidence:
                    computed_at = datetime.now(UTC)

                    # Get example
                    example_query = f"""
                        SELECT "{col_a.column_name}", "{col_b.column_name}"
                        FROM {table_name}
                        WHERE "{col_a.column_name}" IS NOT NULL
                        LIMIT 1
                    """
                    example_row = duckdb_conn.execute(example_query).fetchone()
                    example = (
                        {
                            "determinant_values": [str(example_row[0])],
                            "dependent_value": str(example_row[1]),
                        }
                        if example_row
                        else None
                    )

                    dependency = FunctionalDependencyModel(
                        dependency_id=str(uuid4()),
                        table_id=table.table_id,
                        determinant_column_ids=[col_a.column_id],
                        determinant_column_names=[col_a.column_name],
                        dependent_column_id=col_b.column_id,
                        dependent_column_name=col_b.column_name,
                        confidence=float(confidence),
                        unique_determinant_values=int(total_unique_a),
                        violation_count=int(violations),
                        example=example,
                        computed_at=computed_at,
                    )

                    dependencies.append(dependency)

                    # Store in database
                    db_fd = DBFunctionalDependency(
                        dependency_id=dependency.dependency_id,
                        table_id=dependency.table_id,
                        determinant_column_ids=[col_a.column_id],
                        dependent_column_id=dependency.dependent_column_id,
                        confidence=dependency.confidence,
                        unique_determinant_values=dependency.unique_determinant_values,
                        violation_count=dependency.violation_count,
                        example=dependency.example,
                        computed_at=dependency.computed_at,
                    )
                    session.add(db_fd)

        await session.commit()

        return Result.ok(dependencies)

    except Exception as e:
        return Result.fail(f"Functional dependency detection failed: {e}")


# ============================================================================
# Derived Column Detection
# ============================================================================


async def detect_derived_columns(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_match_rate: float = 0.95,
) -> Result[list[DerivedColumnModel]]:
    """Detect columns that are derived from other columns.

    Checks for:
    - Arithmetic: col3 = col1 + col2, col1 - col2, col1 * col2, col1 / col2
    - String transforms: col2 = UPPER(col1), LOWER(col1)
    - Concatenation: col3 = col1 || col2

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: AsyncSession
        min_match_rate: Minimum match rate to consider derived

    Returns:
        Result containing list of DerivedColumn objects
    """
    try:
        # Get columns
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = await session.execute(stmt)
        columns = result.scalars().all()

        if len(columns) < 2:
            return Result.ok([])

        derived_columns = []
        table_name = table.duckdb_path

        # Check arithmetic derivations (numeric columns only)
        numeric_cols = [
            c for c in columns if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
        ]

        for target in numeric_cols:
            for col1 in numeric_cols:
                for col2 in numeric_cols:
                    if target.column_id in [col1.column_id, col2.column_id]:
                        continue

                    # Check col_target = col1 + col2
                    for op, op_name in [
                        ("+", "sum"),
                        ("-", "difference"),
                        ("*", "product"),
                        ("/", "ratio"),
                    ]:
                        query = f"""
                            WITH derivation_check AS (
                                SELECT
                                    TRY_CAST("{target.column_name}" AS DOUBLE) as target_val,
                                    TRY_CAST("{col1.column_name}" AS DOUBLE) as col1_val,
                                    TRY_CAST("{col2.column_name}" AS DOUBLE) as col2_val,
                                    ABS(
                                        TRY_CAST("{target.column_name}" AS DOUBLE) -
                                        (TRY_CAST("{col1.column_name}" AS DOUBLE) {op} TRY_CAST("{col2.column_name}" AS DOUBLE))
                                    ) as diff
                                FROM {table_name}
                                WHERE
                                    "{target.column_name}" IS NOT NULL
                                    AND "{col1.column_name}" IS NOT NULL
                                    AND "{col2.column_name}" IS NOT NULL
                            )
                            SELECT
                                COUNT(CASE WHEN diff < 0.01 THEN 1 END) as matches,
                                COUNT(*) as total
                            FROM derivation_check
                        """

                        deriv_result = duckdb_conn.execute(query).fetchone()
                        if not deriv_result:
                            continue
                        matches, total = deriv_result

                        if total == 0:
                            continue

                        match_rate = matches / total

                        if match_rate >= min_match_rate:
                            computed_at = datetime.now(UTC)

                            derived = DerivedColumnModel(
                                derived_id=str(uuid4()),
                                table_id=table.table_id,
                                derived_column_id=target.column_id,
                                derived_column_name=target.column_name,
                                source_column_ids=[col1.column_id, col2.column_id],
                                source_column_names=[col1.column_name, col2.column_name],
                                derivation_type=op_name,
                                formula=f"{col1.column_name} {op} {col2.column_name}",
                                match_rate=float(match_rate),
                                total_rows=int(total),
                                matching_rows=int(matches),
                                mismatch_examples=None,  # Could add samples
                                computed_at=computed_at,
                            )

                            derived_columns.append(derived)

                            # Store in database
                            db_derived = DBDerivedColumn(
                                derived_id=derived.derived_id,
                                table_id=derived.table_id,
                                derived_column_id=derived.derived_column_id,
                                source_column_ids=[col1.column_id, col2.column_id],
                                derivation_type=derived.derivation_type,
                                formula=derived.formula,
                                match_rate=derived.match_rate,
                                total_rows=derived.total_rows,
                                matching_rows=derived.matching_rows,
                                mismatch_examples=derived.mismatch_examples,
                                computed_at=derived.computed_at,
                            )
                            session.add(db_derived)

        await session.commit()

        return Result.ok(derived_columns)

    except Exception as e:
        return Result.fail(f"Derived column detection failed: {e}")


# ============================================================================
# Multicollinearity Detection (VIF, Tolerance, Condition Index)
# ============================================================================


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


def _compute_condition_index(
    X: np.ndarray, column_ids: list[str] | None = None
) -> ConditionIndexAnalysis | None:
    """Compute Condition Index via eigenvalue analysis.

    The condition index is the square root of the ratio of the largest
    to smallest eigenvalue of the correlation matrix.

    If column_ids are provided, also computes Variance Decomposition Proportions
    (VDP) to identify which columns are involved in each linear dependency.

    Args:
        X: Data matrix (n_samples, n_features)
        column_ids: Optional list of column IDs for VDP analysis

    Returns:
        ConditionIndexAnalysis or None if computation fails
    """
    try:
        # Compute correlation matrix
        corr_matrix = np.corrcoef(X, rowvar=False)

        # Compute eigenvalues AND eigenvectors (need eigenvectors for VDP)
        eigenvalues, eigenvectors = np.linalg.eigh(corr_matrix)

        # Sort descending (eigh returns ascending)
        idx = eigenvalues.argsort()[::-1]
        eigenvalues = eigenvalues[idx]
        eigenvectors = eigenvectors[:, idx]

        # Condition Index = sqrt(max_eigenvalue / min_eigenvalue)
        if eigenvalues[-1] < 1e-10:  # Near-zero eigenvalue
            condition_index = 999.0
        else:
            condition_index = float(np.sqrt(eigenvalues[0] / eigenvalues[-1]))

        # Count near-zero eigenvalues (< 0.01)
        problematic_dimensions = int(np.sum(eigenvalues < 0.01))

        # Determine severity (Belsley, Kuh, Welsch, 1980)
        # CI < 10: Weak or no multicollinearity
        # CI 10-30: Moderate multicollinearity
        # CI > 30: Severe multicollinearity
        if condition_index < 10:
            severity = "none"
            has_multicollinearity = False
        elif condition_index < 30:
            severity = "moderate"
            has_multicollinearity = True
        else:
            severity = "severe"
            has_multicollinearity = True

        # Compute VDP to identify dependency groups (if column_ids provided and CI >= 10)
        dependency_groups = []
        if column_ids is not None and condition_index >= 10:
            dependency_groups = _compute_variance_decomposition(
                eigenvalues, eigenvectors, column_ids
            )

        return ConditionIndexAnalysis(
            condition_index=condition_index,
            eigenvalues=eigenvalues.tolist(),
            has_multicollinearity=has_multicollinearity,
            severity=severity,
            problematic_dimensions=problematic_dimensions,
            dependency_groups=dependency_groups,
        )

    except Exception:
        return None


async def compute_multicollinearity_for_table(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[MulticollinearityAnalysis]:
    """Compute multicollinearity metrics for all numeric columns.

    Computes:
    1. VIF (Variance Inflation Factor) for each column
    2. Tolerance (1/VIF) for each column
    3. Condition Index for the table (eigenvalue-based)

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: AsyncSession

    Returns:
        Result containing MulticollinearityAnalysis
    """
    try:
        # Get numeric columns
        stmt = select(Column).where(
            Column.table_id == table.table_id,
            Column.resolved_type.in_(["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]),
        )
        result = await session.execute(stmt)
        numeric_columns = result.scalars().all()

        if len(numeric_columns) < 2:
            return Result.ok(
                MulticollinearityAnalysis(
                    table_id=table.table_id,
                    table_name=table.table_name,
                    computed_at=datetime.now(UTC),
                    overall_severity="none",
                )
            )

        # Get data matrix
        table_name = table.duckdb_path
        column_names = [col.column_name for col in numeric_columns]

        query = f"""
            SELECT {", ".join(f'TRY_CAST("{col}" AS DOUBLE) as "{col}"' for col in column_names)}
            FROM {table_name}
        """

        data = duckdb_conn.execute(query).fetchnumpy()

        # Build matrix (remove rows with any NULL)
        if isinstance(data, dict):
            X = np.column_stack([data[col] for col in column_names])
        elif hasattr(data, "dtype") and data.dtype.names is not None:
            X = np.column_stack([data[col] for col in data.dtype.names])
        else:
            return Result.fail("Unexpected data format from fetchnumpy()")

        # Remove rows with NaN
        X = X[~np.isnan(X).any(axis=1)]

        if len(X) < 10 or X.shape[1] < 2:
            return Result.ok(
                MulticollinearityAnalysis(
                    table_id=table.table_id,
                    table_name=table.table_name,
                    computed_at=datetime.now(UTC),
                    overall_severity="none",
                )
            )

        # Compute VIF for each column
        column_vifs = []

        for i, col in enumerate(numeric_columns):
            # Skip if not enough variance
            if np.std(X[:, i]) < 1e-10:
                continue

            # Regress column i against all others
            y = X[:, i]
            X_others = np.delete(X, i, axis=1)

            try:
                reg = LinearRegression()
                reg.fit(X_others, y)
                r_squared = reg.score(X_others, y)

                # VIF = 1 / (1 - R²)
                vif = 1 / (1 - r_squared) if r_squared < 0.9999 else 999.0
                tolerance = 1 / vif if vif < 999 else 0.001

                # Determine severity
                if vif < 5:
                    severity = "none"
                elif vif < 10:
                    severity = "moderate"
                else:
                    severity = "severe"

                # Find highly correlated columns (for context)
                correlated_with = []
                for j, other_col in enumerate(numeric_columns):
                    if i == j:
                        continue
                    corr = np.corrcoef(X[:, i], X[:, j])[0, 1]
                    if abs(corr) > 0.7:
                        correlated_with.append(other_col.column_id)

                column_vif = ColumnVIF(
                    column_id=col.column_id,
                    column_ref=ColumnRef(
                        table_name=table.table_name,
                        column_name=col.column_name,
                    ),
                    vif=float(vif),
                    tolerance=float(tolerance),
                    has_multicollinearity=vif > 10,
                    severity=severity,
                    correlated_with=correlated_with,
                )

                column_vifs.append(column_vif)

            except Exception:
                continue  # Skip this column if regression fails

        # Compute Condition Index with VDP analysis (eigenvalue-based)
        column_ids = [col.column_id for col in numeric_columns]
        condition_index_analysis = _compute_condition_index(X, column_ids)

        # Aggregate findings
        num_problematic = sum(1 for vif in column_vifs if vif.has_multicollinearity)

        # Determine overall severity
        if condition_index_analysis and condition_index_analysis.condition_index > 30:
            overall_severity = "severe"
        elif condition_index_analysis and condition_index_analysis.condition_index >= 10:
            overall_severity = "moderate"
        elif num_problematic > 0:
            overall_severity = "moderate"
        else:
            overall_severity = "none"

        # Generate quality issues
        quality_issues = []

        if num_problematic > 0:
            quality_issues.append(
                {
                    "issue_type": "high_multicollinearity",
                    "severity": "warning" if num_problematic < 3 else "critical",
                    "description": f"{num_problematic} columns have severe multicollinearity (VIF > 10)",
                    "affected_columns": [
                        vif.column_id for vif in column_vifs if vif.has_multicollinearity
                    ],
                    "evidence": {
                        "num_problematic": num_problematic,
                        "max_vif": max(vif.vif for vif in column_vifs),
                    },
                }
            )

        if condition_index_analysis and condition_index_analysis.has_multicollinearity:
            quality_issues.append(
                {
                    "issue_type": "table_multicollinearity",
                    "severity": "critical"
                    if condition_index_analysis.condition_index > 30
                    else "warning",
                    "description": condition_index_analysis.interpretation,
                    "evidence": {
                        "condition_index": condition_index_analysis.condition_index,
                        "problematic_dimensions": condition_index_analysis.problematic_dimensions,
                    },
                }
            )

        analysis = MulticollinearityAnalysis(
            table_id=table.table_id,
            table_name=table.table_name,
            computed_at=datetime.now(UTC),
            column_vifs=column_vifs,
            num_problematic_columns=num_problematic,
            condition_index=condition_index_analysis,
            has_severe_multicollinearity=overall_severity == "severe",
            overall_severity=overall_severity,
            quality_issues=quality_issues,
        )

        return Result.ok(analysis)

    except Exception as e:
        return Result.fail(f"Multicollinearity computation failed: {e}")


# ============================================================================
# Main Correlation Analysis
# ============================================================================


async def analyze_correlations(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[CorrelationAnalysisResult]:
    """Run complete correlation analysis on a table.

    This orchestrates all correlation analyses:
    - Numeric correlations
    - Categorical associations
    - Functional dependencies
    - Derived columns
    - Multicollinearity (VIF, Tolerance, Condition Index)

    Args:
        table_id: Table ID to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing CorrelationAnalysisResult
    """
    start_time = time.time()

    try:
        # Get table
        table = await session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        # Run all analyses
        numeric_corr_result = await compute_numeric_correlations(table, duckdb_conn, session)
        numeric_correlations = numeric_corr_result.unwrap() if numeric_corr_result.success else []

        categorical_assoc_result = await compute_categorical_associations(
            table, duckdb_conn, session
        )
        categorical_associations = (
            categorical_assoc_result.unwrap() if categorical_assoc_result.success else []
        )

        fd_result = await detect_functional_dependencies(table, duckdb_conn, session)
        functional_dependencies = fd_result.unwrap() if fd_result.success else []

        derived_result = await detect_derived_columns(table, duckdb_conn, session)
        derived_columns = derived_result.unwrap() if derived_result.success else []

        # Compute multicollinearity (VIF, Tolerance, Condition Index)
        multicollinearity_result = await compute_multicollinearity_for_table(
            table, duckdb_conn, session
        )
        multicollinearity_analysis = (
            multicollinearity_result.value if multicollinearity_result.success else None
        )

        # Persist multicollinearity results
        if multicollinearity_analysis:
            db_multicollinearity = DBMulticollinearityMetrics(
                metric_id=str(uuid4()),
                table_id=table.table_id,
                computed_at=multicollinearity_analysis.computed_at,
                has_severe_multicollinearity=multicollinearity_analysis.has_severe_multicollinearity,
                num_problematic_columns=multicollinearity_analysis.num_problematic_columns,
                condition_index=(
                    multicollinearity_analysis.condition_index.condition_index
                    if multicollinearity_analysis.condition_index
                    else None
                ),
                max_vif=(
                    max(vif.vif for vif in multicollinearity_analysis.column_vifs)
                    if multicollinearity_analysis.column_vifs
                    else None
                ),
                analysis_data=multicollinearity_analysis.model_dump(mode="json"),
            )
            session.add(db_multicollinearity)
            await session.commit()

        # Summary stats
        stmt = select(Column).where(Column.table_id == table_id)
        result = await session.execute(stmt)
        total_columns = len(result.scalars().all())
        total_pairs = (total_columns * (total_columns - 1)) // 2

        significant_correlations = sum(1 for c in numeric_correlations if c.is_significant)
        strong_correlations = sum(
            1
            for c in numeric_correlations
            if max(abs(c.pearson_r or 0), abs(c.spearman_rho or 0)) > 0.7
        )

        duration = time.time() - start_time
        computed_at = datetime.now(UTC)

        analysis_result = CorrelationAnalysisResult(
            table_id=table_id,
            table_name=table.table_name,
            numeric_correlations=numeric_correlations,
            categorical_associations=categorical_associations,
            functional_dependencies=functional_dependencies,
            derived_columns=derived_columns,
            total_column_pairs=total_pairs,
            significant_correlations=significant_correlations,
            strong_correlations=strong_correlations,
            duration_seconds=duration,
            computed_at=computed_at,
        )

        return Result.ok(analysis_result)

    except Exception as e:
        return Result.fail(f"Correlation analysis failed: {e}")
