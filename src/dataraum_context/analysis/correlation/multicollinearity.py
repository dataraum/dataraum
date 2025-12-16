"""Multicollinearity detection (VIF, Tolerance, Condition Index).

Computes multicollinearity metrics:
1. VIF (Variance Inflation Factor) for each column
2. Tolerance (1/VIF) for each column
3. Condition Index for the table (eigenvalue-based)
4. Variance Decomposition Proportions (VDP) for dependency group identification

VALUE FOR DATA QUALITY (not just modeling):
- Completeness/Relevance: Identifies redundant features (e.g., "age_years" vs "age_days")
- Consistency/Integrity: High correlation may signal logical dependencies; outliers may
  indicate data entry errors
- Efficiency: Removing highly correlated variables streamlines storage and processing

LIMITATIONS - USE WITH CAUTION ON RAW DATA:
- Outliers can artificially inflate/deflate correlations → clean data first
- Missing values cause issues → handle NULLs before analysis
- VIF is for continuous numerical variables only → categorical needs different treatment
- Spurious correlations appear in large datasets → apply domain knowledge
- Only detects LINEAR relationships → non-linear dependencies go unnoticed

RECOMMENDATION:
Run multicollinearity analysis AFTER data cleaning (outlier handling, null imputation)
as part of a broader data quality assessment. Currently disabled by default in the
pipeline; enable when data cleaning stages are in place.
"""

from datetime import UTC, datetime

import duckdb
import numpy as np
from sklearn.linear_model import LinearRegression
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.models import (
    ColumnVIF,
    ConditionIndexAnalysis,
    DependencyGroup,
    MulticollinearityAnalysis,
)
from dataraum_context.core.models.base import ColumnRef, Result
from dataraum_context.storage import Column, Table


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
