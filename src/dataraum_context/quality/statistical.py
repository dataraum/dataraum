"""Statistical quality assessment for columns.

This module implements advanced statistical quality metrics:
- Benford's Law compliance (fraud detection)
- Distribution stability (KS test across time periods)
- Outlier detection (IQR and Isolation Forest)
- Multicollinearity detection (VIF)

These metrics are optional and may require additional dependencies:
- scipy (already in core dependencies)
- scikit-learn (optional: pip install dataraum-context[statistical-quality])
"""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb
import numpy as np
from scipy import stats
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import ColumnRef, Result
from dataraum_context.profiling.models import (
    BenfordAnalysis,
    DistributionStability,
    OutlierDetection,
    StatisticalQualityResult,
)
from dataraum_context.storage.models_v2.core import Column, Table
from dataraum_context.storage.models_v2.statistical_context import (
    StatisticalQualityMetrics as DBStatisticalQualityMetrics,
)

# Type checking imports to avoid hard dependency on scikit-learn
if TYPE_CHECKING:
    pass


# ============================================================================
# Benford's Law Testing
# ============================================================================


async def check_benford_law(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Result[BenfordAnalysis | None]:
    """Test if a numeric column follows Benford's Law.

    Benford's Law states that in many real-world datasets, the first digit
    follows a specific logarithmic distribution. Deviation from this can
    indicate data manipulation or fraud.

    Applicable to: amounts, counts, financial transactions, population data
    Not applicable to: assigned numbers (IDs, phone numbers), uniformly distributed data

    Args:
        table: Table containing the column
        column: Column to test
        duckdb_conn: DuckDB connection

    Returns:
        Result containing BenfordAnalysis or None if not applicable
    """
    try:
        table_name = table.duckdb_path
        col_name = column.column_name

        # Get numeric values
        query = f"""
            SELECT TRY_CAST("{col_name}" AS DOUBLE) as val
            FROM {table_name}
            WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
            AND TRY_CAST("{col_name}" AS DOUBLE) != 0
        """

        values = duckdb_conn.execute(query).fetchnumpy()["val"]

        if len(values) < 100:
            # Not enough data for meaningful Benford's test
            return Result.ok(None)

        # Extract first digits
        first_digits = np.array([int(str(abs(x))[0]) for x in values])

        # Count occurrences of each digit (1-9)
        observed_counts = np.bincount(first_digits, minlength=10)[1:]  # Exclude 0
        observed_freq = observed_counts / len(first_digits)

        # Benford's Law expected frequencies
        expected_freq = np.log10(1 + 1 / np.arange(1, 10))

        # Chi-square test
        chi2, p_value = stats.chisquare(observed_counts, expected_freq * len(first_digits))

        # Convert to float for type safety - use numpy's item() for proper conversion
        chi2_float = float(np.asarray(chi2).item())
        p_value_float = float(np.asarray(p_value).item())

        # Interpretation
        is_compliant = bool(p_value_float > 0.05)
        if is_compliant:
            interpretation = "Follows Benford's Law (no anomalies detected)"
        elif p_value_float > 0.01:
            interpretation = "Weak deviation from Benford's Law (monitor)"
        else:
            interpretation = "Strong deviation from Benford's Law (investigate potential anomalies)"

        # Digit distribution for review
        digit_distribution = {
            str(digit): float(freq) for digit, freq in enumerate(observed_freq, start=1)
        }

        result = BenfordAnalysis(
            chi_square=chi2_float,
            p_value=p_value_float,
            is_compliant=is_compliant,
            digit_distribution=digit_distribution,
            interpretation=interpretation,
        )

        return Result.ok(result)

    except Exception as e:
        return Result.fail(f"Benford's Law test failed: {e}")


# ============================================================================
# Distribution Stability Testing (KS Test)
# ============================================================================


async def check_distribution_stability(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    comparison_days: int = 30,
) -> Result[DistributionStability | None]:
    """Test if column's distribution is stable across time periods.

    Uses Kolmogorov-Smirnov (KS) test to compare distributions between
    recent period and previous period. Significant change may indicate
    data quality issues or business process changes.

    Requires a timestamp column in the table for temporal comparison.

    Args:
        table: Table containing the column
        column: Column to test
        duckdb_conn: DuckDB connection
        comparison_days: Number of days to use for each period

    Returns:
        Result containing DistributionStability or None if not applicable
    """
    try:
        # TODO: This requires temporal column detection
        # For now, return None (not applicable)
        # Will implement after temporal enrichment is integrated
        return Result.ok(None)

    except Exception as e:
        return Result.fail(f"Distribution stability test failed: {e}")


# ============================================================================
# Outlier Detection
# ============================================================================


async def detect_outliers_iqr(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Result[OutlierDetection | None]:
    """Detect outliers using Interquartile Range (IQR) method.

    IQR method:
    - Calculate Q1 (25th percentile) and Q3 (75th percentile)
    - IQR = Q3 - Q1
    - Lower fence = Q1 - 1.5 * IQR
    - Upper fence = Q3 + 1.5 * IQR
    - Values outside fences are outliers

    Args:
        table: Table containing the column
        column: Column to test
        duckdb_conn: DuckDB connection

    Returns:
        Result containing OutlierDetection or None if not numeric
    """
    try:
        table_name = table.duckdb_path
        col_name = column.column_name

        # Calculate quartiles using DuckDB
        query = f"""
            WITH quartiles AS (
                SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) as q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) as q3
                FROM (
                    SELECT TRY_CAST("{col_name}" AS DOUBLE) as val
                    FROM {table_name}
                ) t
                WHERE val IS NOT NULL
            ),
            bounds AS (
                SELECT
                    q1,
                    q3,
                    q3 - q1 as iqr,
                    q1 - 1.5 * (q3 - q1) as lower_fence,
                    q3 + 1.5 * (q3 - q1) as upper_fence
                FROM quartiles
            )
            SELECT
                lower_fence,
                upper_fence,
                (SELECT COUNT(*) FROM (
                    SELECT TRY_CAST("{col_name}" AS DOUBLE) as val FROM {table_name}
                ) t WHERE val IS NOT NULL) as total_count,
                (SELECT COUNT(*) FROM (
                    SELECT TRY_CAST("{col_name}" AS DOUBLE) as val FROM {table_name}
                ) t
                CROSS JOIN bounds
                WHERE val IS NOT NULL AND (val < lower_fence OR val > upper_fence)) as outlier_count
            FROM bounds
        """

        result_row = duckdb_conn.execute(query).fetchone()

        if not result_row or result_row[2] == 0:
            return Result.ok(None)

        lower_fence, upper_fence, total_count, outlier_count = result_row
        outlier_ratio = outlier_count / total_count if total_count > 0 else 0.0

        # Get sample outliers
        sample_query = f"""
            WITH bounds AS (
                SELECT
                    {lower_fence} as lower_fence,
                    {upper_fence} as upper_fence
            )
            SELECT TRY_CAST("{col_name}" AS DOUBLE) as val
            FROM {table_name}
            CROSS JOIN bounds
            WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
            AND (TRY_CAST("{col_name}" AS DOUBLE) < lower_fence
                 OR TRY_CAST("{col_name}" AS DOUBLE) > upper_fence)
            LIMIT 10
        """

        outlier_samples = [
            {"value": float(row[0]), "method": "iqr"}
            for row in duckdb_conn.execute(sample_query).fetchall()
        ]

        result = OutlierDetection(
            # IQR Method
            iqr_lower_fence=float(lower_fence),
            iqr_upper_fence=float(upper_fence),
            iqr_outlier_count=int(outlier_count),
            iqr_outlier_ratio=float(outlier_ratio),
            # Isolation Forest (not computed yet)
            isolation_forest_score=0.0,
            isolation_forest_anomaly_count=0,
            isolation_forest_anomaly_ratio=0.0,
            # Sample outliers
            outlier_samples=outlier_samples if outlier_samples else [],
        )

        return Result.ok(result)

    except Exception as e:
        return Result.fail(f"IQR outlier detection failed: {e}")


async def detect_outliers_isolation_forest(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    contamination: float = 0.05,
) -> tuple[float, int, float, list[dict[str, Any]]] | None:
    """Detect outliers using Isolation Forest (ML-based).

    Isolation Forest is an unsupervised anomaly detection algorithm that
    isolates anomalies by randomly selecting a feature and then randomly
    selecting a split value between the maximum and minimum values of the
    selected feature.

    Requires: scikit-learn (pip install dataraum-context[statistical-quality])

    Args:
        table: Table containing the column
        column: Column to test
        duckdb_conn: DuckDB connection
        contamination: Expected proportion of outliers (default 0.05 = 5%)

    Returns:
        Tuple of (avg_score, anomaly_count, anomaly_ratio, samples) or None if sklearn not available
    """
    try:
        # Check if scikit-learn is available
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            return None  # Skip if not installed

        table_name = table.duckdb_path
        col_name = column.column_name

        # Get numeric values
        query = f"""
            SELECT TRY_CAST("{col_name}" AS DOUBLE) as val
            FROM {table_name}
            WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
        """

        values = duckdb_conn.execute(query).fetchnumpy()["val"]

        if len(values) < 100:
            # Not enough data for meaningful outlier detection
            return None

        # Ensure we have a numpy array (not Categorical)
        if not isinstance(values, np.ndarray):
            values = np.asarray(values)

        # Reshape for sklearn (needs 2D array)
        X = values.reshape(-1, 1)

        # Fit Isolation Forest
        iso_forest = IsolationForest(contamination=contamination, random_state=42)  # type: ignore[arg-type]
        predictions = iso_forest.fit_predict(X)
        scores = iso_forest.score_samples(X)

        # -1 = outlier, 1 = inlier
        outlier_mask = predictions == -1
        outlier_count = int(np.sum(outlier_mask))
        outlier_ratio = float(outlier_count / len(values))
        avg_score = float(np.mean(scores))

        # Get sample outliers with their scores
        outlier_indices = np.where(outlier_mask)[0][:10]  # Top 10
        outlier_samples = [
            {
                "value": float(values[idx]),
                "method": "isolation_forest",
                "score": float(scores[idx]),
            }
            for idx in outlier_indices
        ]

        return (avg_score, outlier_count, outlier_ratio, outlier_samples)

    except Exception:
        return None


# ============================================================================
# Main Statistical Quality Assessment
# ============================================================================


async def assess_statistical_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[list[StatisticalQualityResult]]:
    """Assess statistical quality for all numeric columns in a table.

    This function:
    1. Runs quality tests (Benford, outliers, stability, VIF)
    2. Generates quality issues
    3. Computes overall quality score
    4. Builds StatisticalQualityResult (Pydantic source of truth)
    5. Persists using hybrid storage (structured + JSONB)

    Args:
        table_id: Table ID to assess
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing list of StatisticalQualityResult objects
    """
    try:
        # Get table from metadata
        table = await session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        # Get all columns
        from sqlalchemy import select

        stmt = select(Column).where(Column.table_id == table_id)
        query_result = await session.execute(stmt)
        columns = query_result.scalars().all()

        results = []

        for column in columns:
            # Skip non-numeric columns for statistical quality tests
            if column.resolved_type not in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]:
                continue

            # Run quality assessment for this column
            quality_result = await _assess_column_quality(
                table=table,
                column=column,
                duckdb_conn=duckdb_conn,
                session=session,
            )

            if quality_result.success and quality_result.value:
                results.append(quality_result.value)

        return Result.ok(results)

    except Exception as e:
        return Result.fail(f"Statistical quality assessment failed: {e}")


async def _assess_column_quality(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[StatisticalQualityResult]:
    """Assess statistical quality for a single column.

    Follows the gold-standard hybrid storage pattern from temporal enrichment.
    """
    try:
        computed_at = datetime.now(UTC)

        # Run Benford's Law test
        benford_result = await check_benford_law(table, column, duckdb_conn)
        benford_analysis = benford_result.value if benford_result.success else None

        # Run distribution stability test
        stability_result = await check_distribution_stability(table, column, duckdb_conn)
        dist_stability = stability_result.value if stability_result.success else None

        # Run IQR outlier detection
        iqr_result = await detect_outliers_iqr(table, column, duckdb_conn)
        outlier_detection = iqr_result.value if iqr_result.success else None

        # Run Isolation Forest outlier detection and merge into OutlierDetection
        iso_forest_data = await detect_outliers_isolation_forest(table, column, duckdb_conn)
        if iso_forest_data and outlier_detection:
            avg_score, anomaly_count, anomaly_ratio, iso_samples = iso_forest_data
            # Update the outlier_detection with Isolation Forest results
            outlier_detection.isolation_forest_score = avg_score
            outlier_detection.isolation_forest_anomaly_count = anomaly_count
            outlier_detection.isolation_forest_anomaly_ratio = anomaly_ratio
            # Merge samples
            if iso_samples:
                outlier_detection.outlier_samples.extend(iso_samples)

        # VIF computation (TODO: optimize by computing for all columns at once)
        vif_score = None
        vif_correlated_columns = []
        # Skipping VIF for now - requires correlation with other columns

        # Generate quality issues
        quality_issues = _generate_statistical_quality_issues(
            benford_analysis=benford_analysis,
            outlier_detection=outlier_detection,
            dist_stability=dist_stability,
        )

        # Compute comprehensive quality score
        quality_score = _compute_statistical_quality_score(
            benford_analysis=benford_analysis,
            outlier_detection=outlier_detection,
            issue_count=len(quality_issues),
        )

        # Build StatisticalQualityResult (Pydantic source of truth)
        quality_result = StatisticalQualityResult(
            column_id=column.column_id,
            column_ref=ColumnRef(
                table_name=table.table_name,
                column_name=column.column_name,
            ),
            benford_analysis=benford_analysis,
            outlier_detection=outlier_detection,
            distribution_stability=dist_stability,
            vif_score=vif_score,
            vif_correlated_columns=vif_correlated_columns,
            quality_score=quality_score,
            quality_issues=quality_issues,
        )

        # Persist using hybrid storage
        db_metric = DBStatisticalQualityMetrics(
            metric_id=str(uuid4()),
            column_id=column.column_id,
            computed_at=computed_at,
            # STRUCTURED: Queryable quality indicators
            quality_score=quality_score,
            benford_compliant=benford_analysis.is_compliant if benford_analysis else None,
            distribution_stable=dist_stability.is_stable if dist_stability else None,
            has_outliers=(outlier_detection.iqr_outlier_ratio > 0.05)
            if outlier_detection
            else None,
            iqr_outlier_ratio=outlier_detection.iqr_outlier_ratio if outlier_detection else None,
            isolation_forest_anomaly_ratio=outlier_detection.isolation_forest_anomaly_ratio
            if outlier_detection
            else None,
            # JSONB: Full Pydantic model (zero mapping!)
            quality_data=quality_result.model_dump(mode="json"),
        )
        session.add(db_metric)
        await session.commit()

        return Result.ok(quality_result)

    except Exception as e:
        return Result.fail(f"Failed to assess column quality for {column.column_name}: {e}")


def _generate_statistical_quality_issues(
    benford_analysis: BenfordAnalysis | None,
    outlier_detection: OutlierDetection | None,
    dist_stability: DistributionStability | None,
) -> list[dict[str, Any]]:
    """Generate quality issues from statistical analysis results."""
    issues = []

    # Benford's Law violation
    if benford_analysis and not benford_analysis.is_compliant:
        severity = "warning" if benford_analysis.p_value > 0.01 else "critical"
        issues.append(
            {
                "issue_type": "benford_violation",
                "severity": severity,
                "description": benford_analysis.interpretation,
                "evidence": {
                    "chi_square": benford_analysis.chi_square,
                    "p_value": benford_analysis.p_value,
                },
            }
        )

    # High outlier ratio (IQR)
    if outlier_detection and outlier_detection.iqr_outlier_ratio > 0.05:
        severity = "warning" if outlier_detection.iqr_outlier_ratio < 0.10 else "critical"
        issues.append(
            {
                "issue_type": "outliers_iqr",
                "severity": severity,
                "description": f"{outlier_detection.iqr_outlier_ratio * 100:.1f}% of values are outliers (IQR method)",
                "evidence": {
                    "outlier_count": outlier_detection.iqr_outlier_count,
                    "outlier_ratio": outlier_detection.iqr_outlier_ratio,
                },
            }
        )

    # High anomaly ratio (Isolation Forest)
    if (
        outlier_detection
        and outlier_detection.isolation_forest_anomaly_ratio > 0.0
        and outlier_detection.isolation_forest_anomaly_ratio > 0.05
    ):
        severity = (
            "warning" if outlier_detection.isolation_forest_anomaly_ratio < 0.10 else "critical"
        )
        issues.append(
            {
                "issue_type": "outliers_isolation_forest",
                "severity": severity,
                "description": f"{outlier_detection.isolation_forest_anomaly_ratio * 100:.1f}% of values are anomalies (Isolation Forest)",
                "evidence": {
                    "anomaly_count": outlier_detection.isolation_forest_anomaly_count,
                    "anomaly_ratio": outlier_detection.isolation_forest_anomaly_ratio,
                },
            }
        )

    # Distribution instability
    if dist_stability and not dist_stability.is_stable:
        issues.append(
            {
                "issue_type": "distribution_shift",
                "severity": "warning",
                "description": "Distribution has shifted significantly over time",
                "evidence": {
                    "ks_statistic": dist_stability.ks_statistic,
                    "p_value": dist_stability.ks_p_value,
                },
            }
        )

    return issues


def _compute_statistical_quality_score(
    benford_analysis: BenfordAnalysis | None,
    outlier_detection: OutlierDetection | None,
    issue_count: int,
) -> float:
    """Compute comprehensive statistical quality score (0-1)."""
    score = 1.0

    # Benford's Law penalty
    if benford_analysis and not benford_analysis.is_compliant:
        # Stronger penalty for more significant deviation
        if benford_analysis.p_value < 0.01:
            score -= 0.3
        else:
            score -= 0.15

    # Outlier penalty (IQR)
    if outlier_detection and outlier_detection.iqr_outlier_ratio > 0.05:
        if outlier_detection.iqr_outlier_ratio > 0.20:
            score -= 0.4  # Very high outlier ratio
        elif outlier_detection.iqr_outlier_ratio > 0.10:
            score -= 0.3
        else:
            score -= 0.15

    # Issue count penalty
    if issue_count > 0:
        score -= issue_count * 0.05  # 5% per issue

    return max(0.0, min(1.0, score))
