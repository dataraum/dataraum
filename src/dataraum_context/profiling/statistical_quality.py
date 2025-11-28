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
from typing import TYPE_CHECKING
from uuid import uuid4

import duckdb
import numpy as np
from scipy import stats
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.config import get_settings
from dataraum_context.core.models import Result
from dataraum_context.core.models.statistical import (
    BenfordTestResult,
    DistributionStabilityResult,
    OutlierDetectionResult,
    QualityIssue,
    StatisticalQualityMetrics,
    StatisticalQualityResult,
    VIFResult,
)
from dataraum_context.storage.models_v2.core import Column, Table
from dataraum_context.storage.models_v2.statistical_context import (
    StatisticalQualityMetrics as DBStatisticalQualityMetrics,
)

# Type checking imports to avoid hard dependency on scikit-learn
if TYPE_CHECKING:
    from sklearn.ensemble import IsolationForest


# ============================================================================
# Benford's Law Testing
# ============================================================================


async def check_benford_law(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Result[BenfordTestResult | None]:
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
        Result containing BenfordTestResult or None if not applicable
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

        # Interpretation
        compliant = p_value > 0.05
        if compliant:
            interpretation = "Follows Benford's Law (no anomalies detected)"
        elif p_value > 0.01:
            interpretation = "Weak deviation from Benford's Law (monitor)"
        else:
            interpretation = "Strong deviation from Benford's Law (investigate potential anomalies)"

        # Digit distribution for review
        digit_distribution = {
            int(digit): float(freq) for digit, freq in enumerate(observed_freq, start=1)
        }

        result = BenfordTestResult(
            chi_square=float(chi2),
            p_value=float(p_value),
            compliant=compliant,
            interpretation=interpretation,
            digit_distribution=digit_distribution,
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
) -> Result[DistributionStabilityResult | None]:
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
        Result containing DistributionStabilityResult or None if not applicable
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
) -> Result[OutlierDetectionResult | None]:
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
        Result containing OutlierDetectionResult or None if not numeric
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

        result = OutlierDetectionResult(
            method="iqr",
            outlier_count=int(outlier_count),
            outlier_ratio=float(outlier_ratio),
            lower_fence=float(lower_fence),
            upper_fence=float(upper_fence),
            outlier_samples=outlier_samples if outlier_samples else None,
        )

        return Result.ok(result)

    except Exception as e:
        return Result.fail(f"IQR outlier detection failed: {e}")


async def detect_outliers_isolation_forest(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    contamination: float = 0.05,
) -> Result[OutlierDetectionResult | None]:
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
        Result containing OutlierDetectionResult or None if sklearn not available
    """
    try:
        # Check if scikit-learn is available
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            return Result.ok(None)  # Skip if not installed

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
            return Result.ok(None)

        # Reshape for sklearn (needs 2D array)
        X = values.reshape(-1, 1)

        # Fit Isolation Forest
        iso_forest = IsolationForest(contamination=contamination, random_state=42)
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

        result = OutlierDetectionResult(
            method="isolation_forest",
            outlier_count=outlier_count,
            outlier_ratio=outlier_ratio,
            average_anomaly_score=avg_score,
            outlier_samples=outlier_samples if outlier_samples else None,
        )

        return Result.ok(result)

    except Exception as e:
        return Result.fail(f"Isolation Forest outlier detection failed: {e}")


# ============================================================================
# Multicollinearity Detection (VIF)
# ============================================================================


async def compute_vif(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> Result[VIFResult | None]:
    """Compute Variance Inflation Factor (VIF) for multicollinearity detection.

    VIF measures how much the variance of a regression coefficient is inflated
    due to multicollinearity with other predictors.

    Interpretation:
    - VIF = 1: No correlation
    - VIF < 5: Low correlation
    - VIF 5-10: Moderate correlation
    - VIF > 10: High multicollinearity (problematic)

    This requires computing correlations with all other numeric columns in the table.

    Args:
        table: Table containing the column
        column: Column to test
        duckdb_conn: DuckDB connection

    Returns:
        Result containing VIFResult or None if not enough numeric columns
    """
    try:
        # TODO: This requires correlation matrix computation across all columns
        # Will implement after correlation analysis module is created
        return Result.ok(None)

    except Exception as e:
        return Result.fail(f"VIF computation failed: {e}")


# ============================================================================
# Main Statistical Quality Assessment
# ============================================================================


async def assess_statistical_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[StatisticalQualityResult]:
    """Assess statistical quality for all columns in a table.

    Runs all applicable quality tests:
    - Benford's Law (for numeric columns)
    - Distribution stability (if temporal data available)
    - Outlier detection (IQR and Isolation Forest)
    - VIF (if multiple numeric columns)

    Args:
        table_id: Table ID to assess
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing StatisticalQualityResult
    """
    import time

    start_time = time.time()

    try:
        # Get table from metadata
        table = await session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        # Get all columns
        from sqlalchemy import select

        stmt = select(Column).where(Column.table_id == table_id)
        result = await session.execute(stmt)
        columns = result.scalars().all()

        all_metrics = []

        for column in columns:
            # Skip non-numeric columns for most tests
            if column.resolved_type not in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]:
                continue

            computed_at = datetime.now(UTC)

            # Run Benford's Law test
            benford_result = await check_benford_law(table, column, duckdb_conn)
            benford_test = benford_result.value if benford_result.success else None

            # Run distribution stability test
            stability_result = await check_distribution_stability(table, column, duckdb_conn)
            dist_stability = stability_result.value if stability_result.success else None

            # Run IQR outlier detection
            iqr_result = await detect_outliers_iqr(table, column, duckdb_conn)
            iqr_outliers = iqr_result.value if iqr_result.success else None

            # Run Isolation Forest outlier detection
            iso_result = await detect_outliers_isolation_forest(table, column, duckdb_conn)
            iso_outliers = iso_result.value if iso_result.success else None

            # Run VIF computation
            vif_result_obj = await compute_vif(table, column, duckdb_conn)
            vif_res = vif_result_obj.value if vif_result_obj.success else None

            # Aggregate quality issues
            quality_issues = []

            if benford_test and not benford_test.compliant:
                quality_issues.append(
                    QualityIssue(
                        issue_type="benford_violation",
                        severity="warning" if benford_test.p_value > 0.01 else "critical",
                        description=benford_test.interpretation,
                        evidence={
                            "chi_square": benford_test.chi_square,
                            "p_value": benford_test.p_value,
                        },
                    )
                )

            if iqr_outliers and iqr_outliers.outlier_ratio > 0.05:
                quality_issues.append(
                    QualityIssue(
                        issue_type="outliers",
                        severity="warning" if iqr_outliers.outlier_ratio < 0.10 else "critical",
                        description=f"{iqr_outliers.outlier_ratio * 100:.1f}% of values are outliers (IQR method)",
                        evidence={"outlier_count": iqr_outliers.outlier_count},
                    )
                )

            # Compute overall quality score (0-1)
            quality_score = 1.0
            if benford_test and not benford_test.compliant:
                quality_score -= 0.3
            if iqr_outliers and iqr_outliers.outlier_ratio > 0.10:
                quality_score -= 0.3

            quality_score = max(0.0, quality_score)

            # Create metrics object
            metric = StatisticalQualityMetrics(
                metric_id=str(uuid4()),
                column_id=column.column_id,
                computed_at=computed_at,
                benford_test=benford_test,
                distribution_stability=dist_stability,
                outlier_detection=iqr_outliers or iso_outliers,
                vif_result=vif_res,
                quality_score=quality_score,
                quality_issues=quality_issues,
            )

            all_metrics.append(metric)

            # Store in database
            db_metric = DBStatisticalQualityMetrics(
                metric_id=metric.metric_id,
                column_id=metric.column_id,
                computed_at=metric.computed_at,
                # Benford
                benford_chi_square=benford_test.chi_square if benford_test else None,
                benford_p_value=benford_test.p_value if benford_test else None,
                benford_compliant=benford_test.compliant if benford_test else None,
                benford_interpretation=benford_test.interpretation if benford_test else None,
                benford_digit_distribution=benford_test.digit_distribution
                if benford_test
                else None,
                # Outliers
                iqr_outlier_count=iqr_outliers.outlier_count if iqr_outliers else None,
                iqr_outlier_ratio=iqr_outliers.outlier_ratio if iqr_outliers else None,
                iqr_lower_fence=iqr_outliers.lower_fence if iqr_outliers else None,
                iqr_upper_fence=iqr_outliers.upper_fence if iqr_outliers else None,
                isolation_forest_anomaly_count=iso_outliers.outlier_count if iso_outliers else None,
                isolation_forest_anomaly_ratio=iso_outliers.outlier_ratio if iso_outliers else None,
                isolation_forest_score=iso_outliers.average_anomaly_score if iso_outliers else None,
                outlier_samples=(iqr_outliers.outlier_samples if iqr_outliers else None)
                or (iso_outliers.outlier_samples if iso_outliers else None),
                # VIF
                vif_score=vif_res.vif_score if vif_res else None,
                vif_correlated_columns=vif_res.correlated_columns if vif_res else None,
                # Overall
                quality_score=metric.quality_score,
                quality_issues=[issue.dict() for issue in metric.quality_issues],
            )
            session.add(db_metric)

        await session.commit()

        duration = time.time() - start_time

        result = StatisticalQualityResult(
            metrics=all_metrics,
            duration_seconds=duration,
        )

        return Result.ok(result)

    except Exception as e:
        return Result.fail(f"Statistical quality assessment failed: {e}")
