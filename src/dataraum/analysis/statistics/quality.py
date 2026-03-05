"""Statistical quality assessment for columns.

This module implements focused statistical quality metrics:
- Benford's Law compliance (fraud detection)
- Outlier detection (IQR and Modified Z-Score)

Note: Distribution stability (KS test) is in analysis/temporal module.

Dependencies:
- scipy (already in core dependencies)

Uses parallel processing for large tables to speed up assessment.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import duckdb
import numpy as np
from scipy import stats
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.statistics.models import (
    BenfordAnalysis,
    OutlierDetection,
    StatisticalQualityResult,
)
from dataraum.analysis.statistics.quality_db_models import (
    StatisticalQualityMetrics as DBStatisticalQualityMetrics,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import ColumnRef, Result
from dataraum.storage import Column, Table

logger = get_logger(__name__)


# ============================================================================
# Benford's Law Testing
# ============================================================================


def check_benford_law(
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
# Outlier Detection
# ============================================================================


def detect_outliers_iqr(
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
            ORDER BY ABS(TRY_CAST("{col_name}" AS DOUBLE) - (lower_fence + upper_fence) / 2.0) DESC
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
            # Sample outliers
            outlier_samples=outlier_samples if outlier_samples else [],
        )

        return Result.ok(result)

    except Exception as e:
        return Result.fail(f"IQR outlier detection failed: {e}")


def detect_outliers_zscore(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    threshold: float = 3.5,
) -> tuple[int, float, list[dict[str, Any]]] | None:
    """Detect outliers using Modified Z-Score (MAD-based).

    The modified Z-score uses the median and MAD (Median Absolute Deviation)
    instead of mean and standard deviation, making it robust to the outliers
    it's trying to detect.

    Formula: modified_z = 0.6745 * (x - median) / MAD
    Values where |modified_z| > threshold are flagged as outliers.

    Args:
        table: Table containing the column
        column: Column to test
        duckdb_conn: DuckDB connection
        threshold: Modified Z-score threshold (default 3.5)

    Returns:
        Tuple of (outlier_count, outlier_ratio, samples) or None if not applicable
    """
    try:
        table_name = table.duckdb_path
        col_name = column.column_name

        # Compute median and MAD in a single pass, then count outliers
        query = f"""
            WITH base AS (
                SELECT TRY_CAST("{col_name}" AS DOUBLE) AS val
                FROM {table_name}
                WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
            ),
            stats AS (
                SELECT
                    MEDIAN(val) AS med,
                    MEDIAN(ABS(val - (SELECT MEDIAN(val) FROM base))) AS mad,
                    COUNT(*) AS total
                FROM base
            )
            SELECT
                stats.med,
                stats.mad,
                stats.total,
                (SELECT COUNT(*) FROM base, stats
                 WHERE ABS(0.6745 * (base.val - stats.med) / NULLIF(stats.mad, 0)) > {threshold}
                ) AS outlier_count
            FROM stats
        """

        result_row = duckdb_conn.execute(query).fetchone()

        if not result_row or result_row[2] == 0:
            return None

        median_val, mad_val, total_count, outlier_count = result_row

        # MAD of 0 means all values are identical — no outliers
        if mad_val is None or mad_val == 0:
            return None

        outlier_ratio = outlier_count / total_count if total_count > 0 else 0.0

        # Get sample outliers
        sample_query = f"""
            WITH base AS (
                SELECT TRY_CAST("{col_name}" AS DOUBLE) AS val
                FROM {table_name}
                WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
            )
            SELECT val, ABS(0.6745 * (val - {median_val}) / {mad_val}) AS zscore
            FROM base
            WHERE ABS(0.6745 * (val - {median_val}) / {mad_val}) > {threshold}
            ORDER BY zscore DESC
            LIMIT 10
        """

        outlier_samples = [
            {"value": float(row[0]), "method": "zscore", "score": float(row[1])}
            for row in duckdb_conn.execute(sample_query).fetchall()
        ]

        return (int(outlier_count), float(outlier_ratio), outlier_samples)

    except Exception as e:
        logger.debug("zscore_detection_failed", column=column.column_name, error=str(e))
        return None


# ============================================================================
# Main Statistical Quality Assessment
# ============================================================================


def _assess_column_quality_parallel(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    table_duckdb_path: str,
    column_id: str,
    column_name: str,
    column_resolved_type: str,
) -> tuple[str, str, BenfordAnalysis | None, OutlierDetection | None] | None:
    """Assess statistical quality for a single column in a worker thread.

    Runs in its own thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.
    Returns the raw analysis results for the main thread to persist.
    """

    # Create minimal Table/Column-like objects for the analysis functions
    class TableProxy:
        def __init__(self, name: str, duckdb_path: str):
            self.table_name = name
            self.duckdb_path = duckdb_path

    class ColumnProxy:
        def __init__(self, col_id: str, col_name: str, resolved_type: str):
            self.column_id = col_id
            self.column_name = col_name
            self.resolved_type = resolved_type

    table = TableProxy(table_name, table_duckdb_path)
    column = ColumnProxy(column_id, column_name, column_resolved_type)

    cursor = duckdb_conn.cursor()
    try:
        # Run Benford's Law test
        benford_result = check_benford_law(table, column, cursor)  # type: ignore[arg-type]
        benford_analysis = benford_result.value if benford_result.success else None

        # Run IQR outlier detection
        iqr_result = detect_outliers_iqr(table, column, cursor)  # type: ignore[arg-type]
        outlier_detection = iqr_result.value if iqr_result.success else None

        # Run Modified Z-Score outlier detection and merge into OutlierDetection
        zscore_data = detect_outliers_zscore(table, column, cursor)  # type: ignore[arg-type]
        if zscore_data and outlier_detection:
            zscore_count, zscore_ratio, zscore_samples = zscore_data
            outlier_detection.zscore_outlier_count = zscore_count
            outlier_detection.zscore_outlier_ratio = zscore_ratio
            if zscore_samples:
                outlier_detection.outlier_samples.extend(zscore_samples)

        return (column_id, column_name, benford_analysis, outlier_detection)
    except Exception as e:
        logger.warning("column_quality_assessment_failed", column=column_name, error=str(e))
        return None
    finally:
        cursor.close()


def assess_statistical_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    max_workers: int = 4,
) -> Result[list[StatisticalQualityResult]]:
    """Assess statistical quality for all numeric columns in a table.

    This function:
    1. Runs quality tests (Benford, outliers via IQR and Isolation Forest)
    2. Generates quality issues
    3. Computes overall quality score
    4. Builds StatisticalQualityResult (Pydantic source of truth)
    5. Persists using hybrid storage (structured + JSONB)

    Uses parallel processing for file-based DBs to speed up assessment.

    Args:
        table_id: Table ID to assess
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        max_workers: Maximum parallel workers

    Returns:
        Result containing list of StatisticalQualityResult objects
    """
    try:
        # session.get() checks the identity map first, so pending objects
        # added via session.add() in this session are found without flush.
        table = session.get(Table, str(table_id))

        # Fallback: check session.new for pending objects not yet in identity map
        # (can happen with autoflush=False when objects were added but PK not indexed)
        if not table:
            for obj in session.new:
                if isinstance(obj, Table) and obj.table_id == str(table_id):
                    table = obj
                    break

        if not table:
            return Result.fail(f"Table not found: {table_id}")

        # Query columns from DB. With autoflush=False, session.execute() hits
        # SQLite which can't see unflushed objects. We must also check session.new
        # for pending Columns (non-PK query — identity map doesn't help here).
        stmt = select(Column).where(Column.table_id == table_id)
        query_result = session.execute(stmt)
        columns = list(query_result.scalars().all())

        pending_columns = [
            obj for obj in session.new if isinstance(obj, Column) and obj.table_id == table_id
        ]
        if pending_columns:
            existing_ids = {c.column_id for c in columns}
            for pc in pending_columns:
                if pc.column_id not in existing_ids:
                    columns.append(pc)

        # Filter to numeric columns
        numeric_columns = [
            c for c in columns if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
        ]

        if not numeric_columns:
            return Result.ok([])

        logger.debug(
            "assessing_statistical_quality",
            table=table.table_name,
            numeric_columns=len(numeric_columns),
        )

        table_duckdb_path = table.duckdb_path
        if not table_duckdb_path:
            return Result.fail("Table has no DuckDB path")

        results: list[StatisticalQualityResult] = []
        computed_at = datetime.now(UTC)

        # Use parallel processing with cursors from shared connection
        # DuckDB cursors are thread-safe for read operations
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _assess_column_quality_parallel,
                    duckdb_conn,
                    table.table_name,
                    table_duckdb_path,
                    column.column_id,
                    column.column_name,
                    column.resolved_type,  # type: ignore[arg-type]  # filtered to numeric above
                )
                for column in numeric_columns
            ]

            for future in futures:
                result = future.result()
                if result:
                    column_id, column_name, benford_analysis, outlier_detection = result

                    # Generate quality issues
                    quality_issues = _generate_statistical_quality_issues(
                        benford_analysis=benford_analysis,
                        outlier_detection=outlier_detection,
                    )

                    # Build StatisticalQualityResult
                    quality_result = StatisticalQualityResult(
                        column_id=column_id,
                        column_ref=ColumnRef(
                            table_name=table.table_name,
                            column_name=column_name,
                        ),
                        benford_analysis=benford_analysis,
                        outlier_detection=outlier_detection,
                        quality_issues=quality_issues,
                    )
                    results.append(quality_result)

                    # Persist using hybrid storage (sequential - SQLite writes)
                    db_metric = DBStatisticalQualityMetrics(
                        metric_id=str(uuid4()),
                        column_id=column_id,
                        computed_at=computed_at,
                        benford_compliant=benford_analysis.is_compliant
                        if benford_analysis
                        else None,
                        has_outliers=(outlier_detection.iqr_outlier_ratio > 0.05)
                        if outlier_detection
                        else None,
                        iqr_outlier_ratio=outlier_detection.iqr_outlier_ratio
                        if outlier_detection
                        else None,
                        zscore_outlier_ratio=outlier_detection.zscore_outlier_ratio
                        if outlier_detection
                        else None,
                        quality_data=quality_result.model_dump(mode="json"),
                    )
                    session.add(db_metric)

        logger.debug(
            "statistical_quality_assessment_complete",
            table=table.table_name,
            results=len(results),
        )

        return Result.ok(results)

    except Exception as e:
        logger.error("statistical_quality_assessment_failed", error=str(e))
        return Result.fail(f"Statistical quality assessment failed: {e}")


def _generate_statistical_quality_issues(
    benford_analysis: BenfordAnalysis | None,
    outlier_detection: OutlierDetection | None,
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

    # High outlier ratio (Modified Z-Score)
    if outlier_detection and outlier_detection.zscore_outlier_ratio > 0.05:
        severity = "warning" if outlier_detection.zscore_outlier_ratio < 0.10 else "critical"
        issues.append(
            {
                "issue_type": "outliers_zscore",
                "severity": severity,
                "description": f"{outlier_detection.zscore_outlier_ratio * 100:.1f}% of values are outliers (Modified Z-Score)",
                "evidence": {
                    "outlier_count": outlier_detection.zscore_outlier_count,
                    "outlier_ratio": outlier_detection.zscore_outlier_ratio,
                },
            }
        )

    return issues
