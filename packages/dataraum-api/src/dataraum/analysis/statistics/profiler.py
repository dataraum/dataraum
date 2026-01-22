"""Statistics profiler for typed tables.

Computes ALL row-based statistics on CLEAN data after type resolution:
- Basic counts (total, null, distinct, cardinality)
- String stats (min/max/avg length)
- Top values (frequency analysis)
- Numeric stats (min, max, mean, stddev, skewness, kurtosis, cv)
- Percentiles (p01, p25, p50, p75, p99)
- Histograms

This REQUIRES a typed table (layer="typed"). Row-based statistics
must be computed on clean data to be accurate.

Uses parallel processing for large tables to speed up profiling.

Note: Correlation analysis is handled separately by analysis/correlation module.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.statistics.db_models import (
    StatisticalProfile as DBColumnProfile,
)
from dataraum.analysis.statistics.models import (
    ColumnProfile,
    HistogramBucket,
    NumericStats,
    StatisticsProfileResult,
    StringStats,
    ValueCount,
)
from dataraum.core.config import get_settings
from dataraum.core.logging import get_logger
from dataraum.core.models.base import ColumnRef, Result
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


def _profile_column_stats_parallel(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    table_duckdb_path: str,
    column_id: str,
    column_name: str,
    resolved_type: str,
    profiled_at: datetime,
    top_k: int,
) -> ColumnProfile | None:
    """Profile a single column in a worker thread.

    Runs in its own thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.
    Returns ColumnProfile directly for the main thread to persist.
    """
    cursor = duckdb_conn.cursor()
    try:
        # Basic counts
        count_query = f"""
            SELECT
                COUNT(*) as total_count,
                COUNT("{column_name}") as non_null_count,
                COUNT(DISTINCT "{column_name}") as distinct_count
            FROM "{table_duckdb_path}"
        """

        counts = cursor.execute(count_query).fetchone()
        total_count = counts[0] if counts and len(counts) > 0 else 0
        non_null_count = counts[1] if counts and len(counts) > 1 else 0
        distinct_count = counts[2] if counts and len(counts) > 2 else 0
        null_count = total_count - non_null_count

        null_ratio = null_count / total_count if total_count > 0 else 0.0
        cardinality_ratio = distinct_count / non_null_count if non_null_count > 0 else 0.0

        # Numeric stats
        numeric_stats = None
        if resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]:
            try:
                numeric_query = f"""
                    SELECT
                        MIN("{column_name}") as min_val,
                        MAX("{column_name}") as max_val,
                        AVG("{column_name}"::DOUBLE) as mean_val,
                        STDDEV("{column_name}"::DOUBLE) as stddev_val,
                        SKEWNESS("{column_name}"::DOUBLE) as skewness_val,
                        KURTOSIS("{column_name}"::DOUBLE) as kurtosis_val,
                        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY "{column_name}") as p01,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column_name}") as p25,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "{column_name}") as p50,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column_name}") as p75,
                        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "{column_name}") as p99
                    FROM "{table_duckdb_path}"
                    WHERE "{column_name}" IS NOT NULL
                """
                numeric_row = cursor.execute(numeric_query).fetchone()

                if numeric_row and numeric_row[0] is not None:
                    mean_val = float(numeric_row[2]) if numeric_row[2] is not None else 0.0
                    stddev_val = float(numeric_row[3]) if numeric_row[3] is not None else 0.0
                    cv_val = stddev_val / abs(mean_val) if mean_val != 0 else None

                    numeric_stats = NumericStats(
                        min_value=float(numeric_row[0]),
                        max_value=float(numeric_row[1]),
                        mean=mean_val,
                        stddev=stddev_val,
                        skewness=(float(numeric_row[4]) if numeric_row[4] is not None else None),
                        kurtosis=(float(numeric_row[5]) if numeric_row[5] is not None else None),
                        cv=cv_val,
                        percentiles={
                            "p01": (float(numeric_row[6]) if numeric_row[6] is not None else None),
                            "p25": (float(numeric_row[7]) if numeric_row[7] is not None else None),
                            "p50": (float(numeric_row[8]) if numeric_row[8] is not None else None),
                            "p75": (float(numeric_row[9]) if numeric_row[9] is not None else None),
                            "p99": (
                                float(numeric_row[10]) if numeric_row[10] is not None else None
                            ),
                        },
                    )
            except Exception as e:
                logger.debug("numeric_stats_failed", column=column_name, error=str(e))

        # String stats
        string_stats = None
        if resolved_type == "VARCHAR":
            try:
                string_query = f"""
                    SELECT
                        MIN(LENGTH("{column_name}")) as min_len,
                        MAX(LENGTH("{column_name}")) as max_len,
                        AVG(LENGTH("{column_name}")) as avg_len
                    FROM "{table_duckdb_path}"
                    WHERE "{column_name}" IS NOT NULL
                """
                string_row = cursor.execute(string_query).fetchone()

                if string_row and string_row[0] is not None:
                    string_stats = StringStats(
                        min_length=int(string_row[0]),
                        max_length=int(string_row[1]),
                        avg_length=float(string_row[2]),
                    )
            except Exception as e:
                logger.debug("string_stats_failed", column=column_name, error=str(e))

        # Top values
        top_values = []
        try:
            top_values_query = f"""
                SELECT
                    "{column_name}" as value,
                    COUNT(*) as count,
                    (COUNT(*) * 100.0 / {total_count}) as percentage
                FROM "{table_duckdb_path}"
                WHERE "{column_name}" IS NOT NULL
                GROUP BY "{column_name}"
                ORDER BY count DESC
                LIMIT {top_k}
            """
            top_values_rows = cursor.execute(top_values_query).fetchall()

            for value, count, percentage in top_values_rows:
                top_values.append(
                    ValueCount(value=value, count=int(count), percentage=float(percentage))
                )
        except Exception as e:
            logger.debug("top_values_failed", column=column_name, error=str(e))

        # Histogram
        histogram = []
        if numeric_stats is not None:
            try:
                num_bins = 20
                min_val = numeric_stats.min_value
                max_val = numeric_stats.max_value

                if min_val != max_val and non_null_count > 0:
                    range_width = max_val - min_val
                    bucket_width = range_width / num_bins

                    histogram_query = f"""
                        WITH bucketed AS (
                            SELECT
                                WIDTH_BUCKET(
                                    "{column_name}"::DOUBLE,
                                    {min_val},
                                    {max_val},
                                    {num_bins}
                                ) as bucket_num
                            FROM "{table_duckdb_path}"
                            WHERE "{column_name}" IS NOT NULL
                        )
                        SELECT
                            bucket_num,
                            COUNT(*) as count
                        FROM bucketed
                        WHERE bucket_num > 0 AND bucket_num <= {num_bins}
                        GROUP BY bucket_num
                        ORDER BY bucket_num
                    """

                    histogram_rows = cursor.execute(histogram_query).fetchall()

                    for bucket_num, count in histogram_rows:
                        bucket_min = min_val + (bucket_num - 1) * bucket_width
                        bucket_max = min_val + bucket_num * bucket_width
                        histogram.append(
                            HistogramBucket(
                                bucket_min=float(bucket_min),
                                bucket_max=float(bucket_max),
                                count=int(count),
                            )
                        )
            except Exception as e:
                logger.debug("histogram_failed", column=column_name, error=str(e))

        return ColumnProfile(
            column_id=column_id,
            column_ref=ColumnRef(table_name=table_name, column_name=column_name),
            profiled_at=profiled_at,
            total_count=total_count,
            null_count=null_count,
            distinct_count=distinct_count,
            null_ratio=null_ratio,
            cardinality_ratio=cardinality_ratio,
            numeric_stats=numeric_stats,
            string_stats=string_stats,
            histogram=histogram if histogram else None,
            top_values=top_values,
        )
    except Exception as e:
        logger.warning("column_profile_failed", column=column_name, table=table_name, error=str(e))
        return None
    finally:
        cursor.close()


def profile_statistics(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    max_workers: int = 4,
) -> Result[StatisticsProfileResult]:
    """Profile typed data to compute all row-based statistics.

    This function computes ALL row-based statistics on CLEAN data:
    - Basic counts (total, null, distinct, cardinality)
    - String stats (min/max/avg length)
    - Top values (frequency analysis)
    - Numeric stats (min, max, mean, stddev, skewness, kurtosis, cv)
    - Percentiles (p01, p25, p50, p75, p99)
    - Histograms

    Uses parallel processing for file-based DBs to speed up profiling.

    REQUIRES table.layer == "typed". Raises error otherwise.

    Note: Correlation analysis is handled separately by analysis/correlation module.

    Args:
        table_id: Table ID to profile
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        max_workers: Maximum parallel workers

    Returns:
        Result containing StatisticsProfileResult
    """
    start_time = time.time()
    settings = get_settings()

    try:
        # Get table from metadata
        table = session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        if table.layer != "typed":
            return Result.fail(f"Statistics profiling requires typed tables. Got: {table.layer}")

        # Get all columns for this table
        from sqlalchemy import select

        column_stmt = (
            select(Column).where(Column.table_id == table.table_id).order_by(Column.column_position)
        )
        column_result = session.execute(column_stmt)
        columns = column_result.scalars().all()

        if not columns:
            return Result.fail(f"No columns found for table {table.table_id}")

        profiled_at = datetime.now(UTC)
        profiles: list[ColumnProfile] = []
        top_k = settings.profile_top_k_values

        # Use parallel processing with cursors from shared connection
        # DuckDB cursors are thread-safe for read operations
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _profile_column_stats_parallel,
                    duckdb_conn,
                    table.table_name,
                    table.duckdb_path,
                    column.column_id,
                    column.column_name,
                    column.resolved_type or "VARCHAR",
                    profiled_at,
                    top_k,
                )
                for column in columns
            ]

            for future in futures:
                profile = future.result()
                if profile:
                    profiles.append(profile)

                    # Store in metadata database (sequential - SQLite writes)
                    non_null_count = profile.total_count - profile.null_count
                    is_unique = (
                        profile.distinct_count == non_null_count if non_null_count > 0 else False
                    )

                    db_profile = DBColumnProfile(
                        profile_id=str(uuid4()),
                        column_id=profile.column_id,
                        profiled_at=profiled_at,
                        layer="typed",
                        total_count=profile.total_count,
                        null_count=profile.null_count,
                        distinct_count=profile.distinct_count,
                        null_ratio=profile.null_ratio,
                        cardinality_ratio=profile.cardinality_ratio,
                        is_unique=is_unique,
                        is_numeric=profile.numeric_stats is not None,
                        profile_data=profile.model_dump(mode="json"),
                    )
                    session.add(db_profile)

        # Update table's last_profiled_at
        table.last_profiled_at = profiled_at

        # No flush needed - commit happens at session_scope() end
        # The caller (phase/orchestrator) manages the transaction

        duration = time.time() - start_time

        return Result.ok(
            StatisticsProfileResult(
                column_profiles=profiles,
                duration_seconds=duration,
            )
        )

    except Exception as e:
        return Result.fail(f"Statistics profiling failed: {e}")
