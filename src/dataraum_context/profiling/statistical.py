"""Statistical profiling for columns."""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.config import Settings, get_settings
from dataraum_context.core.models.base import ColumnRef, Result
from dataraum_context.profiling.models import (
    ColumnProfile,
    DetectedPattern,
    HistogramBucket,
    NumericStats,
    StringStats,
    ValueCount,
)
from dataraum_context.profiling.patterns import load_pattern_config
from dataraum_context.storage.models_v2 import Column, Table
from dataraum_context.storage.models_v2 import StatisticalProfile as DBColumnProfile


async def compute_statistical_profile(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[list[ColumnProfile]]:
    """Compute statistical profiles for all columns in a table.

    Args:
        table: Table to profile
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing list of ColumnProfile objects
    """
    settings = get_settings()
    profiles = []

    try:
        # Get all columns for this table
        columns = await session.run_sync(
            lambda sync_session: sync_session.query(Column)
            .filter(Column.table_id == table.table_id)
            .order_by(Column.column_position)
            .all()
        )

        if not columns:
            return Result.fail(f"No columns found for table {table.table_id}")

        profiled_at = datetime.now(UTC)

        for column in columns:
            # Profile this column
            profile_result = await _profile_column(
                table=table,
                column=column,
                duckdb_conn=duckdb_conn,
                profiled_at=profiled_at,
                settings=settings,
            )

            if not profile_result.success:
                # Log warning but continue with other columns
                continue

            profile = profile_result.value

            if not profile:
                continue

            # Calculate is_unique and duplicate_count
            non_null_count = profile.total_count - profile.null_count
            is_unique = profile.distinct_count == non_null_count if non_null_count > 0 else False

            # Store in metadata database using hybrid storage
            db_profile = DBColumnProfile(
                profile_id=str(uuid4()),
                column_id=column.column_id,
                profiled_at=profiled_at,
                # STRUCTURED: Queryable core dimensions
                total_count=profile.total_count,
                null_count=profile.null_count,
                distinct_count=profile.distinct_count,
                null_ratio=profile.null_ratio,
                cardinality_ratio=profile.cardinality_ratio,
                # Flags for filtering
                is_unique=is_unique,
                is_numeric=profile.numeric_stats is not None,
                # JSONB: Full Pydantic model (zero mapping!)
                profile_data=profile.model_dump(mode="json"),
            )
            session.add(db_profile)
            profiles.append(profile)

        await session.commit()
        return Result.ok(profiles)

    except Exception as e:
        return Result.fail(f"Failed to compute statistical profile: {e}")


async def _profile_column(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    profiled_at: datetime,
    settings: Settings,
) -> Result[ColumnProfile]:
    """Profile a single column.

    Args:
        table: Parent table
        column: Column to profile
        duckdb_conn: DuckDB connection
        profiled_at: Timestamp for this profiling run
        settings: Application settings

    Returns:
        Result containing ColumnProfile
    """
    try:
        table_name = table.duckdb_path
        col_name = column.column_name

        # Basic counts
        count_query = f"""
            SELECT
                COUNT(*) as total_count,
                COUNT("{col_name}") as non_null_count,
                COUNT(DISTINCT "{col_name}") as distinct_count
            FROM {table_name}
        """

        counts = duckdb_conn.execute(count_query).fetchone()
        total_count = counts[0] if counts and len(counts) > 0 else 0
        non_null_count = counts[1] if counts and len(counts) > 1 else 0
        distinct_count = counts[2] if counts and len(counts) > 2 else 0
        null_count = total_count - non_null_count

        null_ratio = null_count / total_count if total_count > 0 else 0.0
        cardinality_ratio = distinct_count / non_null_count if non_null_count > 0 else 0.0

        # Try numeric stats (will fail if not numeric-parseable)
        numeric_stats = None
        try:
            numeric_query = f"""
                SELECT
                    MIN(TRY_CAST("{col_name}" AS DOUBLE)) as min_val,
                    MAX(TRY_CAST("{col_name}" AS DOUBLE)) as max_val,
                    AVG(TRY_CAST("{col_name}" AS DOUBLE)) as mean_val,
                    STDDEV(TRY_CAST("{col_name}" AS DOUBLE)) as stddev_val,
                    SKEWNESS(TRY_CAST("{col_name}" AS DOUBLE)) as skewness_val,
                    KURTOSIS(TRY_CAST("{col_name}" AS DOUBLE)) as kurtosis_val,
                    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p01,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p25,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p75,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p99
                FROM {table_name}
                WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
            """
            numeric_row = duckdb_conn.execute(numeric_query).fetchone()

            if numeric_row and numeric_row[0] is not None:
                mean_val = float(numeric_row[2]) if numeric_row[2] is not None else 0.0
                stddev_val = float(numeric_row[3]) if numeric_row[3] is not None else 0.0

                # Calculate coefficient of variation (avoid division by zero)
                cv_val = None
                if mean_val != 0 and stddev_val is not None:
                    cv_val = stddev_val / abs(mean_val)

                numeric_stats = NumericStats(
                    min_value=float(numeric_row[0]),
                    max_value=float(numeric_row[1]),
                    mean=mean_val,
                    stddev=stddev_val,
                    skewness=float(numeric_row[4]) if numeric_row[4] is not None else None,
                    kurtosis=float(numeric_row[5]) if numeric_row[5] is not None else None,
                    cv=cv_val,
                    percentiles={
                        "p01": float(numeric_row[6]) if numeric_row[6] is not None else None,
                        "p25": float(numeric_row[7]) if numeric_row[7] is not None else None,
                        "p50": float(numeric_row[8]) if numeric_row[8] is not None else None,
                        "p75": float(numeric_row[9]) if numeric_row[9] is not None else None,
                        "p99": float(numeric_row[10]) if numeric_row[10] is not None else None,
                    },
                )
        except Exception:
            pass  # Not numeric, that's ok

        # String stats (always applicable to VARCHAR)
        string_stats = None
        try:
            string_query = f"""
                SELECT
                    MIN(LENGTH("{col_name}")) as min_len,
                    MAX(LENGTH("{col_name}")) as max_len,
                    AVG(LENGTH("{col_name}")) as avg_len
                FROM {table_name}
                WHERE "{col_name}" IS NOT NULL
            """
            string_row = duckdb_conn.execute(string_query).fetchone()

            if string_row and string_row[0] is not None:
                string_stats = StringStats(
                    min_length=int(string_row[0]),
                    max_length=int(string_row[1]),
                    avg_length=float(string_row[2]),
                )
        except Exception:
            pass

        # Top values
        top_k = settings.profile_top_k_values
        top_values = []
        try:
            top_values_query = f"""
                SELECT
                    "{col_name}" as value,
                    COUNT(*) as count,
                    (COUNT(*) * 100.0 / {total_count}) as percentage
                FROM {table_name}
                WHERE "{col_name}" IS NOT NULL
                GROUP BY "{col_name}"
                ORDER BY count DESC
                LIMIT {top_k}
            """
            top_values_rows = duckdb_conn.execute(top_values_query).fetchall()

            for value, count, percentage in top_values_rows:
                top_values.append(
                    ValueCount(
                        value=value,
                        count=int(count),
                        percentage=float(percentage),
                    )
                )
        except Exception:
            pass

        # Pattern detection using sample values
        detected_patterns = []
        try:
            pattern_config = load_pattern_config()

            # Sample values for pattern detection (use top values + additional random sample)
            sample_values = [v.value for v in top_values if v.value is not None]

            # Get additional random sample if needed
            if len(sample_values) < 50 and non_null_count > 0:
                sample_query = f"""
                    SELECT DISTINCT "{col_name}"
                    FROM {table_name}
                    WHERE "{col_name}" IS NOT NULL
                    USING SAMPLE 50 ROWS
                """
                try:
                    sample_rows = duckdb_conn.execute(sample_query).fetchall()
                    sample_values.extend(str(row[0]) for row in sample_rows if row[0] is not None)
                except Exception:
                    pass

            # Convert all to strings for pattern matching
            sample_values = list({str(v) for v in sample_values if v})

            if sample_values:
                # Count pattern matches
                pattern_counts: dict[str, int] = {}
                for value in sample_values:
                    matched = pattern_config.match_value(str(value))
                    for pattern in matched:
                        pattern_counts[pattern.name] = pattern_counts.get(pattern.name, 0) + 1

                # Calculate match rates and create DetectedPattern entries
                sample_size = len(sample_values)
                for pattern_name, count in pattern_counts.items():
                    match_rate = count / sample_size
                    if match_rate >= 0.1:  # Only include patterns with >= 10% match rate
                        # Find the pattern to get semantic_type
                        found_pattern = next(
                            (
                                p
                                for p in pattern_config.get_value_patterns()
                                if p.name == pattern_name
                            ),
                            None,
                        )
                        detected_patterns.append(
                            DetectedPattern(
                                name=pattern_name,
                                match_rate=match_rate,
                                semantic_type=found_pattern.semantic_type
                                if found_pattern
                                else None,
                            )
                        )

                # Sort by match rate descending
                detected_patterns.sort(key=lambda p: p.match_rate, reverse=True)
        except Exception:
            pass  # Pattern detection failed, that's ok

        # Histogram (only for numeric columns)
        histogram = []
        if numeric_stats is not None:
            try:
                num_bins = 20  # Standard histogram bin count

                # Use WIDTH_BUCKET approach for histogram
                # First get the range
                min_val = numeric_stats.min_value
                max_val = numeric_stats.max_value

                # Only create histogram if there's a meaningful range
                if min_val != max_val and non_null_count > 0:
                    # Calculate bucket width
                    range_width = max_val - min_val
                    bucket_width = range_width / num_bins

                    # Generate histogram using WIDTH_BUCKET
                    histogram_query = f"""
                        WITH bucketed AS (
                            SELECT
                                WIDTH_BUCKET(
                                    TRY_CAST("{col_name}" AS DOUBLE),
                                    {min_val},
                                    {max_val},
                                    {num_bins}
                                ) as bucket_num
                            FROM {table_name}
                            WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
                        )
                        SELECT
                            bucket_num,
                            COUNT(*) as count
                        FROM bucketed
                        WHERE bucket_num > 0 AND bucket_num <= {num_bins}
                        GROUP BY bucket_num
                        ORDER BY bucket_num
                    """

                    histogram_rows = duckdb_conn.execute(histogram_query).fetchall()

                    for bucket_num, count in histogram_rows:
                        # Calculate bucket boundaries
                        bucket_min = min_val + (bucket_num - 1) * bucket_width
                        bucket_max = min_val + bucket_num * bucket_width

                        histogram.append(
                            HistogramBucket(
                                bucket_min=float(bucket_min),
                                bucket_max=float(bucket_max),
                                count=int(count),
                            )
                        )
            except Exception:
                pass  # Histogram generation failed, that's ok

        # Build profile
        profile = ColumnProfile(
            column_id=column.column_id,
            column_ref=ColumnRef(
                table_name=table.table_name,
                column_name=column.column_name,
            ),
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
            detected_patterns=detected_patterns,
        )

        return Result.ok(profile)

    except Exception as e:
        return Result.fail(f"Failed to profile column {column.column_name}: {e}")
