"""Statistical profiling for columns."""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.config import get_settings
from dataraum_context.core.models import (
    ColumnProfile,
    ColumnRef,
    NumericStats,
    Result,
    StringStats,
    ValueCount,
)
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

            # Store in metadata database
            db_profile = DBColumnProfile(
                profile_id=str(uuid4()),
                column_id=column.column_id,
                profiled_at=profiled_at,
                total_count=profile.total_count,
                null_count=profile.null_count,
                distinct_count=profile.distinct_count,
                null_ratio=profile.null_ratio,
                cardinality_ratio=profile.cardinality_ratio,
                min_value=profile.numeric_stats.min_value if profile.numeric_stats else None,
                max_value=profile.numeric_stats.max_value if profile.numeric_stats else None,
                mean_value=profile.numeric_stats.mean if profile.numeric_stats else None,
                stddev_value=profile.numeric_stats.stddev if profile.numeric_stats else None,
                percentiles=(profile.numeric_stats.percentiles if profile.numeric_stats else None),
                min_length=profile.string_stats.min_length if profile.string_stats else None,
                max_length=profile.string_stats.max_length if profile.string_stats else None,
                avg_length=profile.string_stats.avg_length if profile.string_stats else None,
                histogram=[
                    {"bucket_min": b.bucket_min, "bucket_max": b.bucket_max, "count": b.count}
                    for b in (profile.histogram or [])
                ],
                top_values=[
                    {"value": v.value, "count": v.count, "percentage": v.percentage}
                    for v in (profile.top_values or [])
                ],
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
    settings,
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
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p25,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TRY_CAST("{col_name}" AS DOUBLE)) as p75
                FROM {table_name}
                WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
            """
            numeric_row = duckdb_conn.execute(numeric_query).fetchone()

            if numeric_row and numeric_row[0] is not None:
                numeric_stats = NumericStats(
                    min_value=float(numeric_row[0]),
                    max_value=float(numeric_row[1]),
                    mean=float(numeric_row[2]) if numeric_row[2] is not None else 0.0,
                    stddev=float(numeric_row[3]) if numeric_row[3] is not None else 0.0,
                    percentiles={
                        "p25": float(numeric_row[4]) if numeric_row[4] is not None else 0.0,
                        "p50": float(numeric_row[5]) if numeric_row[5] is not None else 0.0,
                        "p75": float(numeric_row[6]) if numeric_row[6] is not None else 0.0,
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
            histogram=None,  # TODO: Implement histogram
            top_values=top_values,
            detected_patterns=[],  # Will be filled by pattern detection
        )

        return Result.ok(profile)

    except Exception as e:
        return Result.fail(f"Failed to profile column {column.column_name}: {e}")
