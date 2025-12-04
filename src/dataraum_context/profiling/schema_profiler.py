"""Schema profiler for raw VARCHAR tables.

Performs type discovery operations that are STABLE regardless of
how many rows are later quarantined:
- Pattern detection (sample-based)
- Type inference → TypeCandidates

Does NOT compute row-based statistics (counts, frequencies, percentiles).
Those are computed in the statistics profiler AFTER type resolution
when the data is clean.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.profiling.models import DetectedPattern, SchemaProfileResult, TypeCandidate
from dataraum_context.profiling.patterns import PatternConfig, load_pattern_config
from dataraum_context.profiling.type_inference import infer_type_candidates
from dataraum_context.storage.models_v2.core import Column, Table


async def profile_schema(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[SchemaProfileResult]:
    """Profile table structure for type discovery (works on raw VARCHAR).

    This function performs sample-based analysis that is stable even if
    rows are later quarantined during type resolution:
    - Pattern detection (regex matching on sampled values)
    - Type inference → TypeCandidates with confidence scores

    Does NOT compute row-based stats (counts, frequencies, percentiles) -
    those move to statistics stage after quarantine is determined.

    Args:
        table_id: Table ID to profile
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing SchemaProfileResult with type candidates and patterns
    """
    start_time = time.time()

    try:
        # Get table from metadata
        table = await session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        if table.layer != "raw":
            return Result.fail(f"Schema profiling is for raw tables only. Got: {table.layer}")

        # Get columns
        columns = await session.run_sync(
            lambda sync_session: sync_session.query(Column)
            .filter(Column.table_id == table.table_id)
            .order_by(Column.column_position)
            .all()
        )

        # Pattern detection (sample-based, stable)
        detected_patterns: dict[str, list[DetectedPattern]] = {}
        pattern_config = load_pattern_config()

        for column in columns:
            col_patterns = await _detect_column_patterns(
                table=table,
                column=column,
                duckdb_conn=duckdb_conn,
                pattern_config=pattern_config,
            )
            if col_patterns:
                detected_patterns[column.column_name] = col_patterns

        # Type inference (pattern matching + parse testing)
        # This is sample-based and stable regardless of later quarantine
        type_candidates: list[TypeCandidate] = []
        type_result = await infer_type_candidates(
            table=table,
            duckdb_conn=duckdb_conn,
            session=session,
        )

        if not type_result.success:
            return Result.fail(f"Type inference failed: {type_result.error}")

        type_candidates = type_result.unwrap()

        # Update table's last_profiled_at
        table.last_profiled_at = datetime.now(UTC)
        await session.commit()

        duration = time.time() - start_time

        return Result.ok(
            SchemaProfileResult(
                type_candidates=type_candidates,
                detected_patterns=detected_patterns,
                duration_seconds=duration,
            )
        )

    except Exception as e:
        return Result.fail(f"Schema profiling failed: {e}")


async def _detect_column_patterns(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    pattern_config: PatternConfig,
) -> list[DetectedPattern]:
    """Detect patterns in a column using sample values.

    This is sample-based and stable regardless of later quarantine.

    Args:
        table: Parent table
        column: Column to analyze
        duckdb_conn: DuckDB connection
        pattern_config: Pattern configuration

    Returns:
        List of detected patterns with match rates
    """
    try:
        table_name = table.duckdb_path
        col_name = column.column_name

        # Get sample values (sample-based, stable)
        sample_query = f"""
            SELECT DISTINCT "{col_name}"
            FROM "{table_name}"
            WHERE "{col_name}" IS NOT NULL
            USING SAMPLE 100 ROWS
        """
        sample_rows = duckdb_conn.execute(sample_query).fetchall()
        sample_values = [str(row[0]) for row in sample_rows if row[0] is not None]

        if not sample_values:
            return []

        # Convert all to strings and deduplicate
        sample_values = list({str(v) for v in sample_values if v})

        if not sample_values:
            return []

        # Count pattern matches
        pattern_counts: dict[str, int] = {}
        for value in sample_values:
            matched = pattern_config.match_value(str(value))
            for pattern in matched:
                pattern_counts[pattern.name] = pattern_counts.get(pattern.name, 0) + 1

        # Calculate match rates and create DetectedPattern entries
        detected_patterns = []
        sample_size = len(sample_values)

        for pattern_name, count in pattern_counts.items():
            match_rate = count / sample_size
            if match_rate >= 0.1:  # Only include patterns with >= 10% match rate
                # Find the pattern to get semantic_type
                found_pattern = next(
                    (p for p in pattern_config.get_value_patterns() if p.name == pattern_name),
                    None,
                )
                detected_patterns.append(
                    DetectedPattern(
                        name=pattern_name,
                        match_rate=match_rate,
                        semantic_type=found_pattern.semantic_type if found_pattern else None,
                    )
                )

        # Sort by match rate descending
        detected_patterns.sort(key=lambda p: p.match_rate, reverse=True)

        return detected_patterns

    except Exception:
        return []  # Pattern detection failed, return empty
