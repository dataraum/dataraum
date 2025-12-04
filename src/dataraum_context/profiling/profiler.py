"""Main profiling orchestrator.

This module provides the main entry points for profiling:
- profile_schema(): Raw stage profiling (type discovery)
- profile_statistics(): Typed stage profiling (all statistics)
- profile_and_resolve_types(): Combined profiling + type resolution
"""

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.profiling.models import (
    SchemaProfileResult,
    StatisticsProfileResult,
    TypeResolutionResult,
)
from dataraum_context.profiling.schema_profiler import (
    profile_schema as _profile_schema,
)
from dataraum_context.profiling.statistics_profiler import (
    profile_statistics as _profile_statistics,
)
from dataraum_context.profiling.type_resolution import resolve_types


# Re-export for convenience
async def profile_schema(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[SchemaProfileResult]:
    """Profile raw table structure for type discovery.

    See schema_profiler.profile_schema for full documentation.
    """
    return await _profile_schema(table_id, duckdb_conn, session)


async def profile_statistics(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    include_correlations: bool = True,
) -> Result[StatisticsProfileResult]:
    """Profile typed table to compute all row-based statistics.

    See statistics_profiler.profile_statistics for full documentation.
    """
    return await _profile_statistics(table_id, duckdb_conn, session, include_correlations)


async def profile_and_resolve_types(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    auto_resolve: bool = True,
    min_confidence: float = 0.85,
) -> Result[TypeResolutionResult]:
    """Profile table and optionally resolve types.

    Combines schema profiling and type resolution into a single operation:
    1. Calls profile_schema() for type discovery (sample-based)
    2. If auto_resolve, calls resolve_types() to create typed table
    3. Returns TypeResolutionResult

    Args:
        table_id: Table ID to profile and resolve
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        auto_resolve: Whether to automatically resolve types
        min_confidence: Minimum confidence for auto-decision

    Returns:
        Result containing TypeResolutionResult or error
    """
    # Schema profiling (sample-based type discovery)
    schema_result = await profile_schema(table_id, duckdb_conn, session)
    if not schema_result.success:
        return Result.fail(f"Schema profiling failed: {schema_result.error}")

    if not auto_resolve:
        return Result.ok(
            TypeResolutionResult(
                typed_table_name="",
                quarantine_table_name="",
                total_rows=0,
                typed_rows=0,
                quarantined_rows=0,
                column_results=[],
            ),
            warnings=["Auto-resolve disabled. Type candidates generated but not resolved."],
        )

    # Resolve types
    return await resolve_types(table_id, duckdb_conn, session, min_confidence)
