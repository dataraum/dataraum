"""Main profiling orchestrator."""

import time
from typing import Any

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.profiling.models import ProfileResult, TypeResolutionResult
from dataraum_context.profiling.statistical import compute_statistical_profile
from dataraum_context.profiling.type_inference import infer_type_candidates
from dataraum_context.profiling.type_resolution import resolve_types
from dataraum_context.storage.models_v2 import Table


async def profile_table(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[ProfileResult]:
    """Profile a table to generate statistical metadata and type candidates.

    This is the main entry point for profiling. It:
    1. Computes statistical profiles (counts, nulls, distributions)
    2. Detects patterns and infers type candidates
    3. Stores results in metadata database

    Args:
        table_id: Table ID to profile
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing ProfileResult or error
    """
    start_time = time.time()

    try:
        # Get table from metadata
        table = await session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        # Step 1: Statistical profiling
        stats_result = await compute_statistical_profile(
            table=table,
            duckdb_conn=duckdb_conn,
            session=session,
        )

        if not stats_result.success:
            return Result.fail(f"Statistical profiling failed: {stats_result.error}")

        # Step 2: Type inference (only for 'raw' layer tables)
        type_candidates: list[Any] = []
        if table.layer == "raw":
            type_result = await infer_type_candidates(
                table=table,
                duckdb_conn=duckdb_conn,
                session=session,
            )

            if not type_result.success:
                return Result.fail(f"Type inference failed: {type_result.error}")

            type_candidates = type_result.unwrap()

        # Update table's last_profiled_at
        from datetime import UTC, datetime

        table.last_profiled_at = datetime.now(UTC)
        await session.commit()

        # Build result
        duration = time.time() - start_time

        profiles = stats_result.value

        result = ProfileResult(
            profiles=profiles if profiles else [],
            type_candidates=type_candidates if type_candidates else [],
            duration_seconds=duration,
        )

        return Result.ok(result)

    except Exception as e:
        return Result.fail(f"Profiling failed: {e}")


async def profile_and_resolve_types(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    auto_resolve: bool = True,
    min_confidence: float = 0.85,
) -> Result[TypeResolutionResult]:
    """Profile table and optionally resolve types.

    Combines profiling and type resolution into a single operation:
    1. Calls profile_table() for statistics + type candidates
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
    # Profile the table first
    profile_result = await profile_table(table_id, duckdb_conn, session)
    if not profile_result.success:
        return Result.fail(f"Profiling failed: {profile_result.error}")

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
