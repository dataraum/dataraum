"""Slice analysis runner.

Functions to register slice tables in metadata and run analysis phases on slices.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.slicing.db_models import SliceDefinition
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table

if TYPE_CHECKING:
    from dataraum_context.analysis.semantic.agent import SemanticAgent


@dataclass
class SliceTableInfo:
    """Information about a registered slice table."""

    slice_table_id: str
    slice_table_name: str
    source_table_id: str
    source_table_name: str
    slice_column_name: str
    slice_value: str
    row_count: int


@dataclass
class SliceAnalysisResult:
    """Result from analyzing slices."""

    slices_registered: int
    slices_analyzed: int
    statistics_computed: int
    quality_assessed: int
    semantic_enriched: int
    errors: list[str]


def _sanitize_name(value: str) -> str:
    """Sanitize a value for use in table names."""
    safe = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
    safe = re.sub(r"_+", "_", safe).strip("_").lower()
    return safe


def _get_slice_table_name(source_table_name: str, column_name: str, value: str) -> str:
    """Generate slice table name from components."""
    safe_source = _sanitize_name(source_table_name)
    safe_column = _sanitize_name(column_name)
    safe_value = _sanitize_name(value)
    return f"slice_{safe_source}_{safe_column}_{safe_value}"


async def register_slice_tables(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_definitions: list[SliceDefinition] | None = None,
) -> Result[list[SliceTableInfo]]:
    """Register slice tables from DuckDB into metadata database.

    For each slice table found in DuckDB (matching pattern slice_*),
    creates Table and Column entries with layer='slice'.

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        slice_definitions: Optional list of slice definitions to use.
            If not provided, will query from database.

    Returns:
        Result containing list of registered SliceTableInfo
    """
    try:
        # Get slice definitions if not provided
        if slice_definitions is None:
            stmt = select(SliceDefinition).order_by(SliceDefinition.slice_priority)
            result = await session.execute(stmt)
            slice_definitions = list(result.scalars().all())

        if not slice_definitions:
            return Result.ok([])

        # Get all existing slice tables in DuckDB
        tables_result = duckdb_conn.execute("SHOW TABLES").fetchall()
        duckdb_tables = {t[0] for t in tables_result}
        slice_tables_in_duckdb = {t for t in duckdb_tables if t.startswith("slice_")}

        if not slice_tables_in_duckdb:
            return Result.ok([])

        registered: list[SliceTableInfo] = []

        for slice_def in slice_definitions:
            # Load related table and column
            source_table = await session.get(Table, slice_def.table_id)
            source_column = await session.get(Column, slice_def.column_id)

            if not source_table or not source_column:
                continue

            # Get source columns for copying schema
            source_columns_stmt = (
                select(Column)
                .where(Column.table_id == source_table.table_id)
                .order_by(Column.column_position)
            )
            source_columns_result = await session.execute(source_columns_stmt)
            source_columns = list(source_columns_result.scalars().all())

            # Process each slice value
            for value in slice_def.distinct_values or []:
                slice_table_name = _get_slice_table_name(
                    source_table.table_name,
                    source_column.column_name,
                    value,
                )

                # Check if table exists in DuckDB
                if slice_table_name not in slice_tables_in_duckdb:
                    continue

                # Check if already registered
                existing_stmt = select(Table).where(
                    Table.table_name == slice_table_name,
                    Table.layer == "slice",
                )
                existing_result = await session.execute(existing_stmt)
                existing_table = existing_result.scalar_one_or_none()

                if existing_table:
                    # Already registered
                    row_count = duckdb_conn.execute(
                        f"SELECT COUNT(*) FROM {slice_table_name}"
                    ).fetchone()[0]
                    registered.append(
                        SliceTableInfo(
                            slice_table_id=existing_table.table_id,
                            slice_table_name=slice_table_name,
                            source_table_id=source_table.table_id,
                            source_table_name=source_table.table_name,
                            slice_column_name=source_column.column_name,
                            slice_value=value,
                            row_count=row_count,
                        )
                    )
                    continue

                # Get row count from DuckDB
                row_count = duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM {slice_table_name}"
                ).fetchone()[0]

                # Create Table entry for slice
                slice_table = Table(
                    source_id=source_table.source_id,
                    table_name=slice_table_name,
                    layer="slice",
                    duckdb_path=slice_table_name,
                    row_count=row_count,
                )
                session.add(slice_table)
                await session.flush()  # Get the table_id

                # Create Column entries (copy from parent)
                for src_col in source_columns:
                    col = Column(
                        table_id=slice_table.table_id,
                        column_name=src_col.column_name,
                        column_position=src_col.column_position,
                        raw_type=src_col.raw_type,
                        resolved_type=src_col.resolved_type,
                    )
                    session.add(col)

                registered.append(
                    SliceTableInfo(
                        slice_table_id=slice_table.table_id,
                        slice_table_name=slice_table_name,
                        source_table_id=source_table.table_id,
                        source_table_name=source_table.table_name,
                        slice_column_name=source_column.column_name,
                        slice_value=value,
                        row_count=row_count,
                    )
                )

        await session.commit()
        return Result.ok(registered)

    except Exception as e:
        await session.rollback()
        return Result.fail(f"Failed to register slice tables: {e}")


async def run_statistics_on_slice(
    slice_info: SliceTableInfo,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[Any]:
    """Run statistical profiling on a slice table.

    Args:
        slice_info: Information about the slice table
        duckdb_conn: DuckDB connection
        session: Database session

    Returns:
        Result containing profile result
    """
    from dataraum_context.analysis.statistics import profile_statistics

    # The profile_statistics function expects layer='typed', but we need to
    # temporarily work with slice layer. We'll directly call it with the table_id.
    # Since we registered with layer='slice', we need to handle this.

    # Get the slice table
    table = await session.get(Table, slice_info.slice_table_id)
    if not table:
        return Result.fail(f"Slice table not found: {slice_info.slice_table_id}")

    # Temporarily set layer to 'typed' for profiling
    original_layer = table.layer
    table.layer = "typed"
    await session.flush()

    try:
        result = await profile_statistics(
            table_id=slice_info.slice_table_id,
            duckdb_conn=duckdb_conn,
            session=session,
        )
        return result
    finally:
        # Restore layer
        table.layer = original_layer
        await session.flush()


async def run_quality_on_slice(
    slice_info: SliceTableInfo,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[Any]:
    """Run statistical quality assessment on a slice table.

    Args:
        slice_info: Information about the slice table
        duckdb_conn: DuckDB connection
        session: Database session

    Returns:
        Result containing quality result
    """
    from dataraum_context.analysis.statistics import assess_statistical_quality

    result = await assess_statistical_quality(
        table_id=slice_info.slice_table_id,
        duckdb_conn=duckdb_conn,
        session=session,
    )
    return result


async def run_semantic_on_slices(
    slice_infos: list[SliceTableInfo],
    session: AsyncSession,
    agent: SemanticAgent,
) -> Result[Any]:
    """Run semantic analysis on slice tables.

    Args:
        slice_infos: List of slice table infos
        session: Database session
        agent: Semantic agent

    Returns:
        Result containing semantic result
    """
    from dataraum_context.analysis.semantic import enrich_semantic

    table_ids = [s.slice_table_id for s in slice_infos]

    result = await enrich_semantic(
        session=session,
        agent=agent,
        table_ids=table_ids,
        ontology="general",
    )
    return result


async def run_analysis_on_slices(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_infos: list[SliceTableInfo],
    semantic_agent: SemanticAgent | None = None,
    run_statistics: bool = True,
    run_quality: bool = True,
    run_semantic: bool = True,
) -> SliceAnalysisResult:
    """Run analysis phases on slice tables.

    Runs Phase 3 (statistics), Phase 3b (quality), and Phase 5 (semantic)
    on each slice table.

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        slice_infos: List of slice table infos to analyze
        semantic_agent: Semantic agent (required if run_semantic=True)
        run_statistics: Whether to run statistical profiling
        run_quality: Whether to run quality assessment
        run_semantic: Whether to run semantic analysis

    Returns:
        SliceAnalysisResult with counts and errors
    """
    errors: list[str] = []
    stats_count = 0
    quality_count = 0
    semantic_count = 0

    # Run statistics on each slice
    if run_statistics:
        for slice_info in slice_infos:
            result = await run_statistics_on_slice(slice_info, duckdb_conn, session)
            if result.success:
                stats_count += 1
            else:
                errors.append(f"Stats {slice_info.slice_table_name}: {result.error}")

    # Run quality on each slice
    if run_quality:
        for slice_info in slice_infos:
            result = await run_quality_on_slice(slice_info, duckdb_conn, session)
            if result.success:
                quality_count += 1
            else:
                errors.append(f"Quality {slice_info.slice_table_name}: {result.error}")

    # Run semantic on all slices at once (batched for efficiency)
    if run_semantic and semantic_agent:
        result = await run_semantic_on_slices(slice_infos, session, semantic_agent)
        if result.success:
            semantic_count = len(slice_infos)
        else:
            errors.append(f"Semantic analysis: {result.error}")

    return SliceAnalysisResult(
        slices_registered=len(slice_infos),
        slices_analyzed=len(slice_infos),
        statistics_computed=stats_count,
        quality_assessed=quality_count,
        semantic_enriched=semantic_count,
        errors=errors,
    )


__all__ = [
    "SliceTableInfo",
    "SliceAnalysisResult",
    "register_slice_tables",
    "run_analysis_on_slices",
    "run_statistics_on_slice",
    "run_quality_on_slice",
    "run_semantic_on_slices",
]
