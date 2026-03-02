"""Slice analysis runner.

Functions to register slice tables in metadata and run analysis phases on slices.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics import assess_statistical_quality, profile_statistics
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table

logger = get_logger(__name__)


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
    errors: list[str]


def _sanitize_name(value: str) -> str:
    """Sanitize a value for use in table names."""
    safe = re.sub(r"[^a-zA-Z0-9]", "_", str(value))
    safe = re.sub(r"_+", "_", safe).strip("_").lower()
    return safe


def _get_slice_table_name(column_name: str, value: str) -> str:
    """Generate slice table name from components."""
    safe_column = _sanitize_name(column_name)
    safe_value = _sanitize_name(value)
    return f"slice_{safe_column}_{safe_value}"


def register_slice_tables(
    session: Session,
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
            result = session.execute(stmt)
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
            source_table = session.get(Table, slice_def.table_id)
            source_column = session.get(Column, slice_def.column_id)

            if not source_table or not source_column:
                continue

            # Prefer slice_def.column_name (stores the actual LLM-recommended name,
            # which for enriched dim cols is e.g. "kontonummer_des_gegenkontos__land").
            # Fall back to source_column.column_name for older records.
            effective_column_name = slice_def.column_name or source_column.column_name

            # Process each slice value
            for value in slice_def.distinct_values or []:
                slice_table_name = _get_slice_table_name(
                    effective_column_name,
                    value,
                )

                # Check if table exists in DuckDB
                if slice_table_name not in slice_tables_in_duckdb:
                    continue

                # Check if already registered (include source_id in query for correct uniqueness)
                existing_stmt = select(Table).where(
                    Table.source_id == source_table.source_id,
                    Table.table_name == slice_table_name,
                    Table.layer == "slice",
                )
                existing_result = session.execute(existing_stmt)
                existing_table = existing_result.scalar_one_or_none()

                # Non-PK query — session.execute() can't see unflushed objects.
                # Check session.new for Tables added earlier in this loop iteration.
                if not existing_table:
                    for obj in session.new:
                        if (
                            isinstance(obj, Table)
                            and obj.source_id == source_table.source_id
                            and obj.table_name == slice_table_name
                            and obj.layer == "slice"
                        ):
                            existing_table = obj
                            break

                if existing_table:
                    # Already registered
                    count_result = duckdb_conn.execute(
                        f"SELECT COUNT(*) FROM {slice_table_name}"
                    ).fetchone()
                    row_count = count_result[0] if count_result else 0
                    registered.append(
                        SliceTableInfo(
                            slice_table_id=existing_table.table_id,
                            slice_table_name=slice_table_name,
                            source_table_id=source_table.table_id,
                            source_table_name=source_table.table_name,
                            slice_column_name=effective_column_name,
                            slice_value=value,
                            row_count=row_count,
                        )
                    )
                    continue

                # Get row count from DuckDB
                count_result = duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM {slice_table_name}"
                ).fetchone()
                row_count = count_result[0] if count_result else 0

                # Create Table entry for slice
                # Generate table_id explicitly since SQLAlchemy defaults only apply at INSERT time
                slice_table = Table(
                    table_id=str(uuid4()),
                    source_id=source_table.source_id,
                    table_name=slice_table_name,
                    layer="slice",
                    duckdb_path=slice_table_name,
                    row_count=row_count,
                )
                session.add(slice_table)

                # Derive column schema from the slicing view metadata table
                # (layer="slicing_view"), if one exists for this fact table.
                # That table has the correct schema (fact columns + enriched
                # FK-prefixed dimension columns) registered by slicing_view_phase.
                # Fall back to DuckDB DESCRIBE if no slicing_view table is found.
                sv_table_stmt = select(Table).where(
                    Table.source_id == source_table.source_id,
                    Table.table_name == f"slicing_{source_table.table_name}",
                    Table.layer == "slicing_view",
                )
                sv_table = session.execute(sv_table_stmt).scalar_one_or_none()

                if sv_table:
                    sv_cols_stmt = (
                        select(Column)
                        .where(Column.table_id == sv_table.table_id)
                        .order_by(Column.column_position)
                    )
                    schema_cols = session.execute(sv_cols_stmt).scalars().all()
                    for src_col in schema_cols:
                        session.add(
                            Column(
                                column_id=str(uuid4()),
                                table_id=slice_table.table_id,
                                column_name=src_col.column_name,
                                column_position=src_col.column_position,
                                raw_type=src_col.raw_type,
                                resolved_type=src_col.resolved_type,
                            )
                        )
                else:
                    # No slicing view registered — read schema directly from DuckDB.
                    duckdb_cols = duckdb_conn.execute(f"DESCRIBE {slice_table_name}").fetchall()
                    for pos, row in enumerate(duckdb_cols):
                        session.add(
                            Column(
                                column_id=str(uuid4()),
                                table_id=slice_table.table_id,
                                column_name=row[0],
                                column_position=pos,
                                raw_type=row[1],
                                resolved_type=row[1],
                            )
                        )

                registered.append(
                    SliceTableInfo(
                        slice_table_id=slice_table.table_id,
                        slice_table_name=slice_table_name,
                        source_table_id=source_table.table_id,
                        source_table_name=source_table.table_name,
                        slice_column_name=effective_column_name,
                        slice_value=value,
                        row_count=row_count,
                    )
                )

        return Result.ok(registered)

    except Exception as e:
        session.rollback()
        return Result.fail(f"Failed to register slice tables: {e}")


def run_statistics_on_slice(
    slice_info: SliceTableInfo,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> Result[Any]:
    """Run statistical profiling on a slice table.

    Args:
        slice_info: Information about the slice table
        duckdb_conn: DuckDB connection
        session: Database session

    Returns:
        Result containing profile result
    """
    # The profile_statistics function expects layer='typed', but we need to
    # temporarily work with slice layer. We'll directly call it with the table_id.
    # Since we registered with layer='slice', we need to handle this.

    # session.get() checks the identity map first, so pending objects
    # added via session.add() in this session are found without flush.
    table = session.get(Table, slice_info.slice_table_id)

    # Fallback: check session.new for pending objects not yet in identity map
    # (can happen with autoflush=False when objects were added but PK not indexed)
    if not table:
        for obj in session.new:
            if isinstance(obj, Table) and obj.table_id == slice_info.slice_table_id:
                table = obj
                break

    if not table:
        return Result.fail(f"Slice table not found: {slice_info.slice_table_id}")

    # Temporarily set layer to 'typed' for profiling
    # No flush needed - SQLAlchemy identity map returns modified object to same session
    original_layer = table.layer
    table.layer = "typed"

    try:
        result = profile_statistics(
            table_id=slice_info.slice_table_id,
            duckdb_conn=duckdb_conn,
            session=session,
        )
        return result
    finally:
        # Restore layer - no flush needed, commit happens at session_scope() end
        table.layer = original_layer


def run_quality_on_slice(
    slice_info: SliceTableInfo,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> Result[Any]:
    """Run statistical quality assessment on a slice table.

    Args:
        slice_info: Information about the slice table
        duckdb_conn: DuckDB connection
        session: Database session

    Returns:
        Result containing quality result
    """
    result = assess_statistical_quality(
        table_id=slice_info.slice_table_id,
        duckdb_conn=duckdb_conn,
        session=session,
    )
    return result


def run_analysis_on_slices(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_infos: list[SliceTableInfo],
    run_statistics: bool = True,
    run_quality: bool = True,
) -> SliceAnalysisResult:
    """Run analysis phases on slice tables.

    Runs statistics and quality analysis on each slice table.

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        slice_infos: List of slice table infos to analyze
        run_statistics: Whether to run statistical profiling
        run_quality: Whether to run quality assessment

    Returns:
        SliceAnalysisResult with counts and errors
    """
    errors: list[str] = []
    stats_count = 0
    quality_count = 0

    # Run statistics on each slice
    if run_statistics:
        for slice_info in slice_infos:
            result = run_statistics_on_slice(slice_info, duckdb_conn, session)
            if result.success:
                stats_count += 1
            else:
                errors.append(f"Stats {slice_info.slice_table_name}: {result.error}")

    # Run quality on each slice
    if run_quality:
        for slice_info in slice_infos:
            result = run_quality_on_slice(slice_info, duckdb_conn, session)
            if result.success:
                quality_count += 1
            else:
                errors.append(f"Quality {slice_info.slice_table_name}: {result.error}")

    return SliceAnalysisResult(
        slices_registered=len(slice_infos),
        slices_analyzed=len(slice_infos),
        statistics_computed=stats_count,
        quality_assessed=quality_count,
        errors=errors,
    )


__all__ = [
    "SliceTableInfo",
    "SliceAnalysisResult",
    "register_slice_tables",
    "run_analysis_on_slices",
    "run_statistics_on_slice",
    "run_quality_on_slice",
]
