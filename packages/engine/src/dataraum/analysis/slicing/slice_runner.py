"""Slice analysis runner.

Functions to materialize slice tables in DuckDB and register them in metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.naming import slice_table_name, slicing_view_name
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


def register_slice_tables(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_definitions: list[SliceDefinition] | None = None,
    *,
    run_id: str | None = None,
) -> Result[list[SliceTableInfo]]:
    """Register slice tables from DuckDB into metadata database.

    For each slice table found in DuckDB (matching pattern slice_*),
    creates Table and Column entries with layer='slice'.

    Args:
        session: Database session
        duckdb_conn: DuckDB connection
        slice_definitions: Optional list of slice definitions to use.
            If not provided, will query from database.
        run_id: Run scope for the fallback query (DAT-448). Definitions are
            run-versioned; pipeline callers pass ``slice_definitions`` already
            run-scoped, so this only matters for direct callers.

    Returns:
        Result containing list of registered SliceTableInfo
    """
    try:
        # Get THIS run's slice definitions if not provided (run-versioned, DAT-448)
        if slice_definitions is None:
            stmt = (
                select(SliceDefinition)
                .where(SliceDefinition.run_id == run_id)
                .order_by(SliceDefinition.slice_priority)
            )
            result = session.execute(stmt)
            slice_definitions = list(result.scalars().all())

        if not slice_definitions:
            return Result.ok([])

        # Get all existing slice views in DuckDB (SHOW TABLES includes views)
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
                slice_name = slice_table_name(
                    source_table.duckdb_path or "",
                    effective_column_name,
                    value,
                )

                # Check if table exists in DuckDB
                if slice_name not in slice_tables_in_duckdb:
                    continue

                # Stale-definition guard (DAT-405): slice VIEWS bind lazily, so a
                # definition whose column the CURRENT slicing view no longer
                # carries (e.g. an enriched join column on a run whose enriched
                # view is a passthrough) surfaces only here, as a Binder Error on
                # COUNT. Skip that slice with a warning — one stale definition
                # must not fail the phase and kill the whole begin_session run.
                try:
                    count_result = duckdb_conn.execute(
                        f'SELECT COUNT(*) FROM "{slice_name}"'
                    ).fetchone()
                except duckdb.Error as e:
                    logger.warning(
                        "slice_view_unbindable",
                        slice_name=slice_name,
                        slice_column=effective_column_name,
                        error=str(e),
                    )
                    continue
                row_count = count_result[0] if count_result else 0

                # Check if already registered (include source_id in query for correct uniqueness).
                # Earlier iterations flush before they fall through, so this query sees them.
                existing_stmt = select(Table).where(
                    Table.source_id == source_table.source_id,
                    Table.table_name == slice_name,
                    Table.layer == "slice",
                )
                existing_table = session.execute(existing_stmt).scalar_one_or_none()

                if existing_table:
                    # Already registered
                    registered.append(
                        SliceTableInfo(
                            slice_table_id=existing_table.table_id,
                            slice_table_name=slice_name,
                            source_table_id=source_table.table_id,
                            source_table_name=source_table.table_name,
                            slice_column_name=effective_column_name,
                            slice_value=value,
                            row_count=row_count,
                        )
                    )
                    continue

                # Create Table entry for slice
                # Generate table_id explicitly since SQLAlchemy defaults only apply at INSERT time
                slice_table = Table(
                    table_id=str(uuid4()),
                    source_id=source_table.source_id,
                    table_name=slice_name,
                    layer="slice",
                    duckdb_path=slice_name,
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
                    Table.table_name == slicing_view_name(source_table.duckdb_path or ""),
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
                    duckdb_cols = duckdb_conn.execute(f'DESCRIBE "{slice_name}"').fetchall()
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

                # Flush so the next iteration's existing_stmt sees this slice.
                session.flush()

                registered.append(
                    SliceTableInfo(
                        slice_table_id=slice_table.table_id,
                        slice_table_name=slice_name,
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


__all__ = [
    "SliceTableInfo",
    "register_slice_tables",
]
