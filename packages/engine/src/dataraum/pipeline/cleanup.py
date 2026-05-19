"""Phase cleanup for --force re-execution.

Deletes a phase's output records so it can be re-run cleanly.
Each phase declares its own cleanup logic via the ``cleanup()`` method
and ``duckdb_layers`` property on ``BasePhase``.
"""

from __future__ import annotations

import duckdb
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.dml import Delete

from dataraum.core.logging import get_logger
from dataraum.pipeline.db_models import PhaseLog
from dataraum.storage.models import Column, Table

logger = get_logger(__name__)


def exec_delete(session: Session, stmt: Delete) -> int:  # type: ignore[type-arg]
    """Execute a DELETE statement and return the row count."""
    result = session.execute(stmt)
    rc: int = result.rowcount  # type: ignore[attr-defined]
    return rc


def get_table_ids(source_id: str, session: Session) -> list[str]:
    """Get all table_ids for a source."""
    stmt = select(Table.table_id).where(Table.source_id == source_id)
    return list(session.execute(stmt).scalars().all())


def get_column_ids(table_ids: list[str], session: Session) -> list[str]:
    """Get all column_ids for the given tables."""
    if not table_ids:
        return []
    stmt = select(Column.column_id).where(Column.table_id.in_(table_ids))
    return list(session.execute(stmt).scalars().all())


def get_slice_table_names(source_id: str, session: Session) -> list[str]:
    """Get table_name values for slice-layer tables."""
    stmt = select(Table.table_name).where(
        Table.source_id == source_id,
        Table.layer == "slice",
    )
    return list(session.execute(stmt).scalars().all())


def _collect_duckdb_paths(source_id: str, layers: list[str], session: Session) -> list[str]:
    """Collect DuckDB table names for the given layers before metadata is deleted."""
    stmt = select(Table.duckdb_path).where(
        Table.source_id == source_id,
        Table.layer.in_(layers),
        Table.duckdb_path.is_not(None),
    )
    return [p for p in session.execute(stmt).scalars().all() if p is not None]


def _drop_duckdb_tables(
    duckdb_conn: duckdb.DuckDBPyConnection, paths: list[str], layers: list[str]
) -> None:
    """Drop DuckDB tables/views that were collected before metadata cleanup."""
    # Enriched, slicing_view, and slice layers create VIEWs, other layers create TABLEs
    view_layers = {"enriched", "slicing_view", "slice"}
    has_views = bool(view_layers & set(layers))
    for path in paths:
        kind = "VIEW" if has_views else "TABLE"
        try:
            duckdb_conn.execute(f'DROP {kind} IF EXISTS "{path}"')
        except duckdb.Error:
            logger.debug(f"Could not drop DuckDB {kind} {path}")


def cleanup_phase_cascade(
    phase_name: str,
    source_id: str,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> list[str]:
    """Clean a phase and all downstream phases.

    Cleans in reverse dependency order (leaves first) to avoid FK issues.

    Args:
        phase_name: Root phase to clean.
        source_id: Source identifier to scope deletions.
        session: Active SQLAlchemy session (caller manages transaction).
        duckdb_conn: DuckDB connection for dropping tables.

    Returns:
        List of cleaned phase names in cleanup order.
    """
    from dataraum.pipeline.registry import get_all_dependencies, get_downstream_phases

    downstream = get_downstream_phases(phase_name)
    all_phases = {phase_name} | downstream

    # Topological reverse sort: phases with more dependencies go first
    # (they are "further downstream" and should be cleaned before their parents)
    def _dep_count(name: str) -> int:
        return len(get_all_dependencies(name))

    sorted_phases = sorted(all_phases, key=_dep_count, reverse=True)

    cleaned: list[str] = []
    for phase in sorted_phases:
        cleanup_phase(phase, source_id, session, duckdb_conn)
        cleaned.append(phase)

    return cleaned


def cleanup_phase(
    phase_name: str,
    source_id: str,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> int:
    """Delete a phase's output records and DuckDB tables for the given source.

    Resolves the phase from the registry and calls its ``cleanup()`` method.
    Phases that don't override ``cleanup()`` are treated as having no output
    to clean (e.g., import phase).

    Args:
        phase_name: Name of the phase to clean up.
        source_id: Source identifier to scope deletions.
        session: Active SQLAlchemy session (caller manages transaction).
        duckdb_conn: DuckDB connection for dropping tables created by the phase.

    Returns:
        Total number of records deleted.
    """
    from dataraum.pipeline.registry import get_registry

    registry = get_registry()
    phase_cls = registry.get(phase_name)
    if phase_cls is None:
        logger.debug("no_phase_in_registry", phase=phase_name)
        return 0

    phase = phase_cls()

    # Gather scope IDs
    table_ids = get_table_ids(source_id, session)
    column_ids = get_column_ids(table_ids, session)

    # Collect DuckDB paths BEFORE metadata cleanup deletes the records
    duckdb_layers = phase.duckdb_layers
    duckdb_paths = _collect_duckdb_paths(source_id, duckdb_layers, session) if duckdb_layers else []

    # Run phase-specific cleanup (deletes SQLite metadata)
    count = phase.cleanup(session, source_id, table_ids, column_ids)

    # Drop orphaned DuckDB tables/views
    if duckdb_paths:
        _drop_duckdb_tables(duckdb_conn, duckdb_paths, duckdb_layers)
        logger.info("duckdb_cleanup", phase=phase_name, tables_dropped=len(duckdb_paths))

    # Always delete phase logs for this phase + source
    count += exec_delete(
        session,
        delete(PhaseLog).where(
            PhaseLog.source_id == source_id,
            PhaseLog.phase_name == phase_name,
        ),
    )

    logger.info(
        "phase_cleanup_complete",
        phase=phase_name,
        source_id=source_id,
        records_deleted=count,
    )
    return count
