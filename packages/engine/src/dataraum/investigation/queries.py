"""Read/write helpers over the run-table anchor (DAT-506).

A run links the typed tables it operated over (:class:`RunTable`); its source(s)
are derived by joining through to ``Table.source_id`` rather than stored on the
anchor.
"""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import RunTable
from dataraum.storage.models import Table


def link_run_tables(session: Session, run_id: str, table_ids: Iterable[str]) -> int:
    """Link a run to the typed tables it operates over (DAT-506).

    Writes one ``run_tables`` row per typed table so a run's source(s) are
    derivable (:func:`sources_for_run`) and the terminal sealing steps can
    re-read the run's table set without re-passing it. ``add_source`` and
    ``begin_session`` write their anchor from the workflow input at run start.
    Idempotent (``merge`` on the ``(run_id, table_id)`` PK) so a teach re-run
    re-links without a duplicate-key error.

    Returns the number of tables processed (a ``merge`` over an existing PK is a
    no-op, not a new row).
    """
    ids = list(table_ids)
    for table_id in ids:
        session.merge(RunTable(run_id=run_id, table_id=table_id))
    return len(ids)


def tables_for_run(session: Session, run_id: str) -> list[str]:
    """Return the typed-table ids a run operated over, via ``run_tables``.

    The relational scope key for the analysis/readiness layer: the detect step +
    readiness persist run over *these* tables, not "a source's typed tables".

    Args:
        session: active SQLAlchemy session bound to the ``ws_<id>`` schema.
        run_id: the run id to resolve.

    Returns:
        The linked ``typed`` table ids (empty if the run links none).
    """
    rows = session.execute(
        select(Table.table_id)
        .join(RunTable, RunTable.table_id == Table.table_id)
        .where(RunTable.run_id == run_id, Table.layer == "typed")
    )
    return [table_id for (table_id,) in rows]


def sources_for_run(session: Session, run_id: str) -> set[str]:
    """Return the distinct source ids of the tables a run operated over.

    The anchor carries no ``source_id``; a run's source(s) are derived from its
    linked tables. An ``add_source`` run yields a single source; a
    ``begin_session`` selection may span several.

    Args:
        session: active SQLAlchemy session bound to the ``ws_<id>`` schema.
        run_id: the run id to resolve.

    Returns:
        The set of ``Table.source_id`` values reachable through this run's
        ``run_tables`` links (empty if the run links no tables).
    """
    rows = session.execute(
        select(Table.source_id)
        .join(RunTable, RunTable.table_id == Table.table_id)
        .where(RunTable.run_id == run_id)
        .distinct()
    )
    return {source_id for (source_id,) in rows}
