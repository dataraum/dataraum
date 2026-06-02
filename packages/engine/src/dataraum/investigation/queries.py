"""Read helpers over the session model.

A session links typed tables (:class:`SessionTable`); its source(s) are derived
by joining through to ``Table.source_id`` rather than stored on the session.
"""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import SessionTable
from dataraum.storage.models import Table


def link_session_tables(session: Session, session_id: str, table_ids: Iterable[str]) -> int:
    """Link a session to the typed tables it composes (DAT-407).

    Writes one ``session_tables`` row per typed table so a session's source(s)
    are derivable (:func:`sources_for_session`) without the session storing a
    ``source_id``. Written by the ``typing`` phase as a side-effect of typed-table
    creation — same transaction as the ``Table`` row — so add_source's run-session
    owns its tables without a separate workflow step. Idempotent (``merge`` on the
    ``(session_id, table_id)`` PK) so a teach re-type re-links without a
    duplicate-key error.

    Returns the number of tables processed (a ``merge`` over an existing PK is a
    no-op, not a new row).
    """
    ids = list(table_ids)
    for table_id in ids:
        session.merge(SessionTable(session_id=session_id, table_id=table_id))
    return len(ids)


def tables_for_session(session: Session, session_id: str) -> list[str]:
    """Return the typed-table ids a session composes, via ``session_tables``.

    The relational scope key for the analysis/readiness layer (DAT-410): the
    detect step + readiness persist run over *these* tables, not "a source's
    typed tables". For ``add_source`` the set is one source's freshly-typed
    tables (linked by ``typing``); for ``begin_session`` it is the user's
    selection, which may span sources.

    Args:
        session: active SQLAlchemy session bound to the ``ws_<id>`` schema.
        session_id: the investigation-session id to resolve.

    Returns:
        The linked ``typed`` table ids (empty if the session links none).
    """
    rows = session.execute(
        select(Table.table_id)
        .join(SessionTable, SessionTable.table_id == Table.table_id)
        .where(SessionTable.session_id == session_id, Table.layer == "typed")
    )
    return [table_id for (table_id,) in rows]


def sources_for_session(session: Session, session_id: str) -> set[str]:
    """Return the distinct source ids of the tables a session composes.

    The session model carries no ``source_id`` (DAT-407); a session's
    source(s) are derived from its linked tables. An ``add_source`` run yields
    a single source; a ``begin_session`` selection may span several.

    Args:
        session: active SQLAlchemy session bound to the ``ws_<id>`` schema.
        session_id: the investigation-session id to resolve.

    Returns:
        The set of ``Table.source_id`` values reachable through this session's
        ``session_tables`` links (empty if the session links no tables).
    """
    rows = session.execute(
        select(Table.source_id)
        .join(SessionTable, SessionTable.table_id == Table.table_id)
        .where(SessionTable.session_id == session_id)
        .distinct()
    )
    return {source_id for (source_id,) in rows}
