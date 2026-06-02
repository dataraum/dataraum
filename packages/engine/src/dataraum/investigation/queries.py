"""Read helpers over the session model.

A session links typed tables (:class:`SessionTable`); its source(s) are derived
by joining through to ``Table.source_id`` rather than stored on the session.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import SessionTable
from dataraum.storage.models import Table


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
