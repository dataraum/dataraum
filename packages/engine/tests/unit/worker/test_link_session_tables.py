"""``link_session_tables`` activity helper (DAT-407).

add_source links the session to its freshly-typed tables so the session's
source is derivable. The write is idempotent (upsert) so a teach replay
re-links without duplicate-key errors.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from dataraum.investigation import sources_for_session
from dataraum.investigation.db_models import InvestigationSession, SessionTable
from dataraum.storage import Source
from dataraum.storage.models import Table
from dataraum.worker.activity import link_session_tables
from dataraum.worker.contracts import SourceIdentity

_SESSION_ID = "sess_link"
_SOURCE_ID = "src_link"


class _Manager:
    """Minimal ConnectionManager double: ``session_scope`` yields a fixed session."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @contextmanager
    def session_scope(self) -> Any:
        yield self._session


def _seed(session: Session) -> _Manager:
    session.add(Source(source_id=_SOURCE_ID, name="src", source_type="csv"))
    session.add(
        InvestigationSession(
            session_id=_SESSION_ID,
            intent="test",
            status="active",
            started_at=datetime.now(UTC),
        )
    )
    for tid in ("typed_a", "typed_b"):
        session.add(Table(table_id=tid, source_id=_SOURCE_ID, table_name=tid, layer="typed"))
    session.flush()
    return _Manager(session)


def _identity() -> SourceIdentity:
    return SourceIdentity(workspace_id="test", source_id=_SOURCE_ID, session_id=_SESSION_ID)


def test_links_each_typed_table_and_derives_source(session: Session) -> None:
    manager = _seed(session)

    count = link_session_tables(manager, _identity(), ["typed_a", "typed_b"])

    assert count == 2
    assert session.query(SessionTable).filter_by(session_id=_SESSION_ID).count() == 2
    assert sources_for_session(session, _SESSION_ID) == {_SOURCE_ID}


def test_link_is_idempotent_across_replays(session: Session) -> None:
    manager = _seed(session)

    link_session_tables(manager, _identity(), ["typed_a", "typed_b"])
    # A teach replay re-links the same tables — must not raise on the PK.
    link_session_tables(manager, _identity(), ["typed_a"])

    assert session.query(SessionTable).filter_by(session_id=_SESSION_ID).count() == 2


def test_empty_table_set_writes_nothing(session: Session) -> None:
    manager = _seed(session)

    assert link_session_tables(manager, _identity(), []) == 0
    assert session.query(SessionTable).filter_by(session_id=_SESSION_ID).count() == 0
