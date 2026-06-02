"""Session ⇿ table M:N model + source derivation (DAT-407).

A session carries no ``source_id``; it links typed tables through
``session_tables`` and its source(s) are derived by joining to
``Table.source_id``. An ``add_source`` run links one source's tables; a
``begin_session`` run may link tables spanning sources.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.orm import Session

from dataraum.investigation import link_session_tables, sources_for_session
from dataraum.investigation.db_models import InvestigationSession, SessionTable
from dataraum.storage import Source
from dataraum.storage.models import Table


def _source(session: Session, source_id: str) -> None:
    session.add(Source(source_id=source_id, name=f"src_{source_id}", source_type="csv"))


def _typed_table(session: Session, table_id: str, source_id: str) -> None:
    session.add(Table(table_id=table_id, source_id=source_id, table_name=table_id, layer="typed"))


def _make_session(session: Session, session_id: str) -> None:
    session.add(
        InvestigationSession(
            session_id=session_id,
            intent="test",
            status="active",
            started_at=datetime.now(UTC),
        )
    )


@pytest.fixture
def seeded(session: Session) -> Session:
    """Two sources, three typed tables (2 in src_a, 1 in src_b)."""
    _source(session, "src_a")
    _source(session, "src_b")
    _typed_table(session, "t_a1", "src_a")
    _typed_table(session, "t_a2", "src_a")
    _typed_table(session, "t_b1", "src_b")
    session.flush()
    return session


def _link(session: Session, session_id: str, table_id: str) -> None:
    session.add(SessionTable(session_id=session_id, table_id=table_id))


def test_single_source_session_derives_one_source(seeded: Session) -> None:
    _make_session(seeded, "sess_single")
    _link(seeded, "sess_single", "t_a1")
    _link(seeded, "sess_single", "t_a2")
    seeded.flush()

    assert sources_for_session(seeded, "sess_single") == {"src_a"}


def test_multi_source_session_derives_all_sources(seeded: Session) -> None:
    _make_session(seeded, "sess_multi")
    _link(seeded, "sess_multi", "t_a1")
    _link(seeded, "sess_multi", "t_b1")
    seeded.flush()

    assert sources_for_session(seeded, "sess_multi") == {"src_a", "src_b"}


def test_session_with_no_tables_derives_empty(seeded: Session) -> None:
    _make_session(seeded, "sess_empty")
    seeded.flush()

    assert sources_for_session(seeded, "sess_empty") == set()


def test_table_links_to_multiple_sessions(seeded: Session) -> None:
    """A typed table can be composed into more than one session (M:N)."""
    _make_session(seeded, "sess_one")
    _make_session(seeded, "sess_two")
    _link(seeded, "sess_one", "t_a1")
    _link(seeded, "sess_two", "t_a1")
    seeded.flush()

    assert sources_for_session(seeded, "sess_one") == {"src_a"}
    assert sources_for_session(seeded, "sess_two") == {"src_a"}


def test_ending_a_session_removes_only_its_links(seeded: Session) -> None:
    """Deleting a session cascades to its links; the typed table survives."""
    _make_session(seeded, "sess_doomed")
    _link(seeded, "sess_doomed", "t_a1")
    seeded.flush()

    doomed = seeded.get(InvestigationSession, "sess_doomed")
    seeded.delete(doomed)
    seeded.flush()

    # Link gone, table untouched.
    assert seeded.query(SessionTable).filter_by(session_id="sess_doomed").count() == 0
    assert seeded.get(Table, "t_a1") is not None


# --- link_session_tables (the typing-phase write helper) --------------------


def test_link_session_tables_writes_links_and_derives_source(seeded: Session) -> None:
    _make_session(seeded, "sess_link")

    count = link_session_tables(seeded, "sess_link", ["t_a1", "t_b1"])
    seeded.flush()

    assert count == 2
    assert seeded.query(SessionTable).filter_by(session_id="sess_link").count() == 2
    assert sources_for_session(seeded, "sess_link") == {"src_a", "src_b"}


def test_link_session_tables_is_idempotent(seeded: Session) -> None:
    """A teach re-type re-links the same tables — must not raise on the PK."""
    _make_session(seeded, "sess_idem")

    link_session_tables(seeded, "sess_idem", ["t_a1", "t_a2"])
    link_session_tables(seeded, "sess_idem", ["t_a1"])
    seeded.flush()

    assert seeded.query(SessionTable).filter_by(session_id="sess_idem").count() == 2


def test_link_session_tables_empty_is_noop(seeded: Session) -> None:
    _make_session(seeded, "sess_empty_link")

    assert link_session_tables(seeded, "sess_empty_link", []) == 0
    assert seeded.query(SessionTable).filter_by(session_id="sess_empty_link").count() == 0
