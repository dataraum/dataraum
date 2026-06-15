"""Run ⇿ table anchor model + source derivation (DAT-506).

A run carries no ``source_id``; it links typed tables through ``run_tables`` and
its source(s) are derived by joining to ``Table.source_id``. An ``add_source``
run links one source's tables; a ``begin_session`` run may link tables spanning
sources. Sessions live in cockpit_db now (DAT-506) — the engine only records the
per-run typed-table set.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from dataraum.investigation import (
    link_run_tables,
    sources_for_run,
    tables_for_run,
)
from dataraum.investigation.db_models import RunTable
from dataraum.storage import Source
from dataraum.storage.models import Table


def _source(session: Session, source_id: str) -> None:
    session.add(Source(source_id=source_id, name=f"src_{source_id}", source_type="csv"))


def _typed_table(session: Session, table_id: str, source_id: str) -> None:
    session.add(Table(table_id=table_id, source_id=source_id, table_name=table_id, layer="typed"))


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


def _link(session: Session, run_id: str, table_id: str) -> None:
    session.add(RunTable(run_id=run_id, table_id=table_id))


def test_single_source_run_derives_one_source(seeded: Session) -> None:
    _link(seeded, "run_single", "t_a1")
    _link(seeded, "run_single", "t_a2")
    seeded.flush()

    assert sources_for_run(seeded, "run_single") == {"src_a"}


def test_multi_source_run_derives_all_sources(seeded: Session) -> None:
    _link(seeded, "run_multi", "t_a1")
    _link(seeded, "run_multi", "t_b1")
    seeded.flush()

    assert sources_for_run(seeded, "run_multi") == {"src_a", "src_b"}


def test_run_with_no_tables_derives_empty(seeded: Session) -> None:
    assert sources_for_run(seeded, "run_empty") == set()


def test_table_links_to_multiple_runs(seeded: Session) -> None:
    """A typed table can be composed into more than one run (M:N)."""
    _link(seeded, "run_one", "t_a1")
    _link(seeded, "run_two", "t_a1")
    seeded.flush()

    assert sources_for_run(seeded, "run_one") == {"src_a"}
    assert sources_for_run(seeded, "run_two") == {"src_a"}


# --- link_run_tables (the add_source/begin_session anchor write helper) -----


def test_link_run_tables_writes_links_and_derives_source(seeded: Session) -> None:
    count = link_run_tables(seeded, "run_link", ["t_a1", "t_b1"])
    seeded.flush()

    assert count == 2
    assert seeded.query(RunTable).filter_by(run_id="run_link").count() == 2
    assert sources_for_run(seeded, "run_link") == {"src_a", "src_b"}


def test_link_run_tables_is_idempotent(seeded: Session) -> None:
    """A teach re-run re-links the same tables — must not raise on the PK."""
    link_run_tables(seeded, "run_idem", ["t_a1", "t_a2"])
    link_run_tables(seeded, "run_idem", ["t_a1"])
    seeded.flush()

    assert seeded.query(RunTable).filter_by(run_id="run_idem").count() == 2


def test_link_run_tables_empty_is_noop(seeded: Session) -> None:
    assert link_run_tables(seeded, "run_empty_link", []) == 0
    assert seeded.query(RunTable).filter_by(run_id="run_empty_link").count() == 0


# --- tables_for_run (the detect/readiness scope key, DAT-410/506) -----------


def test_tables_for_run_returns_linked_typed_tables(seeded: Session) -> None:
    _link(seeded, "run_scope", "t_a1")
    _link(seeded, "run_scope", "t_b1")
    seeded.flush()

    assert set(tables_for_run(seeded, "run_scope")) == {"t_a1", "t_b1"}


def test_tables_for_run_empty_when_no_links(seeded: Session) -> None:
    assert tables_for_run(seeded, "run_none") == []


def test_tables_for_run_excludes_raw_layer(seeded: Session) -> None:
    """A link to a non-typed table is not returned (scope is typed only)."""
    seeded.add(Table(table_id="t_raw", source_id="src_a", table_name="t_raw", layer="raw"))
    seeded.flush()
    _link(seeded, "run_raw", "t_a1")
    _link(seeded, "run_raw", "t_raw")
    seeded.flush()

    assert tables_for_run(seeded, "run_raw") == ["t_a1"]
