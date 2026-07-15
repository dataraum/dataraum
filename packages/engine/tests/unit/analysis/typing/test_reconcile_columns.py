"""``reconcile_typed_columns`` must never delete a minted surrogate (DAT-766).

A re-run of ``addSource`` over a workspace that already minted a surrogate key
failed the typing phase: ``resolve_types`` builds its ``desired`` column set from
the RAW source's columns only, so an engine-minted ``_sk__*`` column (DAT-277)
present on the typed table from a prior run looks "dropped" and the reconcile
tried to DELETE it — while the surrogate relationship the mint persisted still
referenced it (``ForeignKeyViolation`` → ``PhaseFailed`` → cascade dead). The
surrogate mint owns the ``_sk__*`` lifecycle; typing must leave those columns
alone.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.relationships.surrogate import SURROGATE_PREFIX
from dataraum.analysis.typing.resolution import reconcile_typed_columns
from dataraum.storage import Column, Table, init_database

SURROGATE_COL = f"{SURROGATE_PREFIX}date__entry_id"


@pytest.fixture
def session_factory():
    """In-memory SQLite engine with all tables; FKs off so parent rows are optional."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        yield factory
    finally:
        engine.dispose()


def _seed_typed_table(factory: Any) -> None:
    """A typed table carrying a raw-derived column, a stale column, and a surrogate."""
    with factory() as session:
        session.add(
            Table(
                table_id="typed-1",
                source_id="src-1",
                table_name="journal_entries",
                layer="typed",
                duckdb_path="journal_entries",
            )
        )
        # ``entry_id`` still comes from the raw source; ``stale_col`` was dropped
        # from the source; ``_sk__*`` was minted onto the typed table by DAT-277.
        for pos, name in enumerate(("entry_id", "stale_col", SURROGATE_COL)):
            session.add(
                Column(
                    column_id=f"col-{name}",
                    table_id="typed-1",
                    column_name=name,
                    column_position=pos,
                    raw_type="VARCHAR",
                    resolved_type="VARCHAR",
                )
            )
        session.commit()


def _typed_column_names(factory: Any) -> set[str]:
    with factory() as session:
        return {
            c.column_name
            for c in session.execute(select(Column).where(Column.table_id == "typed-1")).scalars()
        }


def test_reconcile_preserves_surrogate_deletes_stale(session_factory: Any) -> None:
    """A re-type keeps the raw-derived + minted columns, deletes only the stale one."""
    _seed_typed_table(session_factory)

    # ``desired`` mirrors ``resolve_types``: it is built from the raw source and
    # therefore names ``entry_id`` only — neither the stale source column nor the
    # engine-minted surrogate appears in it.
    desired = [("entry_id", "entry_id", 0, "VARCHAR", "BIGINT")]

    with session_factory() as session:
        typed_table = session.execute(select(Table).where(Table.table_id == "typed-1")).scalar_one()
        column_map = reconcile_typed_columns(session, typed_table, desired)
        session.commit()

    survivors = _typed_column_names(session_factory)
    assert SURROGATE_COL in survivors, "minted surrogate must survive a re-type (DAT-766)"
    assert "stale_col" not in survivors, "a genuinely dropped source column is still deleted"
    assert "entry_id" in survivors
    # The reconcile only reports the reconciled (raw-desired) set; the surrogate is
    # left untouched, not re-owned by typing.
    assert set(column_map) == {"entry_id"}


def test_reconcile_updates_surviving_column_in_place(session_factory: Any) -> None:
    """The raw-derived column is UPDATED (id preserved), not re-created."""
    _seed_typed_table(session_factory)
    desired = [("entry_id", "entry_id", 0, "VARCHAR", "BIGINT")]

    with session_factory() as session:
        typed_table = session.execute(select(Table).where(Table.table_id == "typed-1")).scalar_one()
        column_map = reconcile_typed_columns(session, typed_table, desired)
        session.commit()

    assert column_map["entry_id"] == "col-entry_id"  # stable id, updated in place
    with session_factory() as session:
        entry = session.execute(
            select(Column).where(Column.column_id == "col-entry_id")
        ).scalar_one()
        assert entry.resolved_type == "BIGINT"  # re-typed value applied
