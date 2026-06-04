"""Unit tests for the DAT-414 materialization-recipe store + dependency order.

These pin the write-side contract directly (in-memory SQLite), complementing the
real-DuckDB round-trip/reset integration tests:

- ``store_recipe`` upserts on ``(table_id, layer, run_id)`` — a same-run re-store
  overwrites (Temporal at-least-once idempotency); a new run COEXISTS.
- ``_order_by_dependency`` runs a recipe AFTER the recipes it reads from, keyed
  on the fully-qualified target (layer-aware), and tolerates deps that are not
  themselves recipes (e.g. the raw table).
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.typing.recipe import _order_by_dependency, store_recipe
from dataraum.storage import init_database


@pytest.fixture
def session_factory():
    """In-memory SQLite with all tables; FKs off so we skip parent rows."""
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


def _store(session, *, table_id, layer, run_id, ddl, target_fqn, depends_on=None):  # noqa: ANN001
    store_recipe(
        session,
        session_id="sess-1",
        table_id=table_id,
        layer=layer,
        run_id=run_id,
        target_fqn=target_fqn,
        ddl=ddl,
        depends_on=depends_on,
    )


def _all(session_factory):  # noqa: ANN001
    with session_factory() as s:
        return list(s.execute(select(MaterializationRecipe)).scalars().all())


def test_store_recipe_persists_run_stamped_row(session_factory):
    with session_factory() as s:
        _store(
            s,
            table_id="tbl-1",
            layer="typed",
            run_id="run-A",
            ddl="CREATE OR REPLACE TABLE x AS SELECT 1",
            target_fqn='lake.typed."x"',
            depends_on=['lake.raw."x"'],
        )
        s.commit()

    rows = _all(session_factory)
    assert len(rows) == 1
    (row,) = rows
    assert row.table_id == "tbl-1"
    assert row.layer == "typed"
    assert row.run_id == "run-A"
    assert row.target_fqn == 'lake.typed."x"'
    assert row.depends_on == ['lake.raw."x"']


def test_store_recipe_same_run_overwrites(session_factory):
    """A re-store at the same (table, layer, run) overwrites — at-least-once safe."""
    with session_factory() as s:
        _store(s, table_id="tbl-1", layer="typed", run_id="run-A", ddl="V1", target_fqn="fqn-1")
        s.commit()
    with session_factory() as s:
        _store(s, table_id="tbl-1", layer="typed", run_id="run-A", ddl="V2", target_fqn="fqn-1")
        s.commit()

    rows = _all(session_factory)
    assert len(rows) == 1
    assert rows[0].ddl == "V2"


def test_store_recipe_new_run_coexists(session_factory):
    """A different run for the same artifact COEXISTS (versioned, not overwritten)."""
    with session_factory() as s:
        _store(s, table_id="tbl-1", layer="typed", run_id="run-A", ddl="A", target_fqn="fqn-1")
        _store(s, table_id="tbl-1", layer="typed", run_id="run-B", ddl="B", target_fqn="fqn-1")
        s.commit()

    rows = _all(session_factory)
    assert {r.run_id for r in rows} == {"run-A", "run-B"}
    assert {r.ddl for r in rows} == {"A", "B"}


def test_store_recipe_distinct_layers_coexist(session_factory):
    """typed + quarantine for the same table/run are distinct rows."""
    with session_factory() as s:
        _store(s, table_id="tbl-1", layer="typed", run_id="run-A", ddl="T", target_fqn="t-fqn")
        _store(s, table_id="tbl-1", layer="quarantine", run_id="run-A", ddl="Q", target_fqn="q-fqn")
        s.commit()

    rows = _all(session_factory)
    assert {r.layer for r in rows} == {"typed", "quarantine"}


def _recipe(target_fqn, depends_on=None):  # noqa: ANN001
    return MaterializationRecipe(
        session_id="s",
        table_id="t",
        layer="typed",
        run_id="r",
        target_fqn=target_fqn,
        ddl=f"-- {target_fqn}",
        depends_on=depends_on,
    )


class TestOrderByDependency:
    def test_dependency_runs_before_dependent(self):
        base = _recipe('lake.raw."x"')  # produced by an upstream recipe in-set
        dependent = _recipe('lake.typed."x"', depends_on=['lake.raw."x"'])
        ordered = _order_by_dependency([dependent, base])
        assert ordered.index(base) < ordered.index(dependent)

    def test_unknown_dependency_is_tolerated(self):
        """A dep that is not itself a recipe (e.g. the raw table) is 'already present'."""
        only = _recipe('lake.typed."x"', depends_on=['lake.raw."x"'])
        ordered = _order_by_dependency([only])
        assert ordered == [only]

    def test_same_bare_different_layer_does_not_self_depend(self):
        """typed + quarantine share the bare name; FQN keying keeps them independent."""
        typed = _recipe('lake.typed."x"', depends_on=['lake.raw."x"'])
        quarantine = _recipe('lake.quarantine."x"', depends_on=['lake.raw."x"'])
        ordered = _order_by_dependency([typed, quarantine])
        # Both placed exactly once, neither depends on the other.
        assert set(ordered) == {typed, quarantine}
        assert len(ordered) == 2
