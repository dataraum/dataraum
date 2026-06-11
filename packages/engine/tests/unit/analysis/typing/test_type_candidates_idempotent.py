"""Both ``TypeCandidate`` writers are idempotent under at-least-once redelivery (DAT-502).

A Temporal activity can commit then crash before acking, re-running with the
SAME ``run_id``. ``TypeCandidate`` is many-per-column, so its identity widens
to ``(column_id, data_type, detected_pattern, run_id)`` — both writers (the
raw inference append in ``inference.py`` and the typed copies in
``resolution.py``, whose run-scoped clear is gone) UPSERT on that key:
a redelivered run converges on the same rows; a new run's rows coexist.

Drives the real writers over an in-memory DuckDB lake (ATTACH'd ``lake``
catalog with raw/typed/quarantine schemas) — no LLM, no object store.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.typing.db_models import TypeCandidate
from dataraum.analysis.typing.inference import infer_type_candidates
from dataraum.analysis.typing.resolution import resolve_types
from dataraum.storage import Column, Table, init_database


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


@pytest.fixture
def duckdb_conn():
    """In-memory DuckDB with the lake catalog layout the typing writers target."""
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS lake")
    for schema in ("raw", "typed", "quarantine"):
        conn.execute(f"CREATE SCHEMA lake.{schema}")
    conn.execute(
        'CREATE TABLE lake.raw."orders" AS '
        "SELECT * FROM (VALUES ('1'), ('2'), ('3'), ('41')) v(amount)"
    )
    try:
        yield conn
    finally:
        conn.close()


def _seed(factory: Any) -> None:
    with factory() as session:
        session.add(
            Table(
                table_id="tbl-1",
                source_id="src-1",
                table_name="orders",
                layer="raw",
                duckdb_path="orders",
            )
        )
        session.add(
            Column(
                column_id="col-1",
                table_id="tbl-1",
                column_name="amount",
                column_position=0,
                raw_type="VARCHAR",
            )
        )
        session.commit()


def _load_table(session: Any) -> Table:
    return session.execute(select(Table).where(Table.table_id == "tbl-1")).scalar_one()


def _candidate_rows(factory: Any) -> list[TypeCandidate]:
    with factory() as session:
        return list(session.execute(select(TypeCandidate)).scalars())


class TestRawInferenceWriter:
    """``infer_type_candidates`` — the raw append converted to upsert."""

    def test_redelivery_same_run_converges(self, session_factory: Any, duckdb_conn: Any) -> None:
        """Re-running the committed writer body with the SAME run_id converges."""
        _seed(session_factory)

        with session_factory() as session:
            res = infer_type_candidates(
                _load_table(session), duckdb_conn, session, session_id="sess-1", run_id="run-A"
            )
            assert res.success, res.error
            session.commit()
        baseline = {(r.data_type, r.detected_pattern) for r in _candidate_rows(session_factory)}
        assert baseline, "inference must produce at least one candidate"

        # The at-least-once redelivery: same run_id.
        with session_factory() as session:
            res = infer_type_candidates(
                _load_table(session), duckdb_conn, session, session_id="sess-1", run_id="run-A"
            )
            assert res.success, res.error
            session.commit()

        rows = _candidate_rows(session_factory)
        assert {(r.data_type, r.detected_pattern) for r in rows} == baseline
        assert len(rows) == len(baseline)  # converged — no duplicates
        assert all(r.run_id == "run-A" for r in rows)
        assert all(r.detected_pattern is not None for r in rows)  # '' is the no-pattern value

    def test_second_run_coexists(self, session_factory: Any, duckdb_conn: Any) -> None:
        """A second run's candidates land alongside the first run's."""
        _seed(session_factory)

        for run_id in ("run-A", "run-B"):
            with session_factory() as session:
                res = infer_type_candidates(
                    _load_table(session), duckdb_conn, session, session_id="sess-1", run_id=run_id
                )
                assert res.success, res.error
                session.commit()

        rows = _candidate_rows(session_factory)
        assert {r.run_id for r in rows} == {"run-A", "run-B"}
        per_run = {run: [r for r in rows if r.run_id == run] for run in ("run-A", "run-B")}
        assert len(per_run["run-A"]) == len(per_run["run-B"])


class TestTypedCopiesWriter:
    """``resolve_types`` typed copies — the run-scoped clear is gone; upsert converges."""

    def _infer_and_resolve(self, factory: Any, conn: Any, run_id: str) -> None:
        with factory() as session:
            res = infer_type_candidates(
                _load_table(session), conn, session, session_id="sess-1", run_id=run_id
            )
            assert res.success, res.error
            session.commit()
        with factory() as session:
            res = resolve_types(
                "tbl-1", conn, session, min_confidence=0.5, session_id="sess-1", run_id=run_id
            )
            assert res.success, res.error
            session.commit()

    def _resolve_again(self, factory: Any, conn: Any, run_id: str) -> None:
        """The redelivered resolution body alone (inference already committed)."""
        with factory() as session:
            res = resolve_types(
                "tbl-1", conn, session, min_confidence=0.5, session_id="sess-1", run_id=run_id
            )
            assert res.success, res.error
            session.commit()

    def test_redelivery_same_run_converges(self, session_factory: Any, duckdb_conn: Any) -> None:
        """Re-running resolve_types with the SAME run_id does not duplicate copies."""
        _seed(session_factory)
        self._infer_and_resolve(session_factory, duckdb_conn, "run-A")

        with session_factory() as session:
            typed_cols = {
                c.column_id
                for c in session.execute(select(Column)).scalars()
                if c.column_id != "col-1"
            }
        assert typed_cols, "resolution must mint typed columns"

        def _typed_copies() -> list[TypeCandidate]:
            return [r for r in _candidate_rows(session_factory) if r.column_id in typed_cols]

        baseline = {(r.column_id, r.data_type, r.detected_pattern) for r in _typed_copies()}
        assert baseline, "typed columns must carry candidate copies"

        # The at-least-once redelivery: resolve again under the same run_id.
        self._resolve_again(session_factory, duckdb_conn, "run-A")

        copies = _typed_copies()
        assert {(r.column_id, r.data_type, r.detected_pattern) for r in copies} == baseline
        assert len(copies) == len(baseline)  # converged — no duplicates

    def test_prior_runs_copies_untouched(self, session_factory: Any, duckdb_conn: Any) -> None:
        """A new run's typed copies coexist with the prior run's (no clear)."""
        _seed(session_factory)
        self._infer_and_resolve(session_factory, duckdb_conn, "run-A")
        self._infer_and_resolve(session_factory, duckdb_conn, "run-B")

        rows = _candidate_rows(session_factory)
        runs = {r.run_id for r in rows}
        assert runs == {"run-A", "run-B"}
        a_rows = [r for r in rows if r.run_id == "run-A"]
        b_rows = [r for r in rows if r.run_id == "run-B"]
        assert len(a_rows) == len(b_rows)  # run-A survived run-B in full
