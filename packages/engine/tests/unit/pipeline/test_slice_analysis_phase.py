"""Slice-analysis skip semantics — no cross-run stale skip (DAT-502).

Physical slice tables are NOT run-versioned, so their presence says nothing
about the current run. The old "All slices already analyzed" arm compared this
run's definition count against the cross-run slice-TABLE count — after a teach
re-run (fresh ``run_id``) the prior run's slice tables satisfied it and the
fresh run's analyses were silently skipped (the DAT-448 bug class). The arm is
gone: ``should_skip`` gates only on THIS run's slice definitions.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.slice_analysis_phase import SliceAnalysisPhase
from dataraum.storage import Table, init_database


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


def _seed_typed_table(session: Any) -> None:
    session.add(
        Table(
            table_id="tbl-1",
            source_id="src-1",
            table_name="orders",
            layer="typed",
            duckdb_path="csv__orders",
        )
    )


def _slice_def(run_id: str) -> SliceDefinition:
    return SliceDefinition(
        run_id=run_id,
        table_id="tbl-1",
        column_id="col-1",
        column_name="region",
        slice_priority=1,
        distinct_values=["eu", "us"],
        sql_template="CREATE OR REPLACE VIEW x AS SELECT 1",
    )


def _ctx(session: Any, run_id: str) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=None,
        table_ids=["tbl-1"],
        session_id="sess-1",
        run_id=run_id,
    )


def test_rerun_after_teach_is_not_skipped_by_prior_runs_slice_tables(
    session_factory: Any,
) -> None:
    """A fresh run with its own definitions runs even when slice tables exist.

    Run-A built and registered its physical slice tables (layer="slice" Table
    rows survive — the lake is latest-only). The teach re-run (run-B) writes
    fresh SliceDefinitions under its run_id; the phase must NOT skip on run-A's
    leftover slice tables — it re-executes the (CREATE OR REPLACE) slice DDL
    and produces run-B analyses.
    """
    phase = SliceAnalysisPhase()
    with session_factory() as session:
        _seed_typed_table(session)
        # Run-A's physical slice tables, registered as layer="slice" rows.
        for value in ("eu", "us"):
            session.add(
                Table(
                    table_id=f"slice-{value}",
                    source_id="src-1",
                    table_name=f"csv__orders_region_{value}",
                    layer="slice",
                    duckdb_path=f"csv__orders_region_{value}",
                )
            )
        # The teach re-run's fresh definitions under run-B.
        session.add(_slice_def("run-B"))
        session.commit()

        assert phase.should_skip(_ctx(session, "run-B")) is None


def test_skips_when_this_run_has_no_definitions(session_factory: Any) -> None:
    """The genuine precondition stays: no definitions for THIS run → skip.

    Run-A's definitions exist, but the current run (run-B) produced none
    (slicing skipped) — the run-scoped check skips without touching run-A's.
    """
    phase = SliceAnalysisPhase()
    with session_factory() as session:
        _seed_typed_table(session)
        session.add(_slice_def("run-A"))
        session.commit()

        reason = phase.should_skip(_ctx(session, "run-B"))
        assert reason == "No slice definitions found"
