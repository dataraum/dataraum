"""``assess_statistical_quality`` is idempotent under at-least-once retries (DAT-413).

A Temporal activity can commit then crash before acking, re-running with the
SAME ``run_id``. The quality writer now upserts ``StatisticalQualityMetrics`` on
``(column_id, run_id)`` instead of ``session.add`` — so a re-run does not
duplicate the metric row (which would make the head-resolved ``load_statistics``
loader's ``scalar_one_or_none()`` raise), and a second ``run_id`` for the same
column coexists.

Drives the real writer over an in-memory DuckDB table (no LLM, no lake).
"""

from __future__ import annotations

from typing import Any

import duckdb
import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.statistics.quality import assess_statistical_quality
from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics
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
    """In-memory DuckDB with one numeric typed table the writer can profile."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (amount DOUBLE)")
    # 200 rows so Benford has enough data; a couple of obvious IQR outliers.
    conn.execute("INSERT INTO orders SELECT (i % 90) + 10 FROM range(200) t(i)")
    conn.execute("INSERT INTO orders VALUES (100000), (200000)")
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
                layer="typed",
                duckdb_path="orders",
            )
        )
        session.add(
            Column(
                column_id="col-1",
                table_id="tbl-1",
                column_name="amount",
                column_position=0,
                resolved_type="DOUBLE",
            )
        )
        session.commit()


def test_reinsert_same_run_does_not_duplicate(session_factory: Any, duckdb_conn: Any) -> None:
    """Re-running the writer with the SAME run_id updates in place (the retry)."""
    _seed(session_factory)

    with session_factory() as session:
        res = assess_statistical_quality(
            "tbl-1", duckdb_conn, session, max_workers=1, run_id="run-A"
        )
        assert res.success
        session.commit()

    # The at-least-once retry: same run_id.
    with session_factory() as session:
        res = assess_statistical_quality(
            "tbl-1", duckdb_conn, session, max_workers=1, run_id="run-A"
        )
        assert res.success
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(
                select(StatisticalQualityMetrics).where(
                    StatisticalQualityMetrics.column_id == "col-1"
                )
            ).scalars()
        )
    assert len(rows) == 1  # no duplicate, no raise
    assert rows[0].run_id == "run-A"


def test_second_run_id_coexists(session_factory: Any, duckdb_conn: Any) -> None:
    """A second run's metric for the same column lands alongside the first."""
    _seed(session_factory)

    with session_factory() as session:
        assess_statistical_quality("tbl-1", duckdb_conn, session, max_workers=1, run_id="run-A")
        session.commit()
    with session_factory() as session:
        assess_statistical_quality("tbl-1", duckdb_conn, session, max_workers=1, run_id="run-B")
        session.commit()

    with session_factory() as session:
        rows = list(
            session.execute(
                select(StatisticalQualityMetrics).where(
                    StatisticalQualityMetrics.column_id == "col-1"
                )
            ).scalars()
        )
        total = session.scalar(select(func.count()).select_from(StatisticalQualityMetrics))
    assert total == 2
    assert {r.run_id for r in rows} == {"run-A", "run-B"}
