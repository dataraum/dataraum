"""DAT-448/DAT-502: per-period sums are a run-versioned form-(a) writer.

``persist_period_sums`` was the append-only writer's successor: it stamps
``run_id`` and UPSERTs on ``uq_tsa_slice_period_run`` (no run-scoped clear). A
Temporal success-redelivery — committed rows, ack lost, same ``run_id``
re-runs — converges in place, and earlier runs' rows stay untouched. Commits
between persists make the redelivery real (the prior attempt's rows are durable
when the retry fires).
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.temporal_slicing.analyzer import persist_period_sums
from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis
from dataraum.analysis.temporal_slicing.models import PeriodSums
from dataraum.storage.base import init_database


@pytest.fixture
def real_session():
    """In-memory SQLite session with all tables; FKs off so we skip parent rows."""
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
        with factory() as s:
            yield s
    finally:
        engine.dispose()


def _periods() -> list[PeriodSums]:
    return [
        PeriodSums(
            period_label="2024-01",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 2, 1),
            row_count=10,
            column_sums={"debit": 100.0},
        )
    ]


def _persist(session, run_id: str) -> None:
    persist_period_sums(_periods(), "slice_t", "date", session, run_id=run_id)


class TestPeriodSumRunVersioning:
    def test_stamps_run_and_keeps_prior_runs(self, real_session):
        _persist(real_session, "run-a")
        real_session.commit()
        _persist(real_session, "run-b")
        real_session.commit()

        rows = real_session.execute(select(TemporalSliceAnalysis)).scalars().all()
        assert {r.run_id for r in rows} == {"run-a", "run-b"}
        assert len(rows) == 2  # one per run — NOT an append-duplicate

    def test_retry_converges_by_upsert(self, real_session):
        _persist(real_session, "run-a")
        real_session.commit()  # the redelivered attempt sees committed rows
        _persist(real_session, "run-a")  # Temporal at-least-once redelivery
        real_session.commit()

        rows = real_session.execute(select(TemporalSliceAnalysis)).scalars().all()
        assert len(rows) == 1  # upsert converged in place, no duplicate
        assert rows[0].column_sums == {"debit": 100.0}
