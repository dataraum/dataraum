"""DAT-448: drift summaries + period analyses are run-versioned.

Both writers were append-only — a re-run within a session duplicated rows and
the detector loaders read the pile unscoped, cross-run. The persists now stamp
``run_id`` and replace only THIS run's prior rows (idempotent under Temporal
activity retry; earlier runs' rows untouched), and ``load_drift_summaries``
filters to one run.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.temporal_slicing.analyzer import (
    persist_drift_results,
    persist_period_results,
)
from dataraum.analysis.temporal_slicing.db_models import (
    ColumnDriftSummary,
    TemporalSliceAnalysis,
)
from dataraum.analysis.temporal_slicing.models import (
    ColumnDriftResult,
    PeriodAnalysisResult,
    PeriodMetrics,
)
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


def _drift_result(column_name: str = "amount") -> ColumnDriftResult:
    return ColumnDriftResult(
        column_name=column_name,
        max_js_divergence=0.4,
        mean_js_divergence=0.2,
        periods_analyzed=4,
        periods_with_drift=1,
    )


class TestDriftRunVersioning:
    def test_stamps_run_and_keeps_prior_runs(self, real_session):
        persist_drift_results(
            [_drift_result()], "slice_t", "date", real_session, session_id="s1", run_id="run-a"
        )
        persist_drift_results(
            [_drift_result()], "slice_t", "date", real_session, session_id="s1", run_id="run-b"
        )

        rows = real_session.execute(select(ColumnDriftSummary)).scalars().all()
        assert {r.run_id for r in rows} == {"run-a", "run-b"}
        assert len(rows) == 2  # one per run — NOT an append-duplicate

    def test_retry_replaces_only_this_run(self, real_session):
        persist_drift_results(
            [_drift_result()], "slice_t", "date", real_session, session_id="s1", run_id="run-a"
        )
        # Temporal at-least-once retry of the same activity (same run_id):
        persist_drift_results(
            [_drift_result()], "slice_t", "date", real_session, session_id="s1", run_id="run-a"
        )

        rows = real_session.execute(select(ColumnDriftSummary)).scalars().all()
        assert len(rows) == 1  # replaced in place, no duplicate


class TestPeriodRunVersioning:
    def _result(self) -> PeriodAnalysisResult:
        return PeriodAnalysisResult(
            slice_table_name="slice_t",
            time_column="date",
            total_periods=1,
            incomplete_periods=0,
            anomaly_count=0,
            period_metrics=[
                PeriodMetrics(
                    period_label="2024-Q1",
                    period_start=date(2024, 1, 1),
                    period_end=date(2024, 3, 31),
                    row_count=10,
                    expected_days=91,
                    observed_days=91,
                    coverage_ratio=1.0,
                    last_day_ratio=1.0,
                )
            ],
            completeness_results=[],
            volume_anomalies=[],
        )

    def test_stamps_run_and_retry_is_idempotent(self, real_session):
        persist_period_results(self._result(), real_session, session_id="s1", run_id="run-a")
        persist_period_results(self._result(), real_session, session_id="s1", run_id="run-a")
        persist_period_results(self._result(), real_session, session_id="s1", run_id="run-b")

        rows = real_session.execute(select(TemporalSliceAnalysis)).scalars().all()
        assert {r.run_id for r in rows} == {"run-a", "run-b"}
        assert len(rows) == 2
