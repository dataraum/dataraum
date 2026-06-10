"""Aggregation-lineage discovery orchestration — DAT-491.

Real DuckDB alignment + real reconciliation over injected candidates (no LLM in
unit tests — the agent is the only mocked-out seam, by construction: candidates
enter as data). In-memory SQLite with FKs off, same pattern as the resolve tests.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.models import LineageCandidate
from dataraum.analysis.lineage.processor import discover_aggregation_lineage
from dataraum.entropy.detectors.loaders import load_structural_reconciliation
from dataraum.storage import Column, Table, init_database

_RUN = "session-run-1"
_SESSION = "sess-1"


@pytest.fixture
def real_session() -> Iterator[Session]:
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


@pytest.fixture
def duck() -> Iterator[duckdb.DuckDBPyConnection]:
    conn = duckdb.connect(":memory:")
    # Two accounts × 12 periods. balance carries forward the per-period net
    # movement of the journal lines (a STOCK); net_change equals it (a FLOW).
    conn.execute(
        """
        CREATE TABLE trial_balance AS
        WITH m AS (
          SELECT a.account_id, p.period, 40.0 + p.period * (1 + a.account_id) AS net
          FROM (SELECT UNNEST([1, 2]) AS account_id) a,
               (SELECT UNNEST(range(1, 13)) AS period) p
        )
        SELECT account_id,
               '2025-' || lpad(CAST(period AS VARCHAR), 2, '0') AS period,
               SUM(net) OVER (PARTITION BY account_id ORDER BY period) AS balance,
               net AS net_change
        FROM m
        """
    )
    conn.execute(
        """
        CREATE TABLE journal_lines AS
        SELECT account_id,
               '2025-' || lpad(CAST(period AS VARCHAR), 2, '0') || '-15' AS entry_date,
               40.0 + period * (1 + account_id) AS debit,
               0.0 AS credit
        FROM (SELECT UNNEST([1, 2]) AS account_id) a,
             (SELECT UNNEST(range(1, 13)) AS period) p(period)
        """
    )
    try:
        yield conn
    finally:
        conn.close()


def _seed_tables(session: Session) -> dict[str, str]:
    """Seed Table/Column rows; returns name → id for assertions."""
    ids: dict[str, str] = {}
    for name, cols in (
        ("trial_balance", ["account_id", "period", "balance", "net_change"]),
        ("journal_lines", ["account_id", "entry_date", "debit", "credit"]),
    ):
        table = Table(
            table_id=str(uuid4()),
            source_id="src-1",
            table_name=name,
            layer="typed",
            duckdb_path=name,
        )
        session.add(table)
        ids[name] = table.table_id
        for i, col in enumerate(cols):
            column = Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=col,
                column_position=i,
            )
            session.add(column)
            ids[f"{name}.{col}"] = column.column_id
    session.flush()
    return ids


def _candidate(measure_column: str, **overrides: str) -> LineageCandidate:
    base: dict[str, str | None] = {
        "measure_table": "trial_balance",
        "measure_duckdb_path": "trial_balance",
        "measure_column": measure_column,
        "event_table": "journal_lines",
        "event_duckdb_path": "journal_lines",
        "event_value_sql": '"debit" - "credit"',
        "measure_key_sql": '"account_id"',
        "event_key_sql": '"account_id"',
        "measure_period_sql": '"period"',
        "event_period_sql": "strftime(strptime(\"entry_date\", '%Y-%m-%d'), '%Y-%m')",
        "event_filter_sql": None,
        "rationale": "balances roll up journal lines",
    }
    base.update(overrides)
    return LineageCandidate.model_validate(base)


def _discover(
    session: Session, duck: duckdb.DuckDBPyConnection, ids: dict[str, str], *cands: LineageCandidate
) -> int:
    return discover_aggregation_lineage(
        session,
        duck,
        candidates=list(cands),
        table_ids=[ids["trial_balance"], ids["journal_lines"]],
        session_id=_SESSION,
        run_id=_RUN,
    )


class TestDiscoverAggregationLineage:
    def test_stock_measure_reconciles_cumulative(self, real_session: Session, duck) -> None:
        ids = _seed_tables(real_session)
        assert _discover(real_session, duck, ids, _candidate("balance")) == 1
        row = real_session.execute(select(MeasureAggregationLineage)).scalar_one()
        assert row.pattern == "cumulative"
        assert row.measure_column_id == ids["trial_balance.balance"]
        assert row.event_table_id == ids["journal_lines"]
        assert row.match_rate > 0.99
        assert row.run_id == _RUN

    def test_flow_measure_reconciles_per_period(self, real_session: Session, duck) -> None:
        ids = _seed_tables(real_session)
        assert _discover(real_session, duck, ids, _candidate("net_change")) == 1
        row = real_session.execute(select(MeasureAggregationLineage)).scalar_one()
        assert row.pattern == "per_period"

    def test_misaligned_period_bridge_drops_candidate(self, real_session: Session, duck) -> None:
        # A wrong bridge produces an empty join → coverage gate drops it; the
        # witness downstream abstains instead of guessing.
        ids = _seed_tables(real_session)
        bad = _candidate(
            "balance", event_period_sql="strftime(strptime(\"entry_date\", '%Y-%m-%d'), '%Y')"
        )
        assert _discover(real_session, duck, ids, bad) == 0

    def test_broken_sql_drops_candidate(self, real_session: Session, duck) -> None:
        ids = _seed_tables(real_session)
        bad = _candidate("balance", event_value_sql='"no_such_column"')
        assert _discover(real_session, duck, ids, bad) == 0

    def test_rerun_is_idempotent(self, real_session: Session, duck) -> None:
        ids = _seed_tables(real_session)
        _discover(real_session, duck, ids, _candidate("balance"))
        _discover(real_session, duck, ids, _candidate("balance"))
        rows = real_session.execute(select(MeasureAggregationLineage)).scalars().all()
        assert len(rows) == 1

    def test_loader_is_exact_run(self, real_session: Session, duck) -> None:
        ids = _seed_tables(real_session)
        _discover(real_session, duck, ids, _candidate("balance"))
        real_session.flush()
        column_id = ids["trial_balance.balance"]
        hit = load_structural_reconciliation(real_session, column_id, _RUN)
        assert hit is not None and hit["pattern"] == "cumulative"
        # Another run (e.g. an add_source detect) sees nothing → witness abstains.
        assert load_structural_reconciliation(real_session, column_id, "other-run") is None
        assert load_structural_reconciliation(real_session, column_id, None) is None
