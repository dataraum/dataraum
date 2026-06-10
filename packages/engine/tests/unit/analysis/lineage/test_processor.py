"""Aggregation-lineage discovery orchestration — DAT-491.

The candidate carries only the hypothesis (measure ← event, value expression);
the processor derives the alignment from the relationship catalog + semantic
time columns + the temporal profile's granularity. These tests seed that
substrate explicitly (Relationship / TableEntity / TemporalColumnProfile /
MetadataSnapshotHead) and run real DuckDB alignment + real reconciliation —
the agent is the only mocked-out seam, by construction: candidates enter as
data. In-memory SQLite with FKs off, same pattern as the resolve tests.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.models import LineageCandidate
from dataraum.analysis.lineage.processor import discover_aggregation_lineage
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.entropy.detectors.loaders import load_structural_reconciliation
from dataraum.storage import Column, Table, init_database
from dataraum.storage.snapshot_head import MetadataSnapshotHead

_RUN = "session-run-1"
_BASE_RUN = "addsource-run-1"
_SESSION = "sess-1"

_TABLES: dict[str, list[tuple[str, str]]] = {
    # name → [(column, resolved_type)]
    "trial_balance": [
        ("account_id", "BIGINT"),
        ("period", "DATE"),
        ("balance", "DOUBLE"),
        ("net_change", "DOUBLE"),
    ],
    "journal_lines": [
        ("account_id", "BIGINT"),
        ("entry_date", "DATE"),
        ("debit", "DOUBLE"),
        ("credit", "DOUBLE"),
    ],
    "journal_lines_split": [
        ("account_id", "BIGINT"),
        ("entry_id", "BIGINT"),
        ("debit", "DOUBLE"),
        ("credit", "DOUBLE"),
    ],
    "journal_entries": [("entry_id", "BIGINT"), ("entry_date", "DATE")],
    "chart_of_accounts": [("account_id", "BIGINT"), ("name", "VARCHAR")],
}

_TIME_COLUMNS = {
    "trial_balance": "period",
    "journal_lines": "entry_date",
    "journal_entries": "entry_date",
}


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
    # Two accounts × 12 monthly periods, period keyed as a month-first DATE.
    # balance carries forward the per-period net movement (a STOCK);
    # net_change equals it (a FLOW).
    conn.execute(
        """
        CREATE TABLE trial_balance AS
        WITH m AS (
          SELECT a.account_id, p.period, 40.0 + p.period * (1 + a.account_id) AS net
          FROM (SELECT UNNEST([1, 2]) AS account_id) a,
               (SELECT UNNEST(range(1, 13)) AS period) p
        )
        SELECT account_id,
               CAST(strptime('2025-' || lpad(CAST(period AS VARCHAR), 2, '0') || '-01',
                             '%Y-%m-%d') AS DATE) AS period,
               SUM(net) OVER (PARTITION BY account_id ORDER BY period) AS balance,
               net AS net_change
        FROM m
        """
    )
    conn.execute(
        """
        CREATE TABLE journal_lines AS
        SELECT account_id,
               CAST(strptime('2025-' || lpad(CAST(period AS VARCHAR), 2, '0') || '-15',
                             '%Y-%m-%d') AS DATE) AS entry_date,
               40.0 + period * (1 + account_id) AS debit,
               0.0 AS credit
        FROM (SELECT UNNEST([1, 2]) AS account_id) a,
             (SELECT UNNEST(range(1, 13)) AS period) p(period)
        """
    )
    # The split header/line shape (the canonical accounting layout): amounts on
    # the line table, the date on the header — bridgeable only via the catalog.
    conn.execute(
        """
        CREATE TABLE journal_entries AS
        SELECT period AS entry_id,
               CAST(strptime('2025-' || lpad(CAST(period AS VARCHAR), 2, '0') || '-15',
                             '%Y-%m-%d') AS DATE) AS entry_date
        FROM (SELECT UNNEST(range(1, 13)) AS period) p(period)
        """
    )
    conn.execute(
        """
        CREATE TABLE journal_lines_split AS
        SELECT account_id,
               period AS entry_id,
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
    """Seed Table/Column/TableEntity rows + the measure period's temporal profile.

    Returns name → id (tables under their name, columns under ``name.col``).
    """
    ids: dict[str, str] = {}
    for name, cols in _TABLES.items():
        table = Table(
            table_id=str(uuid4()),
            source_id="src-1",
            table_name=name,
            layer="typed",
            duckdb_path=name,
        )
        session.add(table)
        ids[name] = table.table_id
        for i, (col, col_type) in enumerate(cols):
            column = Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=col,
                column_position=i,
                resolved_type=col_type,
            )
            session.add(column)
            ids[f"{name}.{col}"] = column.column_id
        session.add(
            TableEntity(
                session_id=_SESSION,
                table_id=table.table_id,
                run_id=_RUN,
                detected_entity_type="test",
                time_column=_TIME_COLUMNS.get(name),
            )
        )
    # The measure period column's granularity, promoted under the add_source head.
    session.add(
        TemporalColumnProfile(
            profile_id=str(uuid4()),
            session_id=_SESSION,
            column_id=ids["trial_balance.period"],
            run_id=_BASE_RUN,
            profiled_at=datetime.now(UTC),
            min_timestamp=datetime(2025, 1, 1, tzinfo=UTC),
            max_timestamp=datetime(2025, 12, 1, tzinfo=UTC),
            detected_granularity="month",
            profile_data={},
        )
    )
    session.add(
        MetadataSnapshotHead(
            target=f"table:{ids['trial_balance']}", stage="temporal", run_id=_BASE_RUN
        )
    )
    session.flush()
    return ids


def _add_rel(
    session: Session, ids: dict[str, str], from_ref: str, to_ref: str, cardinality: str
) -> None:
    """Seed one defined (llm) relationship ``from_ref → to_ref`` (``table.col`` refs)."""
    from_table, _ = from_ref.split(".")
    to_table, _ = to_ref.split(".")
    session.add(
        Relationship(
            session_id=_SESSION,
            run_id=_RUN,
            from_table_id=ids[from_table],
            from_column_id=ids[from_ref],
            to_table_id=ids[to_table],
            to_column_id=ids[to_ref],
            relationship_type="foreign_key",
            cardinality=cardinality,
            confidence=0.9,
            detection_method="llm",
        )
    )
    session.flush()


def _shared_account_dim(session: Session, ids: dict[str, str], event_table: str) -> None:
    """The realistic key topology: both sides reference chart_of_accounts."""
    _add_rel(
        session, ids, "trial_balance.account_id", "chart_of_accounts.account_id", "many-to-one"
    )
    _add_rel(
        session, ids, f"{event_table}.account_id", "chart_of_accounts.account_id", "many-to-one"
    )


def _candidate(measure_column: str, event_table: str = "journal_lines") -> LineageCandidate:
    return LineageCandidate(
        measure_table="trial_balance",
        measure_column=measure_column,
        event_table=event_table,
        event_value_sql='"debit" - "credit"',
        event_filter_sql=None,
        rationale="balances roll up journal lines",
    )


def _discover(
    session: Session, duck: duckdb.DuckDBPyConnection, ids: dict[str, str], *cands: LineageCandidate
) -> int:
    return discover_aggregation_lineage(
        session,
        duck,
        candidates=list(cands),
        table_ids=[v for k, v in ids.items() if "." not in k],
        session_id=_SESSION,
        run_id=_RUN,
    )


class TestDiscoverAggregationLineage:
    def test_stock_measure_reconciles_cumulative(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed_tables(real_session)
        _shared_account_dim(real_session, ids, "journal_lines")
        assert _discover(real_session, duck, ids, _candidate("balance")) == 1
        row = real_session.execute(select(MeasureAggregationLineage)).scalar_one()
        assert row.pattern == "cumulative"
        assert row.measure_column_id == ids["trial_balance.balance"]
        assert row.event_table_id == ids["journal_lines"]
        assert row.match_rate > 0.99
        assert row.run_id == _RUN
        # The synthesized alignment is the provenance, derived not proposed.
        assert row.measure_key_sql == '"account_id"'
        assert "date_trunc('month'" in row.event_period_sql
        assert row.event_join_duckdb_path is None

    def test_flow_measure_reconciles_per_period(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed_tables(real_session)
        _shared_account_dim(real_session, ids, "journal_lines")
        assert _discover(real_session, duck, ids, _candidate("net_change")) == 1
        row = real_session.execute(select(MeasureAggregationLineage)).scalar_one()
        assert row.pattern == "per_period"

    def test_header_line_split_joins_via_catalog(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The line table has no date; the catalog's verified many-to-one edge to
        # the header (which has the time column) supplies the join — the
        # candidate proposes nothing about it.
        ids = _seed_tables(real_session)
        _shared_account_dim(real_session, ids, "journal_lines_split")
        _add_rel(
            real_session,
            ids,
            "journal_lines_split.entry_id",
            "journal_entries.entry_id",
            "many-to-one",
        )
        assert (
            _discover(
                real_session, duck, ids, _candidate("balance", event_table="journal_lines_split")
            )
            == 1
        )
        row = real_session.execute(select(MeasureAggregationLineage)).scalar_one()
        assert row.pattern == "cumulative"
        assert row.event_join_duckdb_path == "journal_entries"
        assert row.event_join_on_sql == 'e."entry_id" = h."entry_id"'
        assert row.match_rate > 0.99

    def test_no_entity_key_in_catalog_abstains(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed_tables(real_session)  # no relationships seeded
        assert _discover(real_session, duck, ids, _candidate("balance")) == 0

    def test_no_granularity_head_abstains(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed_tables(real_session)
        _shared_account_dim(real_session, ids, "journal_lines")
        real_session.execute(
            select(MetadataSnapshotHead)
        )  # head exists; drop it to simulate an unpromoted table
        for head in real_session.execute(select(MetadataSnapshotHead)).scalars().all():
            real_session.delete(head)
        real_session.flush()
        assert _discover(real_session, duck, ids, _candidate("balance")) == 0

    def test_broken_value_sql_drops_candidate(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed_tables(real_session)
        _shared_account_dim(real_session, ids, "journal_lines")
        cand = _candidate("balance").model_copy(update={"event_value_sql": '"no_such_column"'})
        assert _discover(real_session, duck, ids, cand) == 0

    def test_rerun_is_idempotent(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed_tables(real_session)
        _shared_account_dim(real_session, ids, "journal_lines")
        _discover(real_session, duck, ids, _candidate("balance"))
        _discover(real_session, duck, ids, _candidate("balance"))
        rows = real_session.execute(select(MeasureAggregationLineage)).scalars().all()
        assert len(rows) == 1

    def test_loader_is_exact_run(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed_tables(real_session)
        _shared_account_dim(real_session, ids, "journal_lines")
        _discover(real_session, duck, ids, _candidate("balance"))
        real_session.flush()
        column_id = ids["trial_balance.balance"]
        hit = load_structural_reconciliation(real_session, column_id, _RUN)
        assert hit is not None and hit["pattern"] == "cumulative"
        # Another run (e.g. an add_source detect) sees nothing → witness abstains.
        assert load_structural_reconciliation(real_session, column_id, "other-run") is None
        assert load_structural_reconciliation(real_session, column_id, None) is None
