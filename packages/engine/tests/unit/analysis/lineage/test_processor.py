"""Aggregation-lineage discovery over the slice substrate — DAT-491.

Postgres-metadata-only: discovery pairs persisted per-(slice value, period)
sums (``TemporalSliceAnalysis.column_sums``) across facts sharing a slice
dimension and enumerates signed conventions as arithmetic — no DuckDB, no LLM.
The tests seed the substrate exactly as the value layer persists it.
In-memory SQLite with FKs off, same pattern as the resolve tests.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.processor import discover_aggregation_lineage
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.naming import slice_table_name
from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis
from dataraum.entropy.detectors.loaders import load_structural_reconciliation
from dataraum.storage import Column, Table, init_database

_RUN = "session-run-1"
_SESSION = "sess-1"
_DIM = "account_id__account_type"
_VALUES = ("assets", "liabilities")
_PERIODS = [f"2025-{m:02d}" for m in range(1, 13)]


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


def _net(k: int, i: int) -> float:
    """The per-period movement for entity k in period i (the DAT-459 shape)."""
    return 40.0 + (i + 1) * (1 + k)


def _seed(
    session: Session,
    *,
    shared_dimension: bool = True,
    junk_column: bool = False,
    key_column: bool = False,
) -> dict[str, str]:
    """Seed Tables/Columns/SliceDefinitions + the per-period slice sums.

    trial_balance carries ``balance`` (cumulative — a stock) and ``net_change``
    (per-period — a flow); journal_lines carries ``debit``/``credit`` whose
    per-period sums ARE the movement. Both facts sliced by the same dimension.
    """
    ids: dict[str, str] = {}
    extra = ["account_key"] if key_column else []
    for name, cols in (
        ("trial_balance", ["balance", "net_change", *extra]),
        ("journal_lines", ["debit", "credit", *extra]),
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
        for pos, col in enumerate(cols):
            column = Column(
                column_id=str(uuid4()),
                table_id=table.table_id,
                column_name=col,
                column_position=pos,
                resolved_type="DOUBLE",
            )
            session.add(column)
            ids[f"{name}.{col}"] = column.column_id

    sliced_tables = ["trial_balance", "journal_lines"] if shared_dimension else ["trial_balance"]
    for name in sliced_tables:
        session.add(
            SliceDefinition(
                session_id=_SESSION,
                run_id=_RUN,
                table_id=ids[name],
                column_id=ids[f"{name}.{'balance' if name == 'trial_balance' else 'debit'}"],
                column_name=_DIM,
                slice_priority=1,
                distinct_values=list(_VALUES),
                value_count=len(_VALUES),
                detection_source="llm",
            )
        )

    for k, value in enumerate(_VALUES, start=1):
        running = 0.0
        for i, label in enumerate(_PERIODS):
            net = _net(k, i)
            running += net
            tb_sums = {"balance": running, "net_change": net}
            jl_sums: dict[str, float] = {"debit": net, "credit": 0.0}
            if key_column:
                # Identical key sums on both sides — the identity-noise shape.
                tb_sums["account_key"] = float(k * 100)
                jl_sums["account_key"] = float(k * 100)
            if junk_column:
                jl_sums["line_id"] = float((i * 7919 + k * 104729) % 1000)
            for fact_name, sums, rows in (
                ("trial_balance", tb_sums, 5),
                ("journal_lines", jl_sums, 50),
            ):
                if fact_name == "journal_lines" and not shared_dimension:
                    continue
                session.add(
                    TemporalSliceAnalysis(
                        session_id=_SESSION,
                        run_id=_RUN,
                        slice_table_name=slice_table_name(fact_name, _DIM, value),
                        time_column="x",
                        period_label=label,
                        period_start=date(2025, i + 1, 1),
                        period_end=date(2025, i + 1, 28),
                        row_count=rows,
                        column_sums=sums,
                    )
                )
    session.flush()
    return ids


def _discover(session: Session, ids: dict[str, str]) -> int:
    return discover_aggregation_lineage(
        session,
        table_ids=[ids["trial_balance"], ids["journal_lines"]],
        session_id=_SESSION,
        run_id=_RUN,
        period_grain="monthly",
    )


def _row_for(session: Session, column_id: str) -> MeasureAggregationLineage | None:
    return session.execute(
        select(MeasureAggregationLineage).where(
            MeasureAggregationLineage.measure_column_id == column_id
        )
    ).scalar_one_or_none()


class TestDiscoverAggregationLineage:
    def test_stock_measure_reconciles_cumulative(self, real_session: Session) -> None:
        ids = _seed(real_session)
        assert _discover(real_session, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        assert row.slice_dimension == _DIM
        assert row.match_rate > 0.99
        assert row.run_id == _RUN
        # The winning convention reproduces the movement exactly: the single
        # column "debit" (ties with "debit" - "credit" break to singles-first).
        assert row.convention_sql == '"debit"'

    def test_flow_measure_reconciles_per_period(self, real_session: Session) -> None:
        ids = _seed(real_session)
        _discover(real_session, ids)
        row = _row_for(real_session, ids["trial_balance.net_change"])
        assert row is not None
        assert row.pattern == "per_period"
        assert row.event_table_id == ids["journal_lines"]

    def test_junk_numeric_column_does_not_win(self, real_session: Session) -> None:
        # A pseudo-random sum column (an id-ish artifact) offers garbage
        # conventions — the residual ranking must still pick the true one.
        ids = _seed(real_session, junk_column=True)
        _discover(real_session, ids)
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert "line_id" not in row.convention_sql
        assert row.match_rate > 0.99

    def test_no_inverted_lineage_rows(self, real_session: Session) -> None:
        # The direction gate: journal_lines is finer-grained than trial_balance,
        # so NO row may claim a line column aggregates the summary table —
        # the silent inversion the senior review caught on the live run.
        ids = _seed(real_session)
        _discover(real_session, ids)
        for col in ("journal_lines.debit", "journal_lines.credit"):
            assert _row_for(real_session, ids[col]) is None, f"inverted lineage for {col}"

    def test_key_columns_excluded(self, real_session: Session) -> None:
        # A catalog-evidenced key column (relationship endpoint) is neither a
        # measure nor a convention term — identity sums are not quantities.
        from dataraum.analysis.relationships.db_models import Relationship

        ids = _seed(real_session, key_column=True)
        real_session.add(
            Relationship(
                session_id=_SESSION,
                run_id=_RUN,
                from_table_id=ids["trial_balance"],
                from_column_id=ids["trial_balance.account_key"],
                to_table_id=ids["journal_lines"],
                to_column_id=ids["journal_lines.account_key"],
                relationship_type="foreign_key",
                cardinality="one-to-many",
                confidence=0.9,
                detection_method="llm",
            )
        )
        real_session.flush()
        _discover(real_session, ids)
        assert _row_for(real_session, ids["trial_balance.account_key"]) is None
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert "account_key" not in row.convention_sql

    def test_no_shared_dimension_abstains(self, real_session: Session) -> None:
        ids = _seed(real_session, shared_dimension=False)
        assert _discover(real_session, ids) == 0

    def test_rerun_is_idempotent(self, real_session: Session) -> None:
        """Success-redelivery (same run_id, committed rows) converges by upsert (DAT-502)."""
        ids = _seed(real_session)
        first = _discover(real_session, ids)
        real_session.commit()  # the redelivered attempt sees committed rows
        second = _discover(real_session, ids)
        real_session.commit()
        assert first == second
        rows = real_session.execute(select(MeasureAggregationLineage)).scalars().all()
        assert len(rows) == first
        assert all(r.run_id == _RUN for r in rows)

    def test_loader_is_exact_run(self, real_session: Session) -> None:
        ids = _seed(real_session)
        _discover(real_session, ids)
        real_session.flush()
        column_id = ids["trial_balance.balance"]
        hit = load_structural_reconciliation(real_session, column_id, _RUN)
        assert hit is not None and hit["pattern"] == "cumulative"
        # Another run (e.g. an add_source detect) sees nothing → witness abstains.
        assert load_structural_reconciliation(real_session, column_id, "other-run") is None
        assert load_structural_reconciliation(real_session, column_id, None) is None
