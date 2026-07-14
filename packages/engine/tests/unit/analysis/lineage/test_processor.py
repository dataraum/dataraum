"""Aggregation-lineage discovery over inline aggregation — DAT-491/536.

Discovery pairs per-(slice value, period) sums across facts sharing a catalog
slice dimension and enumerates signed conventions as arithmetic — no LLM. Since
DAT-536 the substrate is computed INLINE (one ``GROUP BY dim, period`` over the
fact's enriched view) rather than read from ``TemporalSliceAnalysis``, so the
tests seed a DuckDB fixture whose grouped sums reproduce exactly what the value
layer used to persist — the verdict-equivalence proof for the re-point. Metadata
is in-memory SQLite (FKs off, same pattern as the resolve tests); the queryable
source is an in-memory DuckDB table per fact (typed-fact fallback, no enriched
view seeded).
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
from dataraum.analysis.lineage.processor import discover_aggregation_lineage
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.entropy.detectors.loaders import load_structural_reconciliation
from dataraum.storage import Column, Table, init_database

_RUN = "session-run-1"
_DIM = "account_id__account_type"
# The persisted ``slice_dimension`` is now the conformed-identity label
# (``<dim table>.<attribute>``), not the per-fact physical column name (DAT-756).
_DIM_LABEL = "chart_of_accounts.account_type"
_VALUES = ("assets", "liabilities")
_MONTHS = list(range(1, 13))


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
    try:
        yield conn
    finally:
        conn.close()


def _net(k: int, i: int) -> float:
    """The per-period movement for entity k in period i (the DAT-459 shape)."""
    return 40.0 + (i + 1) * (1 + k)


def _seed(
    session: Session,
    duck: duckdb.DuckDBPyConnection,
    *,
    shared_dimension: bool = True,
    junk_column: bool = False,
    key_column: bool = False,
    multi_axis: bool = False,
    tb_dim_col: str = _DIM,
    jl_dim_col: str = _DIM,
    folded: bool = False,
) -> dict[str, str]:
    """Seed Tables/Columns/SliceDefinitions/TableEntity + the DuckDB rows.

    trial_balance carries ``balance`` (cumulative — a stock) and ``net_change``
    (per-period — a flow); journal_lines carries ``debit``/``credit`` whose
    per-period sums ARE the movement. journal_lines is finer-grained (2 rows per
    cell vs 1) so the direction gate orders it as the event side. Both facts
    sliced by the same dimension. The DuckDB rows are authored so the inline
    ``GROUP BY`` reproduces the canonical per-period sums.

    Dimension identity (DAT-756): each fact's slice carries its referenced identity
    ``(dimension_table_id -> chart_of_accounts, attribute = the ``fk__attr`` suffix,
    fk_role = the prefix)`` — so the pairing keys on the SHARED dim table + attribute,
    not the physical column name. ``tb_dim_col`` / ``jl_dim_col`` set each fact's
    physical slice column (default equal): differing FK names with the same suffix
    still pair (the false-negative). ``folded=True`` nulls the identity on both
    slices — an own-column dimension with no dim table (the false-positive: same
    name, no cross-table identity, must NOT pair).
    """
    ids: dict[str, str] = {}
    dim_table = Table(
        table_id=str(uuid4()),
        source_id="src-1",
        table_name="chart_of_accounts",
        layer="typed",
        duckdb_path="chart_of_accounts",
    )
    session.add(dim_table)
    ids["chart_of_accounts"] = dim_table.table_id
    extra = ["account_key"] if key_column else []
    for name, cols in (
        ("trial_balance", ["balance", "net_change", *extra]),
        ("journal_lines", ["debit", "credit", *extra, *(["line_id"] if junk_column else [])]),
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
        # The agent-named time axes the inline producer resolves (DAT-491/565).
        # multi_axis adds a SECOND, degenerate axis (``ship_date``, constant) on
        # the measure fact: the search must compete both and keep the good one.
        axes = [{"column": "period_date", "aspect": "period", "note": "Period."}]
        if multi_axis and name == "trial_balance":
            axes.append({"column": "ship_date", "aspect": "ship", "note": "Shipped."})
        session.add(
            TableEntity(
                run_id=_RUN,
                table_id=table.table_id,
                detected_entity_type="fact",
                time_columns=axes,
            )
        )

    sliced_tables = ["trial_balance", "journal_lines"] if shared_dimension else ["trial_balance"]
    dim_col_by_fact = {"trial_balance": tb_dim_col, "journal_lines": jl_dim_col}
    for name in sliced_tables:
        dim_col = dim_col_by_fact[name]
        if folded:
            dim_table_id: str | None = None
            attribute: str | None = None
            role: str | None = None
        else:
            dim_table_id = ids["chart_of_accounts"]
            role, _, attribute = dim_col.partition("__")
            attribute = attribute or None
        session.add(
            SliceDefinition(
                run_id=_RUN,
                table_id=ids[name],
                column_id=ids[f"{name}.{'balance' if name == 'trial_balance' else 'debit'}"],
                column_name=dim_col,
                dimension_table_id=dim_table_id,
                dimension_attribute=attribute,
                fk_role=role,
                slice_priority=1,
                distinct_values=list(_VALUES),
                value_count=len(_VALUES),
                detection_source="llm",
            )
        )
    session.flush()

    # DuckDB sources — one table per fact (typed-fact fallback). Columns mirror
    # the metadata above; the dimension is an enriched-style "fk__attr" name and may
    # differ between facts (DAT-756 — the identity is the dim table, not the name).
    tb_extra_cols = ", account_key DOUBLE" if key_column else ""
    tb_axis_col = ", ship_date DATE" if multi_axis else ""
    jl_extra_cols = (", account_key DOUBLE" if key_column else "") + (
        ", line_id DOUBLE" if junk_column else ""
    )
    duck.execute(
        f'CREATE TABLE trial_balance ("{tb_dim_col}" VARCHAR, period_date DATE,'
        f" balance DOUBLE, net_change DOUBLE{tb_extra_cols}{tb_axis_col})"
    )
    duck.execute(
        f'CREATE TABLE journal_lines ("{jl_dim_col}" VARCHAR, period_date DATE,'
        f" debit DOUBLE, credit DOUBLE{jl_extra_cols})"
    )

    tb_rows: list[str] = []
    jl_rows: list[str] = []
    for k, value in enumerate(_VALUES, start=1):
        running = 0.0
        for i, month in enumerate(_MONTHS):
            net = _net(k, i)
            running += net
            d = f"DATE '2025-{month:02d}-15'"
            tb_extra = f", {float(k * 100)}" if key_column else ""
            # Degenerate second axis: every row shares one ship_date, collapsing
            # the per-period series to a single bucket so this axis cannot win.
            tb_axis_val = ", DATE '2025-01-15'" if multi_axis else ""
            tb_rows.append(f"('{value}', {d}, {running}, {net}{tb_extra}{tb_axis_val})")
            # Two finer-grained event rows summing to the movement.
            for half in (net / 2, net - net / 2):
                jl_extra = f", {float(k * 100)}" if key_column else ""
                if junk_column:
                    jl_extra += f", {float((i * 7919 + k * 104729) % 1000)}"
                jl_rows.append(f"('{value}', {d}, {half}, 0.0{jl_extra})")
    duck.execute(f"INSERT INTO trial_balance VALUES {', '.join(tb_rows)}")
    duck.execute(f"INSERT INTO journal_lines VALUES {', '.join(jl_rows)}")
    return ids


def _discover(session: Session, duck: duckdb.DuckDBPyConnection, ids: dict[str, str]) -> int:
    return discover_aggregation_lineage(
        session,
        duckdb_conn=duck,
        table_ids=[ids["trial_balance"], ids["journal_lines"]],
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
    def test_stock_measure_reconciles_cumulative(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed(real_session, duck)
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        assert row.slice_dimension == _DIM_LABEL
        assert row.match_rate > 0.99
        assert row.run_id == _RUN
        # The winning convention reproduces the movement exactly: the single
        # column "debit" (ties with "debit" - "credit" break to singles-first).
        assert row.convention_sql == '"debit"'

    def test_flow_measure_reconciles_per_period(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed(real_session, duck)
        _discover(real_session, duck, ids)
        row = _row_for(real_session, ids["trial_balance.net_change"])
        assert row is not None
        assert row.pattern == "per_period"
        assert row.event_table_id == ids["journal_lines"]

    def test_competes_time_axes_and_keeps_best(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """A measure with TWO time axes (DAT-565): the search buckets by each and
        keeps the best-reconciling verdict. The degenerate ``ship_date`` axis (all
        rows in one period) cannot reconcile; the good ``period_date`` axis still
        wins, identical to the single-axis verdict — a bad axis never degrades it.
        """
        ids = _seed(real_session, duck, multi_axis=True)
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        assert row.match_rate > 0.99
        assert row.convention_sql == '"debit"'

    def test_junk_numeric_column_does_not_win(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # A pseudo-random sum column (an id-ish artifact) offers garbage
        # conventions — the residual ranking must still pick the true one.
        ids = _seed(real_session, duck, junk_column=True)
        _discover(real_session, duck, ids)
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert "line_id" not in row.convention_sql
        assert row.match_rate > 0.99

    def test_no_inverted_lineage_rows(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # The direction gate: journal_lines is finer-grained than trial_balance,
        # so NO row may claim a line column aggregates the summary table —
        # the silent inversion the senior review caught on the live run.
        ids = _seed(real_session, duck)
        _discover(real_session, duck, ids)
        for col in ("journal_lines.debit", "journal_lines.credit"):
            assert _row_for(real_session, ids[col]) is None, f"inverted lineage for {col}"

    def test_key_columns_excluded(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        # A catalog-evidenced key column (relationship endpoint) is neither a
        # measure nor a convention term — identity sums are not quantities.
        from dataraum.analysis.relationships.db_models import Relationship

        ids = _seed(real_session, duck, key_column=True)
        real_session.add(
            Relationship(
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
        _discover(real_session, duck, ids)
        assert _row_for(real_session, ids["trial_balance.account_key"]) is None
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert "account_key" not in row.convention_sql

    def test_no_shared_dimension_abstains(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed(real_session, duck, shared_dimension=False)
        assert _discover(real_session, duck, ids) == 0

    def test_differently_named_fks_pair_via_dim_identity(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """False-negative closes (DAT-756): two facts joining ONE dim table through
        DIFFERENTLY-NAMED FK columns (``gl_account__type`` / ``account_no__type``)
        share the dimension by referenced identity (chart_of_accounts.type), so the
        stock/flow witness runs. Under the old ``column_name`` grouping the pair was
        never formed and the witness was silently inert."""
        ids = _seed(
            real_session, duck, tb_dim_col="gl_account__type", jl_dim_col="account_no__type"
        )
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        assert row.slice_dimension == "chart_of_accounts.type"

    def test_folded_same_named_columns_do_not_pair(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """False-positive closes (DAT-756): two unrelated same-named FOLDED columns
        (own ``status`` column, no dim table -> null identity) are NOT paired. The
        bare name collision that would have fired a spurious witness now abstains —
        a folded dimension has no cross-table identity in Phase A (DAT-757)."""
        ids = _seed(real_session, duck, tb_dim_col="status", jl_dim_col="status", folded=True)
        assert _discover(real_session, duck, ids) == 0

    def test_rerun_is_idempotent(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Success-redelivery (same run_id, committed rows) converges by upsert (DAT-502)."""
        ids = _seed(real_session, duck)
        first = _discover(real_session, duck, ids)
        real_session.commit()  # the redelivered attempt sees committed rows
        second = _discover(real_session, duck, ids)
        real_session.commit()
        assert first == second
        rows = real_session.execute(select(MeasureAggregationLineage)).scalars().all()
        assert len(rows) == first
        assert all(r.run_id == _RUN for r in rows)

    def test_loader_is_exact_run(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        ids = _seed(real_session, duck)
        _discover(real_session, duck, ids)
        real_session.flush()
        column_id = ids["trial_balance.balance"]
        hit = load_structural_reconciliation(real_session, column_id, _RUN)
        assert hit is not None and hit["pattern"] == "cumulative"
        # Another run (e.g. an add_source detect) sees nothing → witness abstains.
        assert load_structural_reconciliation(real_session, column_id, "other-run") is None
        assert load_structural_reconciliation(real_session, column_id, None) is None
