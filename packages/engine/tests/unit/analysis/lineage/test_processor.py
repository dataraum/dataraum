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

from dataraum.analysis.hierarchies.db_models import BusMatrixEntry
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
    tb_role_play_col: str | None = None,
    role_play_wins: bool = False,
    folded: bool = False,
    axis_columns: bool = False,
    values: tuple[str, ...] = _VALUES,
    measure_axis_role: str = "event",
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

    ``tb_role_play_col`` adds a SECOND trial_balance slice at the SAME identity (a
    role-playing FK to the same dim + attribute), carrying its OWN ``Column`` row
    (a distinct ``column_id``, so the DAT-778 winning-slice assertions can tell the
    two slices apart). By default its DuckDB values are degenerate (``'other'`` —
    outside the catalog values, so its own series is empty). The witness must
    still fire off the primary slice: the two role slices must BOTH be tried, never
    collapsed to one (DAT-756 — the identity key groups a list per fact, not a
    last-write-wins single). ``role_play_wins=True`` inverts the competition: the
    role-play column labels EVERY entity truthfully while the primary covers only
    the first two ``values`` (rest ``'other'``) — both slices fire, and the
    role-play, enumerated second, must strictly win on support (Wilson LCB over
    more reconciling entities). Use with ≥ 4 ``values`` so the primary still
    clears ``MIN_ENTITIES_FIRED`` and genuinely competes.

    ``axis_columns=True`` also registers the time-axis names (``period_date``,
    plus ``ship_date`` under ``multi_axis``) as real typed ``Column`` rows —
    resolvable, like a real profiled date column would be. Off by default (the
    minimal fixture): a table's agent-named axis is unvalidated LLM output
    (DAT-780) and commonly WON'T resolve, so the default shape doubles as the
    DAT-778 NULL-``*_time_axis_column_id`` case every other test already
    exercises without asking for it.
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
        # DAT-780: the measure fact's axis role is parametrized so a test can prove
        # an attribute-role date is filtered out of the event-axis set. is_anchor is
        # only meaningful for an event axis; keep it false when the role is attribute.
        m_role = measure_axis_role if name == "trial_balance" else "event"
        axes = [
            {
                "column": "period_date",
                "aspect": "period",
                "role": m_role,
                "is_anchor": m_role == "event",
                "note": "Period.",
            }
        ]
        if multi_axis and name == "trial_balance":
            axes.append(
                {
                    "column": "ship_date",
                    "aspect": "ship",
                    "role": "event",
                    "is_anchor": False,
                    "note": "Shipped.",
                }
            )
        if axis_columns:
            for pos, tc in enumerate(axes, start=len(cols)):
                axis_column = Column(
                    column_id=str(uuid4()),
                    table_id=table.table_id,
                    column_name=tc["column"],
                    column_position=pos,
                    resolved_type="DATE",
                )
                session.add(axis_column)
                ids[f"{name}.{tc['column']}"] = axis_column.column_id
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
                distinct_values=list(values),
                value_count=len(values),
                detection_source="llm",
            )
        )
    # A second trial_balance slice at the SAME identity (role-playing FK): must NOT
    # collapse the primary slice (DAT-756). By default its DuckDB values are
    # degenerate so only the primary reconciles — proving both are tried, not one
    # arbitrarily kept; under ``role_play_wins`` the roles flip (see docstring).
    if tb_role_play_col:
        rp_role, _, rp_attr = tb_role_play_col.partition("__")
        rp_column = Column(
            column_id=str(uuid4()),
            table_id=ids["trial_balance"],
            column_name=tb_role_play_col,
            column_position=90,
            resolved_type="VARCHAR",
        )
        session.add(rp_column)
        ids[f"trial_balance.{tb_role_play_col}"] = rp_column.column_id
        session.add(
            SliceDefinition(
                run_id=_RUN,
                table_id=ids["trial_balance"],
                column_id=rp_column.column_id,
                column_name=tb_role_play_col,
                dimension_table_id=ids["chart_of_accounts"],
                dimension_attribute=rp_attr or None,
                fk_role=rp_role,
                slice_priority=2,
                distinct_values=list(values),
                value_count=len(values),
                detection_source="llm",
            )
        )
    session.flush()

    # DuckDB sources — one table per fact (typed-fact fallback). Columns mirror
    # the metadata above; the dimension is an enriched-style "fk__attr" name and may
    # differ between facts (DAT-756 — the identity is the dim table, not the name).
    tb_extra_cols = ", account_key DOUBLE" if key_column else ""
    tb_axis_col = ", ship_date DATE" if multi_axis else ""
    tb_role_col = f', "{tb_role_play_col}" VARCHAR' if tb_role_play_col else ""
    jl_extra_cols = (", account_key DOUBLE" if key_column else "") + (
        ", line_id DOUBLE" if junk_column else ""
    )
    duck.execute(
        f'CREATE TABLE trial_balance ("{tb_dim_col}" VARCHAR, period_date DATE,'
        f" balance DOUBLE, net_change DOUBLE{tb_extra_cols}{tb_axis_col}{tb_role_col})"
    )
    duck.execute(
        f'CREATE TABLE journal_lines ("{jl_dim_col}" VARCHAR, period_date DATE,'
        f" debit DOUBLE, credit DOUBLE{jl_extra_cols})"
    )

    tb_rows: list[str] = []
    jl_rows: list[str] = []
    for k, value in enumerate(values, start=1):
        running = 0.0
        for i, month in enumerate(_MONTHS):
            net = _net(k, i)
            running += net
            d = f"DATE '2025-{month:02d}-15'"
            tb_extra = f", {float(k * 100)}" if key_column else ""
            # Degenerate second axis: every row shares one ship_date, collapsing
            # the per-period series to a single bucket so this axis cannot win.
            tb_axis_val = ", DATE '2025-01-15'" if multi_axis else ""
            if role_play_wins:
                # The role-play column labels every entity truthfully; the primary
                # covers only the first two (rest 'other', outside the catalog
                # values → excluded from its series). Both slices fire; the
                # role-play reconciles MORE entities and must win on support.
                tb_dim_val = value if k <= 2 else "other"
                tb_role_val = f", '{value}'"
            else:
                # Degenerate role-play value: 'other' is outside the catalog values,
                # so this second same-identity slice contributes no series — only
                # the primary reconciles.
                tb_dim_val = value
                tb_role_val = ", 'other'" if tb_role_play_col else ""
            tb_rows.append(
                f"('{tb_dim_val}', {d}, {running}, {net}{tb_extra}{tb_axis_val}{tb_role_val})"
            )
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


def _seed_ref_cells(
    session: Session,
    ids: dict[str, str],
    per_fact_roles: dict[str, list[str]],
    group: str,
    *,
    dim: str = "chart_of_accounts",
) -> None:
    """Seed referenced bus-matrix cells assigning FK roles a shared role identity.

    The DAT-788 conform decision the lineage witness reads: giving several facts'
    FK roles the SAME ``group`` (as the conform judge would after a ``conform``
    verdict) makes those roles ONE identity. Without a cell a referenced slice
    falls back to the structural per-role signature — the safe default.
    """
    for fact, roles in per_fact_roles.items():
        session.add(
            BusMatrixEntry(
                run_id=_RUN,
                fact_table_id=ids[fact],
                attachment="referenced",
                concept_label=dim,
                dimension_table_id=ids[dim],
                roles=sorted(roles),
                attributes=[],
                confirmation_source="unconfirmed",
                conformed_group=group,
                needs_confirmation=False,
                signature=f"bus:referenced:{ids[fact]}:{ids[dim]}:" + "|".join(sorted(roles)),
            )
        )
    session.flush()


def _row_for(session: Session, column_id: str) -> MeasureAggregationLineage | None:
    return session.execute(
        select(MeasureAggregationLineage).where(
            MeasureAggregationLineage.measure_column_id == column_id
        )
    ).scalar_one_or_none()


def _seed_series(
    session: Session,
    duck: duckdb.DuckDBPyConnection,
    *,
    measure_col: str,
    measure_by_entity: dict[str, list[float]],
    event_cols: list[str],
    event_by_entity: dict[str, dict[str, list[float | None]]],
) -> dict[str, str]:
    """Seed one measure fact + one finer event fact with EXPLICIT per-entity series.

    The convention-selection tests (DAT-759) need adversarial numeric shapes the
    canonical ``_seed`` can't express: per-entity, per-column monthly values for
    the event side and the measure side independently. Each event cell is written
    as two half-rows so the direction gate keeps the event side finer-grained.
    ``None`` event values write SQL NULL — an all-NULL month drops that column
    from the period sums, the shape behind the own-subset-denominator trap.
    """
    ids: dict[str, str] = {}
    entities = sorted(measure_by_entity)
    months = len(next(iter(measure_by_entity.values())))
    dim_table = Table(
        table_id=str(uuid4()),
        source_id="src-1",
        table_name="chart_of_accounts",
        layer="typed",
        duckdb_path="chart_of_accounts",
    )
    session.add(dim_table)
    ids["chart_of_accounts"] = dim_table.table_id
    for name, cols in (("monthly_summary", [measure_col]), ("events", event_cols)):
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
        session.add(
            TableEntity(
                run_id=_RUN,
                table_id=table.table_id,
                detected_entity_type="fact",
                time_columns=[
                    {
                        "column": "period_date",
                        "aspect": "period",
                        "role": "event",
                        "is_anchor": True,
                        "note": "Period.",
                    }
                ],
            )
        )
        session.add(
            SliceDefinition(
                run_id=_RUN,
                table_id=table.table_id,
                column_id=ids[f"{name}.{cols[0]}"],
                column_name=_DIM,
                dimension_table_id=dim_table.table_id,
                dimension_attribute="account_type",
                fk_role="account_id",
                slice_priority=1,
                distinct_values=list(entities),
                value_count=len(entities),
                detection_source="llm",
            )
        )
    session.flush()

    duck.execute(
        f'CREATE TABLE monthly_summary ("{_DIM}" VARCHAR, period_date DATE, {measure_col} DOUBLE)'
    )
    event_ddl = ", ".join(f"{c} DOUBLE" for c in event_cols)
    duck.execute(f'CREATE TABLE events ("{_DIM}" VARCHAR, period_date DATE, {event_ddl})')
    m_rows: list[str] = []
    e_rows: list[str] = []
    for entity in entities:
        for i in range(months):
            d = f"DATE '2025-{i + 1:02d}-15'"
            m_rows.append(f"('{entity}', {d}, {measure_by_entity[entity][i]})")
            cell = [event_by_entity[entity][c][i] for c in event_cols]
            for half in (0.5, 0.5):
                halves = ", ".join("NULL" if v is None else str(v * half) for v in cell)
                e_rows.append(f"('{entity}', {d}, {halves})")
    duck.execute(f"INSERT INTO monthly_summary VALUES {', '.join(m_rows)}")
    duck.execute(f"INSERT INTO events VALUES {', '.join(e_rows)}")
    return ids


class TestConventionSelection:
    """DAT-759: support-first selection (Wilson LCB over the common denominator).

    Grounded by the eval probe ``scripts/probes/dat759-convention-selection``
    (dataraum-eval): min-residual selection picked value-wrong conventions on
    2/3 real measures; Wilson-LCB + ΔBIC arity tie-break selects truth 3/3.
    """

    def test_broad_single_beats_tighter_subset_artifact(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """The headline defect: ``net_amount`` (= debit − credit) fits a 2/4
        entity subset EXACTLY (credit ≡ 0 there) while the true ``debit`` fits
        all 4 entities with a small residual. Min-residual selection picked the
        subset artifact (0 < 0.01); support selection must pick ``debit``
        (LCB 4/4 ≈ 0.51 ≫ 2/4 ≈ 0.15)."""
        entities = ("assets", "equity", "expenses", "liabilities")
        debit = {e: [100.0 + 10 * k + 3 * i for i in range(12)] for k, e in enumerate(entities)}
        # 'assets'/'equity' carry credit movement; 'expenses'/'liabilities' none.
        credit = {
            e: [debit[e][i] / 3 if k < 2 else 0.0 for i in range(12)]
            for k, e in enumerate(entities)
        }
        net = {e: [debit[e][i] - credit[e][i] for i in range(12)] for e in entities}
        # The measure IS the debit movement — exact where credit is dead, 1%
        # off where it is not, so the artifact's subset fit is strictly tighter.
        measure = {
            e: [debit[e][i] * (1.01 if k < 2 else 1.0) for i in range(12)]
            for k, e in enumerate(entities)
        }
        ids = _seed_series(
            real_session,
            duck,
            measure_col="debit_total",
            measure_by_entity=measure,
            event_cols=["credit", "debit", "net_amount"],
            event_by_entity={
                e: {"debit": debit[e], "credit": credit[e], "net_amount": net[e]} for e in entities
            },
        )
        assert (
            discover_aggregation_lineage(
                real_session,
                duckdb_conn=duck,
                table_ids=[ids["monthly_summary"], ids["events"]],
                run_id=_RUN,
                period_grain="monthly",
            )
            > 0
        )
        row = _row_for(real_session, ids["monthly_summary.debit_total"])
        assert row is not None
        assert row.convention_sql == '"debit"'
        assert row.pattern == "per_period"
        assert row.match_rate > 0.99  # all 4 entities vote, unanimous

    def test_collinear_twin_breaks_to_single(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """``debit − net_amount`` is numerically IDENTICAL to ``credit`` (the
        roll-forward collinearity) — no data statistic can order the twins, so
        the LCB tie must break to the lower arity, never to enumeration order."""
        entities = ("assets", "equity", "expenses", "liabilities")
        debit = {e: [80.0 + 5 * k + 2 * i for i in range(12)] for k, e in enumerate(entities)}
        credit = {e: [30.0 + 3 * k + i for i in range(12)] for k, e in enumerate(entities)}
        net = {e: [debit[e][i] - credit[e][i] for i in range(12)] for e in entities}
        ids = _seed_series(
            real_session,
            duck,
            measure_col="credit_total",
            measure_by_entity=credit,
            event_cols=["credit", "debit", "net_amount"],
            event_by_entity={
                e: {"debit": debit[e], "credit": credit[e], "net_amount": net[e]} for e in entities
            },
        )
        discover_aggregation_lineage(
            real_session,
            duckdb_conn=duck,
            table_ids=[ids["monthly_summary"], ids["events"]],
            run_id=_RUN,
            period_grain="monthly",
        )
        row = _row_for(real_session, ids["monthly_summary.credit_total"])
        assert row is not None
        assert row.convention_sql == '"credit"'

    def test_true_difference_wins_by_bic(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Single-preference must not kill a TRUE difference: the measure is
        ``gross − fees`` (fees ≈ 10% of gross). ``gross`` alone also votes on
        every entity (residual ≈ 0.1 < FIRE_RESIDUAL_MAX) so the LCBs tie — the
        ΔBIC > 10 escape (probe: ΔBIC = 56.6) must keep the difference."""
        entities = ("assets", "equity", "expenses", "liabilities")
        gross = {e: [1000.0 + 50 * k + 20 * i for i in range(12)] for k, e in enumerate(entities)}
        fees = {e: [gross[e][i] * (0.08 + 0.01 * (i % 3)) for i in range(12)] for e in entities}
        measure = {
            e: [(gross[e][i] - fees[e][i]) * (1 + 0.001 * (-1) ** i) for i in range(12)]
            for e in entities
        }
        ids = _seed_series(
            real_session,
            duck,
            measure_col="net_revenue",
            measure_by_entity=measure,
            event_cols=["fees", "gross"],
            event_by_entity={e: {"gross": gross[e], "fees": fees[e]} for e in entities},
        )
        discover_aggregation_lineage(
            real_session,
            duckdb_conn=duck,
            table_ids=[ids["monthly_summary"], ids["events"]],
            run_id=_RUN,
            period_grain="monthly",
        )
        row = _row_for(real_session, ids["monthly_summary.net_revenue"])
        assert row is not None
        assert row.convention_sql == '"gross" - "fees"'
        assert row.pattern == "per_period"

    def test_own_subset_denominator_cannot_flatter_support(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """Probe leg b2 at the processor level — pins the LOAD-BEARING caveat
        that ``wilson_lcb`` is fed the pairing's common denominator, never the
        convention's own aligned subset (``verdict.n_entities``).

        ``partial`` is NULL for half the entities and fits its own subset
        perfectly: 5 unanimous voters. The true ``base`` reconciles 7 of the 10
        entities (three are deliberately off-scale and abstain). On the OWN
        subset the trap outranks truth — LCB(5/5) ≈ 0.57 > LCB(7/10) ≈ 0.40 —
        so reverting the denominator flips this test to ``partial``; on the
        common denominator the trap collapses to LCB(5/10) ≈ 0.24 and ``base``
        wins."""
        entities = tuple(f"e{i}" for i in range(10))
        base = {e: [200.0 + 15 * k + 5 * i for i in range(12)] for k, e in enumerate(entities)}
        partial: dict[str, list[float | None]] = {
            e: [base[e][i] if k < 5 else None for i in range(12)] for k, e in enumerate(entities)
        }
        # e0–e6 ARE the base movement; e7–e9 are 2× off-scale (residual 1.0 → abstain).
        measure = {
            e: [base[e][i] * (1.0 if k < 7 else 2.0) for i in range(12)]
            for k, e in enumerate(entities)
        }
        ids = _seed_series(
            real_session,
            duck,
            measure_col="total",
            measure_by_entity=measure,
            event_cols=["base", "partial"],
            event_by_entity={e: {"base": base[e], "partial": partial[e]} for e in entities},
        )
        discover_aggregation_lineage(
            real_session,
            duckdb_conn=duck,
            table_ids=[ids["monthly_summary"], ids["events"]],
            run_id=_RUN,
            period_grain="monthly",
        )
        row = _row_for(real_session, ids["monthly_summary.total"])
        assert row is not None
        assert row.convention_sql == '"base"'
        assert row.match_rate == pytest.approx(0.7)  # 7 of 10 aligned entities vote


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
        # column "debit" ("debit" - "credit" is its collinear twin here — the
        # DAT-759 support-LCB tie breaks to the lower arity).
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

        DAT-778: the WINNER of that competition persists — on both the measure
        and event side — instead of being discarded once the verdict is picked.
        ``axis_columns=True`` registers ``period_date``/``ship_date`` as real
        typed columns (as a profiled date column would be), so the winning
        axis's ``column_id`` resolves too, proving the persisted id is the
        winner's, never the loser's (``ship_date`` would also resolve here, so
        a wrong pick would silently pass without this fixture).
        """
        ids = _seed(real_session, duck, multi_axis=True, axis_columns=True)
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        assert row.match_rate > 0.99
        assert row.convention_sql == '"debit"'
        # The measure-side axis competition picked "period_date", never the
        # degenerate "ship_date" — resolved to the real Column axis_columns
        # registered, not the loser's.
        assert row.measure_time_axis_column == "period_date"
        assert row.measure_time_axis_column_id == ids["trial_balance.period_date"]
        # journal_lines only ever had one axis, but it must still be CAPTURED,
        # not silently dropped — the bug this ticket fixes discarded both sides.
        assert row.event_time_axis_column == "period_date"
        assert row.event_time_axis_column_id == ids["journal_lines.period_date"]
        # The winning physical slice column resolves straight off the
        # catalog's own SliceDefinition.column_id (schema-guaranteed NOT NULL).
        assert row.measure_slice_column_id == ids["trial_balance.balance"]
        assert row.event_slice_column_id == ids["journal_lines.debit"]

    def test_attribute_role_axis_is_excluded_from_rollup(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-780 Gap 1: an attribute-role date is never a rollup axis.

        The measure fact's only date is tagged role='attribute' (a due_date-style
        column). The consumer filters strictly on role='event', so trial_balance
        contributes no time axis, no measure series can form, and discovery
        abstains — proving the event/attribute rule is enforced at the consumer,
        not merely documented. With role='event' the SAME fixture reconciles.
        """
        ids_attr = _seed(real_session, duck, measure_axis_role="attribute")
        assert _discover(real_session, duck, ids_attr) == 0
        assert _row_for(real_session, ids_attr["trial_balance.balance"]) is None

    def test_time_axis_column_id_is_null_when_axis_name_unresolvable(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-778 NULL case: ``TimeColumn.column`` is unvalidated LLM output
        (DAT-780 tightens this at save) — the winning axis NAME always persists
        (it is literally what won the competition), but when that name doesn't
        resolve to a real ``Column`` on the table, the id is an honest NULL,
        never a sentinel string. The default (minimal) fixture doesn't register
        ``period_date`` as a ``Column`` — the natural shape of "a witness with
        no [resolvable] axis" this table can produce.
        """
        ids = _seed(real_session, duck)
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.measure_time_axis_column == "period_date"
        assert row.measure_time_axis_column_id is None
        assert row.event_time_axis_column == "period_date"
        assert row.event_time_axis_column_id is None
        # The physical slice column is unaffected by the axis-name gap — it is
        # always resolvable, straight from SliceDefinition.column_id.
        assert row.measure_slice_column_id == ids["trial_balance.balance"]
        assert row.event_slice_column_id == ids["journal_lines.debit"]

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
                # Stored canonically many→one (DAT-777 CHECK); direction is
                # incidental here — the test only needs a key-column relationship.
                cardinality="many-to-one",
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

    def test_differently_named_fks_pair_only_when_conformed(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-788: two facts joining ONE dim table through DIFFERENTLY-NAMED FK
        columns (``gl_account__type`` / ``account_no__type``) pair ONLY when the
        conform judge merged their roles — the bus-matrix referenced cell gives both
        roles one ``conformed_group``. This is how the DAT-756 false-negative closes
        now: not blindly by (dim, attr), but through the judge's cross-role verdict."""
        ids = _seed(
            real_session, duck, tb_dim_col="gl_account__type", jl_dim_col="account_no__type"
        )
        dim = ids["chart_of_accounts"]
        _seed_ref_cells(
            real_session,
            ids,
            {"trial_balance": ["gl_account"], "journal_lines": ["account_no"]},
            group=f"ref:{dim}:account_no|gl_account",
        )
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        assert row.slice_dimension == "chart_of_accounts.type"

    def test_differently_named_fks_without_conform_stay_separate(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-788 safe default (separate-until-conformed): the SAME differently-named
        FKs, with NO conform verdict, fall back to per-role structural identities and
        do NOT pair. Withholding cross-role conformance, never inventing it — the
        witness stays silent rather than aligning two roles the judge never merged."""
        ids = _seed(
            real_session, duck, tb_dim_col="gl_account__type", jl_dim_col="account_no__type"
        )
        assert _discover(real_session, duck, ids) == 0

    def test_folded_same_named_columns_do_not_pair(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """False-positive closes (DAT-756): two unrelated same-named FOLDED columns
        (own ``status`` column, no dim table -> null identity) are NOT paired. The
        bare name collision that would have fired a spurious witness now abstains —
        a folded dimension has no cross-table identity in Phase A (DAT-757)."""
        ids = _seed(real_session, duck, tb_dim_col="status", jl_dim_col="status", folded=True)
        assert _discover(real_session, duck, ids) == 0

    def test_role_playing_fks_are_separate_axes(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-788: role-playing FKs to one dim (``account_id__account_type`` +
        ``counter_account_id__account_type``) are SEPARATE identities, not one merged
        axis. Only the primary ``account_id`` role is shared with journal_lines, so
        the witness fires off it; the degenerate ``counter_account_id`` role is its own
        singleton identity (no partner) and correctly stays silent — it never gets
        collapsed onto the primary (which would corrupt the pairing) nor spuriously
        pairs on the shared (dim, attribute)."""
        ids = _seed(real_session, duck, tb_role_play_col="counter_account_id__account_type")
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        # The winning slice is the primary account_id role's (its column_id is the
        # balance column in this fixture), NEVER the degenerate counter role's.
        assert row.measure_slice_column_id == ids["trial_balance.balance"]
        assert row.measure_slice_column_id != ids["trial_balance.counter_account_id__account_type"]

    def test_winning_slice_column_persists_among_conformed_roles(
        self, real_session: Session, duck: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-778 × DAT-788: when the judge CONFORMS two FK roles into one identity,
        the fact's two role slices compete WITHIN it, and the persisted
        ``measure_slice_column_id`` is the WINNER's — never the first-enumerated. The
        primary ``account_id`` slice covers only 2 of 4 catalog values (support
        LCB(2,2) ≈ 0.34); the ``counter_account_id`` role labels all 4 truthfully
        (LCB(4,4) ≈ 0.51) and strictly wins on support. Only because the referenced
        cell conforms the two roles into ONE group do they land in the same identity
        and compete — the DAT-788 replacement for the old cross-role collapse."""
        ids = _seed(
            real_session,
            duck,
            tb_role_play_col="counter_account_id__account_type",
            role_play_wins=True,
            values=("assets", "liabilities", "equity", "revenue"),
        )
        dim = ids["chart_of_accounts"]
        group = f"ref:{dim}:account_id|counter_account_id"
        _seed_ref_cells(
            real_session,
            ids,
            {
                "trial_balance": ["account_id", "counter_account_id"],
                "journal_lines": ["account_id"],
            },
            group=group,
        )
        assert _discover(real_session, duck, ids) > 0
        row = _row_for(real_session, ids["trial_balance.balance"])
        assert row is not None
        assert row.pattern == "cumulative"
        assert row.event_table_id == ids["journal_lines"]
        rp_column_id = ids["trial_balance.counter_account_id__account_type"]
        assert row.measure_slice_column_id == rp_column_id
        assert row.measure_slice_column_id != ids["trial_balance.balance"]  # primary lost
        # The event fact carries a single slice; its physical column persists too.
        assert row.event_slice_column_id == ids["journal_lines.debit"]

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
