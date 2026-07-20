"""``days_in_period`` derivation against a real read surface + DuckDB (DAT-785).

Exercises the plumbing the unit guards can't reach: the extract snippet lookup, the
relation → fact resolution, the flow/stock + axis/cadence read off the Postgres
property graph (``og_columns.materialization`` — the vertical-neutral flow verdict;
``og_columns.anchor_time_axis``, DAT-780; ``current_temporal_column_profiles``,
DAT-783), and — the core of the fix — the LIVE window query in DuckDB over the
flow's grounded relation, filtered by the exact WHERE predicate the flow SUM applies.

The load-bearing cases: ``test_where_filtered_window_ignores_the_unfiltered_span``
proves the derived window is measured over the FILTERED rows, not the whole-column
``span_days`` the old code read (the reviewer's Critical); and
``test_derives_flow_window_on_a_non_finance_shape_with_no_statement`` proves the flow
is identified by its resolved ``materialization='flow'`` verdict with NO ``statement``
field — the vertical-neutrality guard the PR-#503 revert demanded. The rest pin the
happy path (a quarterly flow yields its fencepost-corrected window, NOT the config
30), the fencepost, and the fall-loud contract: every way the window can't be
observed — including a stock-only metric with no flow operand — keeps the config
default but flags it, never a silent 30.

Seeds one controlled, fully-promoted workspace (no pipeline, no LLM): Postgres for
the read surface (materialized read views + property graph, as the engine bootstrap
does) and a real ``lake.typed`` DuckDB table for the flow SUM's relation.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import duckdb
import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity
from dataraum.analysis.temporal.db_models import TemporalColumnProfile
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    ParameterDef,
    StepSource,
    StepType,
    TransformationGraph,
)
from dataraum.graphs.period_resolver import resolve_days_in_period
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.server.workspace import schema_name_for
from dataraum.storage import Column, Table
from dataraum.storage.property_graph import (
    drop_property_graph,
    materialize_property_graph,
)
from dataraum.storage.read_views import materialize_read_schema, read_schema_name_for
from dataraum.storage.snapshot_head import MetadataSnapshotHead

WS_ID = os.environ["DATARAUM_WORKSPACE_ID"]
SRC = "00000000-0000-0000-0000-000000000002"  # baseline Source seeded by the fixture
RUN = "00000000-0000-0000-0000-000000000001"
TS = datetime(2026, 1, 1, tzinfo=UTC)
# A quarterly corpus: four quarter-end postings spanning 273 days between endpoints.
# date_trunc('quarter') buckets them into 4 distinct quarters, so the fencepost
# correction is 273 × 4/3 = 364 — the number the hardcoded 30 must be replaced by.
_Q_DATES = [
    datetime(2025, 3, 31),
    datetime(2025, 6, 30),
    datetime(2025, 9, 30),
    datetime(2025, 12, 29),
]
MIN_TS = _Q_DATES[0].replace(tzinfo=UTC)
MAX_TS = _Q_DATES[-1].replace(tzinfo=UTC)
SPAN_DAYS = (MAX_TS - MIN_TS).total_seconds() / 86400  # 273.0
CORRECTED_DAYS = SPAN_DAYS * 4 / 3  # 364.0 — filtered span, fencepost-corrected


def _boot(engine: Engine) -> None:
    """Materialize the read views + property graph exactly as ConnectionManager does."""
    schema = schema_name_for(WS_ID)
    with engine.begin() as conn:
        drop_property_graph(conn, schema)
        materialize_read_schema(conn, schema)
        materialize_property_graph(conn, schema)


def _seed(
    session: Session,
    *,
    ground_flow: bool = True,
    declare_anchor: bool = True,
    profile_axis: bool = True,
    with_revenue: bool = False,
    grain: str = "quarter",
    profile_span: float = SPAN_DAYS,
    cogs_from: str = "income_stmt",
    cogs_expr: str = "SUM(cogs)",
    cogs_where: list[str] | None = None,
    cogs_temporal_behavior: str | None = "additive",
) -> str:
    """Seed the Postgres read surface for a COGS flow.

    ``ground_flow`` false leaves the flow extract without a snippet (ungrounded);
    ``declare_anchor`` false leaves the fact with no anchor time axis (the DAT-801
    null-anchor shape); ``profile_axis`` false leaves the axis column without a
    temporal profile; ``with_revenue`` adds a second flow (dso's revenue) on the same
    fact/axis. ``cogs_from`` / ``cogs_expr`` / ``cogs_where`` override the grounded
    snippet's parts (a ghost relation, a column-less aggregate, a malformed expr, or a
    WHERE predicate). ``cogs_temporal_behavior`` seeds the measure column's
    ``ColumnConcept`` so ``og_columns.materialization`` resolves — ``'additive'`` ⇒
    ``flow`` (the derivation's signal), ``'point_in_time'`` ⇒ ``stock`` (excluded),
    ``None`` leaves it unclassified (materialization NULL). Returns the fact
    ``table_id``.
    """
    fact = Table(
        table_id="t_is",
        source_id=SRC,
        table_name="income_stmt",
        layer="typed",
        duckdb_path="income_stmt",  # DAT-639: duckdb_path == table_name
    )
    session.add(fact)
    session.add_all(
        [
            Column(table_id="t_is", column_name="cogs", column_position=1),
            Column(table_id="t_is", column_name="posting_date", column_position=2),
        ]
    )
    session.flush()
    posting_col = (
        session.query(Column)
        .filter(Column.table_id == "t_is", Column.column_name == "posting_date")
        .one()
    )
    cogs_col = (
        session.query(Column).filter(Column.table_id == "t_is", Column.column_name == "cogs").one()
    )
    # Promote the fact's generation head (current_columns + column-grain temporal
    # profiles) and the catalog head (current_table_entities).
    session.add_all(
        [
            MetadataSnapshotHead(
                head_id="h_t_is",
                target="table:t_is",
                stage="generation",
                run_id=RUN,
                promoted_at=TS,
            ),
            MetadataSnapshotHead(
                head_id="h_cat", target="catalog", stage="catalog", run_id=RUN, promoted_at=TS
            ),
        ]
    )
    time_columns = (
        [
            {
                "column": "posting_date",
                "aspect": "posting",
                "role": "event",
                "is_anchor": True,
                "note": "",
            }
        ]
        if declare_anchor
        else []
    )
    session.add(
        TableEntity(
            table_id="t_is",
            run_id=RUN,
            detected_entity_type="entity",
            table_role="fact",
            time_columns=time_columns,
            detected_at=TS,
        )
    )
    if profile_axis:
        session.add(
            TemporalColumnProfile(
                profile_id="tp_posting",
                column_id=posting_col.column_id,
                run_id=RUN,
                profiled_at=TS,
                # min/max/span here are the WHOLE-COLUMN profile — deliberately NOT what
                # the resolver reads for the window (it measures the filtered rows live);
                # only detected_granularity is consumed, as the period bucket.
                min_timestamp=MIN_TS,
                max_timestamp=MAX_TS,
                span_days=profile_span,
                detected_granularity=grain,
                granularity_confidence=0.9,
                actual_periods=4,
                gaps=[],
            )
        )
    if cogs_temporal_behavior is not None:
        # Catalogue-grain ColumnConcept (resolves under h_cat) → og_columns.materialization:
        # 'additive' ⇒ 'flow' (the derivation's signal), 'point_in_time' ⇒ 'stock' (excluded).
        session.add(
            ColumnConcept(
                column_id=cogs_col.column_id,
                run_id=RUN,
                temporal_behavior=cogs_temporal_behavior,
                annotation_source="llm",
            )
        )
    if ground_flow:
        session.add(
            SQLSnippetRecord(
                workspace_id=WS_ID,
                schema_mapping_id=WS_ID,
                snippet_type="extract",
                standard_field="cost_of_goods_sold",
                statement="income_statement",
                aggregation="sum",
                sql=f"SELECT {cogs_expr} AS value FROM {cogs_from}",
                source="graph:dpo",
                parts={
                    "select": [{"expr": cogs_expr, "alias": "value"}],
                    "from": [cogs_from],
                    "where": cogs_where or [],
                },
            )
        )
    if with_revenue:
        # A second flow (dso's revenue) on the SAME fact, anchored to the SAME axis →
        # the two ccc flows observe one window and agree. Its measure carries the same
        # 'flow' materialization so the resolver identifies it as a flow.
        session.add(Column(table_id="t_is", column_name="revenue_amt", column_position=3))
        session.flush()
        revenue_col = (
            session.query(Column)
            .filter(Column.table_id == "t_is", Column.column_name == "revenue_amt")
            .one()
        )
        session.add(
            ColumnConcept(
                column_id=revenue_col.column_id,
                run_id=RUN,
                temporal_behavior="additive",
                annotation_source="llm",
            )
        )
        session.add(
            SQLSnippetRecord(
                workspace_id=WS_ID,
                schema_mapping_id=WS_ID,
                snippet_type="extract",
                standard_field="revenue",
                statement="income_statement",
                aggregation="sum",
                sql="SELECT SUM(revenue_amt) AS value FROM income_stmt",
                source="graph:ccc",
                parts={
                    "select": [{"expr": "SUM(revenue_amt)", "alias": "value"}],
                    "from": ["income_stmt"],
                    "where": [],
                },
            )
        )
    _add_passthrough_view(session, fact_table_id="t_is", view_name="income_stmt")
    session.commit()
    return "t_is"


def _add_passthrough_view(session: Session, *, fact_table_id: str, view_name: str) -> None:
    """Register a passthrough enriched view over a fact under the catalog head.

    Post-DAT-811 every fact grounds on its enriched view whose SERVED columns describe
    it, and DAT-812 reads measures + anchors off those — so the resolver never bounces
    to the base fact by name. This seeds a 1:1 passthrough view (``view_name`` IS the
    grounded relation) whose ``f.*`` served columns mirror the fact's columns, each
    carrying a ``source_column_id`` back to its typed source — the shape through which
    ``materialization``, the anchor axis, and its cadence all resolve. The view table is
    pure metadata (the live scan runs on the same DuckDB relation name); it surfaces in
    ``current_enriched_columns`` via the catalog head (``h_cat``, ``run_id == RUN``)."""
    view = Table(
        table_id=f"tev_{fact_table_id}",
        source_id=SRC,
        table_name=view_name,
        layer="enriched",
        duckdb_path=view_name,
    )
    session.add(view)
    session.flush()
    session.add(
        EnrichedView(
            fact_table_id=fact_table_id,
            view_table_id=view.table_id,
            view_name=view_name,
            run_id=RUN,
        )
    )
    fact_cols = session.query(Column).filter(Column.table_id == fact_table_id).all()
    for pos, col in enumerate(fact_cols):
        session.add(
            Column(
                table_id=view.table_id,
                column_name=col.column_name,
                column_position=pos,
                origin="fact",
                source_column_id=col.column_id,
            )
        )


def _seed_revenue_on_second_fact(session: Session) -> None:
    """Seed a SECOND income-statement fact carrying the ``revenue`` flow with its own
    (different-window) monthly anchor axis — the divergent-window case ccc must fall
    loud on."""
    session.add(
        Table(
            table_id="t_is2",
            source_id=SRC,
            table_name="income_stmt2",
            layer="typed",
            duckdb_path="income_stmt2",
        )
    )
    session.add_all(
        [
            Column(table_id="t_is2", column_name="revenue_amt", column_position=1),
            Column(table_id="t_is2", column_name="rev_date", column_position=2),
        ]
    )
    session.add(
        MetadataSnapshotHead(
            head_id="h_t_is2", target="table:t_is2", stage="generation", run_id=RUN, promoted_at=TS
        )
    )
    session.flush()
    rev_date_col = (
        session.query(Column)
        .filter(Column.table_id == "t_is2", Column.column_name == "rev_date")
        .one()
    )
    rev_amt_col = (
        session.query(Column)
        .filter(Column.table_id == "t_is2", Column.column_name == "revenue_amt")
        .one()
    )
    # revenue's measure resolves to a 'flow' materialization (its ColumnConcept
    # resolves under h_cat, seeded by the _seed() call this pairs with).
    session.add(
        ColumnConcept(
            column_id=rev_amt_col.column_id,
            run_id=RUN,
            temporal_behavior="additive",
            annotation_source="llm",
        )
    )
    session.add(
        TableEntity(
            table_id="t_is2",
            run_id=RUN,
            detected_entity_type="entity",
            table_role="fact",
            time_columns=[
                {
                    "column": "rev_date",
                    "aspect": "rev",
                    "role": "event",
                    "is_anchor": True,
                    "note": "",
                }
            ],
            detected_at=TS,
        )
    )
    session.add(
        TemporalColumnProfile(
            profile_id="tp_rev",
            column_id=rev_date_col.column_id,
            run_id=RUN,
            profiled_at=TS,
            min_timestamp=datetime(2025, 1, 15, tzinfo=UTC),
            max_timestamp=datetime(2025, 4, 15, tzinfo=UTC),
            span_days=90.0,
            detected_granularity="month",
            granularity_confidence=0.9,
            actual_periods=4,
            gaps=[],
        )
    )
    session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
            sql="SELECT SUM(revenue_amt) AS value FROM income_stmt2",
            source="graph:ccc",
            parts={
                "select": [{"expr": "SUM(revenue_amt)", "alias": "value"}],
                "from": ["income_stmt2"],
                "where": [],
            },
        )
    )
    _add_passthrough_view(session, fact_table_id="t_is2", view_name="income_stmt2")
    session.commit()


def _seed_non_finance(session: Session) -> str:
    """Seed a NON-FINANCE flow: units shipped per quarter, no ``statement`` anywhere.

    The vertical-neutrality regression guard's read surface — a manufacturing-shaped
    fact whose flow measure is identified PURELY by its resolved
    ``materialization='flow'`` verdict (a ColumnConcept ``temporal_behavior='additive'``),
    with no ``statement`` on the snippet or the graph. Returns the fact ``table_id``.
    """
    session.add(
        Table(
            table_id="t_ship",
            source_id=SRC,
            table_name="shipments",
            layer="typed",
            duckdb_path="shipments",
        )
    )
    session.add_all(
        [
            Column(table_id="t_ship", column_name="units", column_position=1),
            Column(table_id="t_ship", column_name="ship_date", column_position=2),
        ]
    )
    session.flush()
    ship_date_col = (
        session.query(Column)
        .filter(Column.table_id == "t_ship", Column.column_name == "ship_date")
        .one()
    )
    units_col = (
        session.query(Column)
        .filter(Column.table_id == "t_ship", Column.column_name == "units")
        .one()
    )
    session.add_all(
        [
            MetadataSnapshotHead(
                head_id="h_t_ship",
                target="table:t_ship",
                stage="generation",
                run_id=RUN,
                promoted_at=TS,
            ),
            MetadataSnapshotHead(
                head_id="h_cat", target="catalog", stage="catalog", run_id=RUN, promoted_at=TS
            ),
        ]
    )
    session.add(
        TableEntity(
            table_id="t_ship",
            run_id=RUN,
            detected_entity_type="entity",
            table_role="fact",
            time_columns=[
                {
                    "column": "ship_date",
                    "aspect": "shipment",
                    "role": "event",
                    "is_anchor": True,
                    "note": "",
                }
            ],
            detected_at=TS,
        )
    )
    session.add(
        TemporalColumnProfile(
            profile_id="tp_ship",
            column_id=ship_date_col.column_id,
            run_id=RUN,
            profiled_at=TS,
            min_timestamp=MIN_TS,
            max_timestamp=MAX_TS,
            span_days=SPAN_DAYS,
            detected_granularity="quarter",
            granularity_confidence=0.9,
            actual_periods=4,
            gaps=[],
        )
    )
    # The ONLY thing marking this measure as the flow: its resolved 'flow'
    # materialization (additive ⇒ flow). No statement, no accounting convention.
    session.add(
        ColumnConcept(
            column_id=units_col.column_id,
            run_id=RUN,
            temporal_behavior="additive",
            annotation_source="llm",
        )
    )
    session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="units_shipped",
            statement=None,  # no statement — the flow is identified by materialization
            aggregation="sum",
            sql="SELECT SUM(units) AS value FROM shipments",
            source="graph:throughput_days",
            parts={
                "select": [{"expr": "SUM(units)", "alias": "value"}],
                "from": ["shipments"],
                "where": [],
            },
        )
    )
    _add_passthrough_view(session, fact_table_id="t_ship", view_name="shipments")
    session.commit()
    return "t_ship"


def _seed_declared_dim_anchor_flow(session: Session) -> None:
    """The PRODUCTION header-dated shape (live finance `journal_lines`): the fact
    DECLARES its anchor as the served header-date name ``entry_id__date`` (a dimension
    served column, NOT a fact column), with NO reconciliation witness.

    This is what the live corpus actually does — ``mal.event_time_axis_column`` is the
    VIEW name ``entry_id__date``, which never resolves to a typed column, so
    ``event_time_axis_column_id`` is NULL and the id branch of the COALESCE is empty.
    The resolver's ``named`` match then lands the ``origin='dimension'`` served column
    and resolves its source (`journal_entries.date`) by identity. A ``named.origin =
    'fact'`` guard would wrongly exclude this path (VERIFIED: live journal_lines
    resolves exactly this way)."""
    session.add_all(
        [
            Table(
                table_id="t_jl",
                source_id=SRC,
                table_name="journal_lines",
                layer="typed",
                duckdb_path="journal_lines",
            ),
            Table(
                table_id="t_je",
                source_id=SRC,
                table_name="journal_entries",
                layer="typed",
                duckdb_path="journal_entries",
            ),
        ]
    )
    session.add_all(
        [
            Column(table_id="t_jl", column_name="amount", column_position=1),
            Column(table_id="t_je", column_name="date", column_position=1),
        ]
    )
    session.add_all(
        [
            MetadataSnapshotHead(
                head_id="h_t_jl",
                target="table:t_jl",
                stage="generation",
                run_id=RUN,
                promoted_at=TS,
            ),
            MetadataSnapshotHead(
                head_id="h_t_je",
                target="table:t_je",
                stage="generation",
                run_id=RUN,
                promoted_at=TS,
            ),
            MetadataSnapshotHead(
                head_id="h_cat", target="catalog", stage="catalog", run_id=RUN, promoted_at=TS
            ),
        ]
    )
    session.flush()
    amount_col = (
        session.query(Column)
        .filter(Column.table_id == "t_jl", Column.column_name == "amount")
        .one()
    )
    date_col = (
        session.query(Column).filter(Column.table_id == "t_je", Column.column_name == "date").one()
    )
    session.add(
        ColumnConcept(
            column_id=amount_col.column_id,
            run_id=RUN,
            temporal_behavior="additive",
            annotation_source="llm",
        )
    )
    # The fact DECLARES the header-date SERVED name as its event anchor (DAT-801 shape) —
    # og_columns' declared-anchor lateral surfaces `entry_id__date`, a dim served name.
    session.add(
        TableEntity(
            table_id="t_jl",
            run_id=RUN,
            detected_entity_type="event",
            table_role="fact",
            time_columns=[
                {
                    "column": "entry_id__date",
                    "aspect": "posting",
                    "role": "event",
                    "is_anchor": True,
                    "note": "",
                }
            ],
            detected_at=TS,
        )
    )
    session.add(
        TemporalColumnProfile(
            profile_id="tp_entry_date",
            column_id=date_col.column_id,
            run_id=RUN,
            profiled_at=TS,
            min_timestamp=MIN_TS,
            max_timestamp=MAX_TS,
            span_days=SPAN_DAYS,
            detected_granularity="quarter",
            granularity_confidence=0.9,
            actual_periods=4,
            gaps=[],
        )
    )
    view = Table(
        table_id="tev_t_jl",
        source_id=SRC,
        table_name="enriched_journal_lines",
        layer="enriched",
        duckdb_path="enriched_journal_lines",
    )
    session.add(view)
    session.flush()
    session.add(
        EnrichedView(
            fact_table_id="t_jl",
            view_table_id="tev_t_jl",
            view_name="enriched_journal_lines",
            run_id=RUN,
        )
    )
    session.add_all(
        [
            Column(
                table_id="tev_t_jl",
                column_name="amount",
                column_position=0,
                origin="fact",
                source_column_id=amount_col.column_id,
            ),
            Column(
                table_id="tev_t_jl",
                column_name="entry_id__date",
                column_position=1,
                origin="dimension",
                source_column_id=date_col.column_id,
            ),
        ]
    )
    session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="amount_flow",
            statement=None,
            aggregation="sum",
            sql="SELECT SUM(amount) AS value FROM enriched_journal_lines",
            source="graph:header_dpo",
            parts={
                "select": [{"expr": "SUM(amount)", "alias": "value"}],
                "from": ["enriched_journal_lines"],
                "where": [],
            },
        )
    )
    session.commit()


# --- DuckDB relation seeding (the flow SUM's real relation) -------------------


def _create_income_stmt(
    conn: duckdb.DuckDBPyConnection,
    *,
    account_type: bool = False,
    wide_noise: bool = False,
    revenue: bool = False,
) -> None:
    """Create ``lake.typed.income_stmt`` with the 4 quarter-end COGS rows.

    ``account_type`` adds the discriminator column the WHERE-filtered tests filter on;
    ``wide_noise`` adds non-COGS rows on a WIDER date range (excluded by the COGS
    filter, so the filtered window ≠ the whole-column span); ``revenue`` adds a
    ``revenue_amt`` flow column on the same rows/axis (ccc same-window).
    """
    cols = ["cogs DOUBLE", "posting_date TIMESTAMP"]
    if account_type:
        cols.append("account_type VARCHAR")
    if revenue:
        cols.append("revenue_amt DOUBLE")
    conn.execute(f"CREATE TABLE income_stmt ({', '.join(cols)})")

    def _row(cogs: float, dt: datetime, acct: str, rev: float) -> list[object]:
        vals: list[object] = [cogs, dt]
        if account_type:
            vals.append(acct)
        if revenue:
            vals.append(rev)
        return vals

    placeholders = ", ".join(["?"] * len(cols))
    for i, dt in enumerate(_Q_DATES):
        conn.execute(
            f"INSERT INTO income_stmt VALUES ({placeholders})",
            _row(100.0 * (i + 1), dt, "COGS", 200.0 * (i + 1)),
        )
    if wide_noise:
        # A non-COGS row a full year EARLIER — the filter excludes it, so the whole-
        # column span (2024→2025, ~724d) is far wider than the filtered COGS window.
        conn.execute(
            f"INSERT INTO income_stmt VALUES ({placeholders})",
            _row(0.0, datetime(2024, 1, 5), "REVENUE", 999.0),
        )


def _create_income_stmt2(conn: duckdb.DuckDBPyConnection) -> None:
    """Create ``lake.typed.income_stmt2`` with 4 monthly revenue rows (90-day window)."""
    conn.execute("CREATE TABLE income_stmt2 (revenue_amt DOUBLE, rev_date TIMESTAMP)")
    for i, dt in enumerate(
        [
            datetime(2025, 1, 15),
            datetime(2025, 2, 15),
            datetime(2025, 3, 15),
            datetime(2025, 4, 15),
        ]
    ):
        conn.execute("INSERT INTO income_stmt2 VALUES (?, ?)", [500.0 * (i + 1), dt])


def _create_shipments(conn: duckdb.DuckDBPyConnection) -> None:
    """Create ``lake.typed.shipments`` with the 4 quarter-end unit-shipment rows."""
    conn.execute("CREATE TABLE shipments (units DOUBLE, ship_date TIMESTAMP)")
    for i, dt in enumerate(_Q_DATES):
        conn.execute("INSERT INTO shipments VALUES (?, ?)", [10.0 * (i + 1), dt])


def _create_enriched_journal_lines(conn: duckdb.DuckDBPyConnection) -> None:
    """Create ``lake.typed.enriched_journal_lines`` — the header-dated view relation.

    The header date is served as ``entry_id__date`` (the FK-prefixed name), so the live
    window scan runs ``MIN/MAX("entry_id__date")`` over these 4 quarter-end rows."""
    conn.execute("CREATE TABLE enriched_journal_lines (amount DOUBLE, entry_id__date TIMESTAMP)")
    for i, dt in enumerate(_Q_DATES):
        conn.execute("INSERT INTO enriched_journal_lines VALUES (?, ?)", [100.0 * (i + 1), dt])


def _dpo_graph() -> TransformationGraph:
    """A dpo-shaped graph with a period parameter and one COGS extract.

    Whether that extract is the FLOW is decided by its measure's resolved
    ``materialization`` verdict (seeded via :func:`_seed`), not the ``statement`` field
    — ``statement`` here is only the snippet-lookup key that grounds the extract.
    """
    return TransformationGraph(
        graph_id="dpo",
        version="1",
        metadata=GraphMetadata(
            name="dpo", description="", category="working_capital", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="dpo", unit="days"),
        parameters=[ParameterDef(name="days_in_period", param_type="integer", default=30)],
        steps={
            "cost_of_goods_sold": GraphStep(
                step_id="cost_of_goods_sold",
                step_type=StepType.EXTRACT,
                source=StepSource(
                    standard_field="cost_of_goods_sold", statement="income_statement"
                ),
                aggregation="sum",
            ),
            "days_in_period": GraphStep(
                step_id="days_in_period",
                step_type=StepType.CONSTANT,
                parameter="days_in_period",
            ),
            "dpo": GraphStep(
                step_id="dpo",
                step_type=StepType.FORMULA,
                expression="(accounts_payable / cost_of_goods_sold) * days_in_period",
                depends_on=["cost_of_goods_sold", "days_in_period"],
                output_step=True,
            ),
        },
    )


def _ccc_graph() -> TransformationGraph:
    """A ccc-shaped graph: TWO income-statement flows (revenue + COGS) feeding ONE
    shared days_in_period constant — the case that must observe one agreed window."""
    return TransformationGraph(
        graph_id="cash_conversion_cycle",
        version="1",
        metadata=GraphMetadata(
            name="ccc", description="", category="working_capital", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="ccc", unit="days"),
        parameters=[ParameterDef(name="days_in_period", param_type="integer", default=30)],
        steps={
            "revenue": GraphStep(
                step_id="revenue",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field="revenue", statement="income_statement"),
                aggregation="sum",
            ),
            "cost_of_goods_sold": GraphStep(
                step_id="cost_of_goods_sold",
                step_type=StepType.EXTRACT,
                source=StepSource(
                    standard_field="cost_of_goods_sold", statement="income_statement"
                ),
                aggregation="sum",
            ),
            "days_in_period": GraphStep(
                step_id="days_in_period",
                step_type=StepType.CONSTANT,
                parameter="days_in_period",
            ),
            "ccc": GraphStep(
                step_id="ccc",
                step_type=StepType.FORMULA,
                expression="(revenue / cost_of_goods_sold) * days_in_period",
                depends_on=["revenue", "cost_of_goods_sold", "days_in_period"],
                output_step=True,
            ),
        },
    )


def _throughput_graph() -> TransformationGraph:
    """A NON-FINANCE metric graph: one flow extract with NO ``statement`` field, a
    period constant, and a formula scaling the flow by the derived window. The flow is
    identified purely by its resolved ``materialization='flow'`` verdict — the
    vertical-neutrality regression guard for the DAT-785 re-land."""
    return TransformationGraph(
        graph_id="throughput_days",
        version="1",
        metadata=GraphMetadata(
            name="throughput_days",
            description="",
            category="operations",
            source=GraphSource.SYSTEM,
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="throughput_days", unit="days"),
        parameters=[ParameterDef(name="days_in_period", param_type="integer", default=30)],
        steps={
            "units_shipped": GraphStep(
                step_id="units_shipped",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field="units_shipped"),  # NO statement field
                aggregation="sum",
            ),
            "days_in_period": GraphStep(
                step_id="days_in_period",
                step_type=StepType.CONSTANT,
                parameter="days_in_period",
            ),
            "throughput_days": GraphStep(
                step_id="throughput_days",
                step_type=StepType.FORMULA,
                expression="units_shipped * days_in_period",
                depends_on=["units_shipped", "days_in_period"],
                output_step=True,
            ),
        },
    )


def _header_flow_graph() -> TransformationGraph:
    """A metric with ONE header-dated flow scaled by the derived window (no statement)."""
    return TransformationGraph(
        graph_id="header_dpo",
        version="1",
        metadata=GraphMetadata(
            name="header_dpo", description="", category="working_capital", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="header_dpo", unit="days"),
        parameters=[ParameterDef(name="days_in_period", param_type="integer", default=30)],
        steps={
            "amount_flow": GraphStep(
                step_id="amount_flow",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field="amount_flow"),
                aggregation="sum",
            ),
            "days_in_period": GraphStep(
                step_id="days_in_period",
                step_type=StepType.CONSTANT,
                parameter="days_in_period",
            ),
            "out": GraphStep(
                step_id="out",
                step_type=StepType.FORMULA,
                expression="amount_flow * days_in_period",
                depends_on=["amount_flow", "days_in_period"],
                output_step=True,
            ),
        },
    )


@pytest.fixture
def pg_session(integration_engine: Engine) -> Session:
    factory = sessionmaker(bind=integration_engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


def test_derives_the_fencepost_corrected_flow_window_not_the_config_30(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The quarterly COGS flow's observed 273-day span, fencepost-corrected to 364
    (273 × 4/3), replaces the hardcoded 30."""
    _seed(pg_session)
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.derived is True
    assert resolution.flag is None
    assert resolution.days == pytest.approx(CORRECTED_DAYS)
    assert resolution.days != 30
    assert resolution.evidence["anchor_time_axis"] == ["posting_date"]
    assert resolution.evidence["filtered_span_days"] == pytest.approx(SPAN_DAYS)
    assert resolution.evidence["actual_periods"] == 4
    assert resolution.evidence["fencepost_factor"] == pytest.approx(4 / 3, abs=1e-4)


def test_where_filtered_window_ignores_the_unfiltered_span(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The Critical: a flow grounded as ``SUM(cogs) WHERE account_type = 'COGS'`` must
    measure its window over the FILTERED rows (273d → 364 corrected), NOT the whole-
    column span (a full year wider) the precomputed profile carries. Proves the
    filtered window ≠ the unfiltered window."""
    unfiltered_span = (MAX_TS - datetime(2024, 1, 5, tzinfo=UTC)).total_seconds() / 86400
    _seed(
        pg_session,
        profile_span=unfiltered_span,  # the whole-column span the OLD code would use
        cogs_where=["account_type = 'COGS'"],
    )
    _create_income_stmt(duckdb_conn, account_type=True, wide_noise=True)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.derived is True
    # The filtered COGS window (fencepost-corrected), NOT the ~724-day whole-column span.
    assert resolution.days == pytest.approx(CORRECTED_DAYS)
    assert resolution.days != pytest.approx(unfiltered_span)
    assert resolution.evidence["filtered_span_days"] == pytest.approx(SPAN_DAYS)


def test_no_flow_operand_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A metric whose only operand resolves to a STOCK materialization (point-in-time,
    no accumulation window) has no flow to observe — keeps 30, flagged. The resolved
    ``materialization`` verdict, not any statement field, decides flow vs stock: the
    stock is excluded and the metric falls loud."""
    _seed(pg_session, cogs_temporal_behavior="point_in_time")  # measure resolves to 'stock'
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert resolution.derived is False
    assert "no flow operand" in (resolution.flag or "")


def test_ungrounded_flow_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A flow whose extract never grounded (no snippet) keeps 30, flagged."""
    _seed(pg_session, ground_flow=False)
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "did not ground" in (resolution.flag or "")


def test_null_anchor_axis_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A flow fact with no anchor time axis (the DAT-801 header-date shape) abstains
    and flags — never a silent 30 derived off a missing axis."""
    _seed(pg_session, declare_anchor=False)
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "no observable anchor-axis span" in (resolution.flag or "")


def test_axis_without_temporal_profile_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """An anchor axis that was never temporally profiled has no cadence to observe."""
    _seed(pg_session, profile_axis=False)
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "no observable anchor-axis span" in (resolution.flag or "")


@pytest.mark.parametrize("cadence", ["irregular", "unknown"])
def test_no_clean_cadence_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    cadence: str,
) -> None:
    """An axis whose detected cadence is ``irregular``/``unknown`` (the two non-bucket
    sentinels) has no clean period to count or fencepost-correct against — abstain,
    never inject a bad grain into the window query."""
    _seed(pg_session, grain=cadence)
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "no clean period" in (resolution.flag or "")


def test_single_filtered_period_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A filtered window that collapses to a single posting date gives no inter-period
    gap to fencepost-correct against — abstain, never a fabricated span."""
    _seed(pg_session)
    duckdb_conn.execute("CREATE TABLE income_stmt (cogs DOUBLE, posting_date TIMESTAMP)")
    duckdb_conn.execute("INSERT INTO income_stmt VALUES (?, ?)", [100.0, _Q_DATES[0]])
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "single-period or degenerate" in (resolution.flag or "")


def test_empty_filtered_window_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A WHERE predicate that matches no row leaves an empty window — abstain."""
    _seed(pg_session, cogs_where=["account_type = 'NONEXISTENT'"])
    _create_income_stmt(duckdb_conn, account_type=True)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "filtered flow window is empty" in (resolution.flag or "")


def test_fact_outside_analysis_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A grounded relation that maps to no fact table keeps 30, flagged."""
    _seed(pg_session, cogs_from="ghost_relation")
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "outside the analysis" in (resolution.flag or "")


def test_aggregate_with_no_column_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A column-less aggregate (``COUNT(*)``) anchors on nothing — abstain."""
    _seed(pg_session, cogs_expr="COUNT(*)")
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "aggregates no column" in (resolution.flag or "")


def test_unparseable_select_expr_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A malformed grounded ``select_expr`` is surfaced loud, never mis-derived."""
    _seed(pg_session, cogs_expr="SUM(")
    _create_income_stmt(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "did not parse" in (resolution.flag or "")


def test_read_surface_failure_rolls_back_and_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """When the read views aren't materialized (og_columns missing), the axis read
    raises — the SAVEPOINT rolls back, the metric falls loud, and the outer session
    is NOT poisoned (a subsequent query still runs)."""
    _seed(pg_session)
    _create_income_stmt(duckdb_conn)
    # Drop the read surface so the axis read (`<read>.og_columns`) raises inside the
    # savepoint. A prior test's _boot leaves the read schema in place (pg_url_clean
    # truncates base tables, not the separate read schema), so drop it explicitly.
    schema = schema_name_for(WS_ID)
    with integration_engine.begin() as conn:
        drop_property_graph(conn, schema)
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{read_schema_name_for(schema)}" CASCADE'))
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_dpo_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert "read surface unavailable" in (resolution.flag or "")
    # The savepoint rollback left the outer session usable.
    assert pg_session.execute(text("SELECT 1")).scalar() == 1


def test_ccc_two_flows_sharing_a_window_derive_one_span(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """ccc's revenue + COGS flows on one fact/axis agree → one derived window, no flag."""
    _seed(pg_session, with_revenue=True)
    _create_income_stmt(duckdb_conn, revenue=True)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_ccc_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.derived is True
    assert resolution.flag is None
    assert resolution.days == pytest.approx(CORRECTED_DAYS)


def test_ccc_flows_disagreeing_on_window_fall_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """ccc's two flows observing DIFFERENT windows (COGS 273d quarterly on t_is,
    revenue 90d monthly on t_is2) cannot reconcile to one shared days_in_period →
    keep 30, flagged. The case a fact-keyed accumulator would silently collapse."""
    _seed(pg_session)  # COGS on t_is, 273-day quarterly window
    _seed_revenue_on_second_fact(pg_session)  # revenue on t_is2, 90-day monthly window
    _create_income_stmt(duckdb_conn)
    _create_income_stmt2(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_ccc_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.days == 30.0
    assert resolution.derived is False
    assert "disagree on the period window" in (resolution.flag or "")


def test_derives_flow_window_on_a_non_finance_shape_with_no_statement(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """VERTICAL-NEUTRALITY regression guard — the whole reason PR #503 was reverted.

    A non-finance metric (units shipped per quarter) whose flow is identified PURELY
    by its resolved ``materialization='flow'`` verdict, with NO ``statement`` field on
    the graph OR the grounded snippet, derives its quarterly window (273 →
    fencepost-corrected 364), NOT the config 30. Proves the derivation works with zero
    finance convention — the defect that reverted PR #503 (identifying the flow via the
    finance-only ``statement == 'income_statement'``) cannot recur."""
    _seed_non_finance(pg_session)
    _create_shipments(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_throughput_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.derived is True
    assert resolution.flag is None
    assert resolution.days == pytest.approx(CORRECTED_DAYS)
    assert resolution.days != 30
    assert resolution.evidence["anchor_time_axis"] == ["ship_date"]


def test_derives_header_dated_flow_via_declared_dim_anchor(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The LIVE finance path: `anchor_time_axis` is a DIM served name (`entry_id__date`)
    resolved via the `named` → dimension-served-column match, NOT the witness id.

    In the real corpus the witness's `event_time_axis_column` is a VIEW name that never
    resolves to a typed column, so `event_time_axis_column_id` is NULL and the id branch
    of the COALESCE is empty; the resolution rides the served column's own
    `source_column_id`. This is the exact shape a `named.origin='fact'` guard would have
    broken. Derives the quarterly window (364), not the config 30."""
    _seed_declared_dim_anchor_flow(pg_session)
    _create_enriched_journal_lines(duckdb_conn)
    _boot(integration_engine)
    resolution = resolve_days_in_period(
        pg_session, duckdb_conn, graph=_header_flow_graph(), workspace_id=WS_ID
    )
    assert resolution is not None
    assert resolution.derived is True
    assert resolution.flag is None
    assert resolution.days == pytest.approx(CORRECTED_DAYS)
    assert resolution.evidence["anchor_time_axis"] == ["entry_id__date"]
