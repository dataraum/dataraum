"""Point-in-time period binding against a real read surface + DuckDB (DAT-887).

Exercises the plumbing the unit guards cannot reach: the relation → served-view
resolution, the stock verdict + anchor axis read off the Postgres property graph
(``og_columns.materialization`` / ``anchor_time_axis``), the reporting-calendar read
off the DAT-730 ladder (``og_period_grain``, which resolves the singleton
``workspace_calendar``), and the live period reads in DuckDB that snap the fiscal
close to a period the relation actually carries.

The load-bearing case is ``test_binds_the_fiscal_close_not_the_trailing_period``: the
DAT-887 defect shape end to end — a balance sheet carrying 14 monthly periods against
a 12-month fiscal year must bind the close (2026-01-01), not ``MAX(period)``
(2026-02-01). ``test_flow_measure_is_left_unbound`` is its mirror: a flow must come
back untouched, because binding an accumulation to an instant would break every
DAT-785 window.

Seeds one controlled, fully-promoted workspace (no pipeline, no LLM): Postgres for the
read surface (materialized read views + property graph, as the engine bootstrap does)
and a real DuckDB relation for the live period reads.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import duckdb
import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity
from dataraum.analysis.temporal.db_models import TemporalColumnProfile
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.boundary_resolver import (
    PeriodBinding,
    compose_period_binding,
    read_reporting_calendar,
    resolve_period_binding,
)
from dataraum.graphs.formula_composer import compose_extract_sql, extract_parts_dict
from dataraum.graphs.models import (
    ExtractGroundingOutput,
    FailedSnippetProvenance,
    GraphProvenanceOutput,
    SnippetFailureMode,
)
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

# The DAT-887 corpus shape: a 12-month fiscal year (2025-01-01 + 12 months) whose
# balance sheet carries FOURTEEN monthly periods, running two past the fiscal close.
# MAX(period) is 2026-02-01; the close — and the only correct as-of — is 2026-01-01.
PERIODS = [f"2025-{m:02d}-01" for m in range(1, 13)] + ["2026-01-01", "2026-02-01"]
CLOSE = datetime(2026, 1, 1)  # the FY2025 close instant
YEAR_END = datetime(2025, 12, 1)  # the period whose CLOSE is that instant
TRAILING = datetime(2026, 2, 1)  # what MAX(period) would have picked


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
    temporal_behavior: str | None = "point_in_time",
    declare_anchor: bool = True,
    profile_axis: bool = True,
) -> None:
    """Seed the Postgres read surface for a balance-sheet stock measure.

    ``temporal_behavior`` seeds the measure's ``ColumnConcept`` so
    ``og_columns.materialization`` resolves — ``'point_in_time'`` ⇒ ``stock`` (the
    measure this module binds), ``'additive'`` ⇒ ``flow`` (left alone), ``None``
    leaves it unclassified. ``declare_anchor`` false leaves the fact with no anchor
    time axis, so a stock has no column to be bound on.
    """
    session.add(
        Table(
            table_id="t_bs",
            source_id=SRC,
            table_name="balance_sheet",
            layer="typed",
            duckdb_path="balance_sheet",
        )
    )
    session.add_all(
        [
            Column(table_id="t_bs", column_name="balance", column_position=1),
            Column(table_id="t_bs", column_name="period", column_position=2),
        ]
    )
    session.flush()
    balance_col = (
        session.query(Column)
        .filter(Column.table_id == "t_bs", Column.column_name == "balance")
        .one()
    )
    period_col = (
        session.query(Column)
        .filter(Column.table_id == "t_bs", Column.column_name == "period")
        .one()
    )
    session.add_all(
        [
            MetadataSnapshotHead(
                head_id="h_t_bs",
                target="table:t_bs",
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
            table_id="t_bs",
            run_id=RUN,
            detected_entity_type="entity",
            table_role="periodic_snapshot",
            time_columns=(
                [
                    {
                        "column": "period",
                        "aspect": "snapshot",
                        "role": "event",
                        "is_anchor": True,
                        "note": "",
                    }
                ]
                if declare_anchor
                else []
            ),
            detected_at=TS,
        )
    )
    if profile_axis:
        # The axis's own cadence: what one period of this relation SPANS, which is what
        # turns max(period) into the instant coverage actually reaches.
        session.add(
            TemporalColumnProfile(
                profile_id="tp_period",
                column_id=period_col.column_id,
                run_id=RUN,
                profiled_at=TS,
                min_timestamp=datetime(2025, 1, 1, tzinfo=UTC),
                max_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                span_days=396.0,
                detected_granularity="month",
                granularity_confidence=0.9,
                actual_periods=14,
                gaps=[],
            )
        )
    if temporal_behavior is not None:
        session.add(
            ColumnConcept(
                column_id=balance_col.column_id,
                run_id=RUN,
                temporal_behavior=temporal_behavior,
                annotation_source="llm",
            )
        )
    _add_passthrough_view(session, fact_table_id="t_bs", view_name="balance_sheet")
    session.commit()


def _add_passthrough_view(session: Session, *, fact_table_id: str, view_name: str) -> None:
    """Register a 1:1 passthrough enriched view over the fact (the DAT-811 shape).

    Every fact grounds on the enriched view whose SERVED columns describe it, and the
    materialization + anchor axis resolve off those — so the seeded view's ``f.*``
    columns each carry a ``source_column_id`` back to their typed source.
    """
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
    for pos, col in enumerate(session.query(Column).filter(Column.table_id == fact_table_id).all()):
        session.add(
            Column(
                table_id=view.table_id,
                column_name=col.column_name,
                column_position=pos,
                origin="fact",
                source_column_id=col.column_id,
            )
        )


def _declare_calendar(engine: Engine, start_month: int) -> None:
    """Declare a fiscal-year start for the workspace (the singleton the ladder reads)."""
    schema = schema_name_for(WS_ID)
    with engine.begin() as conn:
        conn.execute(
            text(
                f'INSERT INTO "{schema}".workspace_calendar '  # noqa: S608 - internal identifier
                f"(pin, fiscal_year_start_month, declared_at) VALUES (TRUE, :m, NOW())"
            ),
            {"m": start_month},
        )


def _create_balance_sheet(conn: duckdb.DuckDBPyConnection) -> None:
    """The live relation the period reads run against."""
    conn.execute("CREATE OR REPLACE TABLE balance_sheet (period DATE, balance DOUBLE)")
    for i, period in enumerate(PERIODS):
        conn.execute("INSERT INTO balance_sheet VALUES (?, ?)", [period, 100.0 + i])


def _resolve(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> PeriodBinding | str | None:
    read_schema = read_schema_name_for(schema_name_for(WS_ID))
    return resolve_period_binding(
        session,
        duckdb_conn,
        relation="balance_sheet",
        select_expr="SUM(balance)",
        read_schema=read_schema,
        calendar=read_reporting_calendar(session, read_schema),
    )


def _grounding_output() -> ExtractGroundingOutput:
    """A grounding that left the period axis to the system (prompt branch (a))."""
    return ExtractGroundingOutput(
        grounding="evidence",
        relation="balance_sheet",
        where=[],
        select_expr="SUM(balance)",
        description="d",
        provenance=GraphProvenanceOutput(column_mappings_basis=[]),
        assumptions=[],
    )


@pytest.fixture
def pg_session(integration_engine: Engine) -> Session:
    factory = sessionmaker(bind=integration_engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


def test_binds_the_fiscal_close_not_the_trailing_period(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """THE DAT-887 defect, end to end on a real read surface.

    14 monthly periods against a 12-month fiscal year. A label is a period START, so the
    row carrying the FY2025 year-end level is 2025-12-01 — December's close. Binding the
    close INSTANT (2026-01-01) reads January's level; binding MAX(period) reads
    February's. Both wrong numbers are pinned here so neither can come back.
    """
    _seed(pg_session)
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == YEAR_END
    assert bound.as_of != CLOSE  # the January-level error
    assert bound.as_of != TRAILING  # the naked-MAX error
    assert bound.window_close == CLOSE
    assert bound.axis == "period"
    assert bound.relation == "balance_sheet"


def test_undeclared_calendar_binds_the_stamped_default_visibly(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """With no declaration the ladder stamps January — and SAYS it was defaulted.

    The recorded observable must carry that basis: a consumer grading the value has
    to be able to tell an assumed calendar from a declared one.
    """
    _seed(pg_session)
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)

    assert isinstance(bound, PeriodBinding)
    assert bound.fiscal_year_start_month == 1
    assert bound.calendar_source == "default"
    assert bound.as_record()["calendar_source"] == "default"


def test_declared_fiscal_year_moves_the_bound_instant(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A DECLARED April fiscal year binds MARCH — the calendar is load-bearing.

    Proves the binding actually reads ``workspace_calendar`` through the ladder rather
    than hardcoding a calendar year that happens to match this corpus.
    """
    _seed(pg_session)
    _create_balance_sheet(duckdb_conn)
    _declare_calendar(integration_engine, 4)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 3, 1)  # the period closing at the April 1 instant
    assert bound.window_close == datetime(2025, 4, 1)
    assert bound.fiscal_year_start_month == 4
    assert bound.calendar_source == "declared"


def test_reporting_calendar_reads_the_ladder(
    integration_engine: Engine, pg_session: Session
) -> None:
    """The calendar read resolves the singleton through ``og_period_grain``."""
    _seed(pg_session)
    _boot(integration_engine)

    calendar = read_reporting_calendar(pg_session, read_schema_name_for(schema_name_for(WS_ID)))

    assert calendar is not None
    assert calendar.fiscal_year_start_month == 1
    assert calendar.source == "default"


def test_flow_measure_is_left_unbound(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A flow accumulates across a window — binding it to an instant would break it.

    The mirror of the defect: ``None``, not a reason, because nothing failed.
    """
    _seed(pg_session, temporal_behavior="additive")
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    assert _resolve(pg_session, duckdb_conn) is None


def test_unclassified_measure_is_left_unbound(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """No stock verdict ⇒ not knowably point-in-time ⇒ nothing to bind and nothing to say."""
    _seed(pg_session, temporal_behavior=None)
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    assert _resolve(pg_session, duckdb_conn) is None


def test_stock_without_an_anchor_axis_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A stock with no axis to bind on DISCLOSES — it must not silently pass unbound."""
    _seed(pg_session, declare_anchor=False)
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)

    assert isinstance(bound, str)
    assert "no anchor time axis" in bound


def test_the_observable_reaches_the_read_surface(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """THE WIRE (DAT-887's acceptance criterion): binding -> parts -> current_groundings.

    Every link between the resolver and the grading surface was previously green with the
    wire cut — nothing asserted that the record reaches ``parts``, and the parts-shape
    test pinned its ABSENCE. This walks the whole path: resolve, compose the parts the
    way the agent does, persist the snippet, then read the period back off
    ``current_groundings.resolved_period`` as the eval does. Dropping the
    ``period_binding`` argument in ``extract_parts_dict`` must fail this.
    """
    _seed(pg_session)
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)
    assert isinstance(bound, PeriodBinding)

    composed = compose_period_binding(
        _grounding_output(), [], bound, {"period", "balance"}, duckdb_conn
    )
    assert composed.record is not None
    pg_session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="accounts_payable",
            statement="balance_sheet",
            aggregation="sum",
            sql=compose_extract_sql("SUM(balance)", "balance_sheet", composed.where),
            source="graph:dpo",
            parts=extract_parts_dict(
                "SUM(balance)", "balance_sheet", composed.where, composed.record
            ),
        )
    )
    pg_session.commit()

    row = pg_session.execute(
        text(  # noqa: S608 - internal identifier
            f"SELECT resolved_period, reporting_window_close, calendar_source"
            f' FROM "{read_schema_name_for(schema_name_for(WS_ID))}".current_groundings'
            f" WHERE concept = 'accounts_payable'"
        )
    ).one()

    assert row[0] == "2025-12-01 00:00:00"  # the year-end level, readable as a column
    assert row[1] == "2026-01-01 00:00:00"
    assert row[2] == "default"


def test_a_flow_records_no_period_on_the_read_surface(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A flow carries no instant, so the observable is NULL — not an empty string."""
    _seed(pg_session, temporal_behavior="additive")
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    pg_session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="revenue",
            statement="balance_sheet",
            aggregation="sum",
            sql="SELECT SUM(balance) AS value FROM balance_sheet",
            source="graph:dso",
            parts=extract_parts_dict("SUM(balance)", "balance_sheet", [], None),
        )
    )
    pg_session.commit()

    row = pg_session.execute(
        text(  # noqa: S608 - internal identifier
            f"SELECT resolved_period"
            f' FROM "{read_schema_name_for(schema_name_for(WS_ID))}".current_groundings'
            f" WHERE concept = 'revenue'"
        )
    ).one()

    assert row[0] is None


def test_unprofiled_axis_falls_loud(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Without a cadence the close-reachability step cannot run — so it discloses.

    Guessing a period length here would silently decide whether a fiscal year counts as
    complete, which is the whole question this step exists to answer.
    """
    _seed(pg_session, profile_axis=False)
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)

    assert isinstance(bound, str)
    assert "no temporal profile" in bound


def test_stock_on_an_unserved_relation_abstains(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A served stock grounded on a NON-enriched relation must abstain, not pass silently.

    The per-view verdict read cannot run without a served enriched view, so this branch
    used to return None — meaning "keep the model's own pin". But on prompt branch (a)
    the model authored NO pin, so that composed a predicate-free stock extract. Low
    reachability (a completed pipeline serves enriched views), but the acceptance bar
    admits no reachable branch at all.
    """
    _seed(pg_session)
    _create_balance_sheet(duckdb_conn)
    duckdb_conn.execute("CREATE OR REPLACE TABLE raw_bs AS SELECT * FROM balance_sheet")
    _boot(integration_engine)

    read_schema = read_schema_name_for(schema_name_for(WS_ID))
    bound = resolve_period_binding(
        pg_session,
        duckdb_conn,
        relation="raw_bs",  # a real DuckDB relation that is NOT a served enriched view
        select_expr="SUM(balance)",
        read_schema=read_schema,
        calendar=read_reporting_calendar(pg_session, read_schema),
    )

    assert isinstance(bound, str)
    assert "not a served enriched view" in bound


def test_flow_on_an_unserved_relation_is_still_left_alone(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The name-keyed fallback is conservative: a FLOW must never be abstained by it."""
    _seed(pg_session, temporal_behavior="additive")
    _create_balance_sheet(duckdb_conn)
    duckdb_conn.execute("CREATE OR REPLACE TABLE raw_bs AS SELECT * FROM balance_sheet")
    _boot(integration_engine)

    read_schema = read_schema_name_for(schema_name_for(WS_ID))
    assert (
        resolve_period_binding(
            pg_session,
            duckdb_conn,
            relation="raw_bs",
            select_expr="SUM(balance)",
            read_schema=read_schema,
            calendar=read_reporting_calendar(pg_session, read_schema),
        )
        is None
    )


# ---------------------------------------------------------------------------
# DAT-893: the anchor a reconciliation witness designates
# ---------------------------------------------------------------------------


def _seed_lineage_witness(session: Session, *, measure_axis: str, event_axis: str) -> None:
    """Seed a DAT-778 reconciliation witness for the balance-sheet stock measure.

    The live corpus shape: the ending balance reconciles against the journal-lines
    detail, so the witness spans TWO tables and its two axis fields name columns on two
    DIFFERENT relations — which is the only shape the processor can produce (it skips
    self-pairs and requires a strictly finer event side). ``pattern='cumulative'`` keeps
    the witness posterior agreeing with the concept prior that this is a stock; a
    ``per_period`` witness would outrank it and make the measure a flow.
    """
    session.add(
        Table(
            table_id="t_jl",
            source_id=SRC,
            table_name="journal_lines",
            layer="typed",
            duckdb_path="journal_lines",
        )
    )
    session.add_all(
        [
            Column(table_id="t_jl", column_name="entry_id__date", column_position=1),
            Column(table_id="t_jl", column_name="account_id", column_position=2),
        ]
    )
    session.flush()
    balance_col = (
        session.query(Column)
        .filter(Column.table_id == "t_bs", Column.column_name == "balance")
        .one()
    )
    period_col = (
        session.query(Column)
        .filter(Column.table_id == "t_bs", Column.column_name == "period")
        .one()
    )
    event_key = (
        session.query(Column)
        .filter(Column.table_id == "t_jl", Column.column_name == "account_id")
        .one()
    )
    session.add(
        MeasureAggregationLineage(
            lineage_id="mal_bs",
            run_id=RUN,
            measure_table_id="t_bs",
            measure_column_id=balance_col.column_id,
            event_table_id="t_jl",
            measure_time_axis_column=measure_axis,
            event_time_axis_column=event_axis,
            measure_slice_column_id=period_col.column_id,
            event_slice_column_id=event_key.column_id,
            slice_dimension="account",
            convention_sql="SUM(amount)",
            period_grain="month",
            pattern="cumulative",
            match_rate=1.0,
            r_flow_median=0.9,
            r_stock_median=0.05,
            n_entities=10,
            n_entities_fired=10,
            sign_fired_primary=10,
            sign_fired_mirror=0,
            sign_fired_both=0,
            created_at=TS,
        )
    )
    session.commit()


def test_a_reconciled_stock_binds_on_its_own_relations_axis(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """THE DAT-893 defect, end to end: a reconciled stock must still bind.

    Undeclared calendar (the stamped January default) + a snapshot relation + a stock
    measure whose witness reconciled against the journal-lines detail. Before the fix the
    anchor resolved the witness's EVENT-side axis — ``entry_id__date``, a column of the
    evidence relation that ``balance_sheet`` does not serve — so the binder could place
    no boundary, abstained, and the extract composed ``SELECT NULL AS value``, taking
    DSO/DPO/DIO/CCC/current_ratio with it.

    The witness must still WIN (it outranks the declaration, DAT-780); what changes is
    which of its two axes it contributes. Here both point at ``period`` in the end, so
    the assertion that discriminates is that a binding exists AT ALL.
    """
    _seed(pg_session)
    _seed_lineage_witness(pg_session, measure_axis="period", event_axis="entry_id__date")
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)

    assert isinstance(bound, PeriodBinding), bound
    assert bound.axis == "period"
    assert bound.as_of == YEAR_END
    assert bound.calendar_source == "default"


def test_the_reconciled_stock_extract_computes_a_real_value(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The observable behind the fix: a NUMBER, not ``SELECT NULL AS value``.

    Composes the binding the way the agent does and runs the result against the live
    relation. The seeded balances are ``100 + index``, so the FY2025 year-end row
    (2025-12-01, index 11) is 111.0 — pinned, so a binding that lands on the wrong
    period cannot pass either.
    """
    _seed(pg_session)
    _seed_lineage_witness(pg_session, measure_axis="period", event_axis="entry_id__date")
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    bound = _resolve(pg_session, duckdb_conn)
    assert isinstance(bound, PeriodBinding), bound
    composed = compose_period_binding(
        _grounding_output(), [], bound, {"period", "balance"}, duckdb_conn
    )
    assert composed.abstain is None

    sql = compose_extract_sql("SUM(balance)", "balance_sheet", composed.where)
    value = duckdb_conn.execute(sql).fetchone()

    assert value is not None
    assert value[0] == 111.0


def test_an_anchor_from_another_relation_persists_the_named_reason(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The constructed mismatch, all the way to the read surface.

    DAT-893's writer fix makes this unreachable through the witness on a clean corpus, so
    the mismatch is constructed directly: an anchor naming a column the served relation
    does not carry. Two things must hold — the binder NAMES the axis and the relation
    rather than reporting an absence, and that reason survives onto the persisted
    failure, where ``failure_reason`` alone would say only "no support".
    """
    _seed(pg_session)
    # The anchor now names an axis of the EVIDENCE relation — exactly what the old view
    # expression served for balance_sheet.ending_balance.
    _seed_lineage_witness(pg_session, measure_axis="entry_id__date", event_axis="entry_id__date")
    _create_balance_sheet(duckdb_conn)
    _boot(integration_engine)

    reason = _resolve(pg_session, duckdb_conn)

    assert isinstance(reason, str)
    assert "entry_id__date" in reason
    assert "balance_sheet" in reason
    assert "no anchor time axis" not in reason

    composed = compose_period_binding(
        _grounding_output(), [], reason, {"period", "balance"}, duckdb_conn
    )
    assert composed.abstain == reason

    pg_session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="accounts_payable",
            statement="balance_sheet",
            aggregation="sum",
            sql="SELECT NULL AS value",
            source="graph:dpo",
            failure_count=1,
            provenance=FailedSnippetProvenance(
                failure_mode=SnippetFailureMode.VERIFIER_REJECTED,
                failure_reason="no support: aggregation returned NULL",
                composition_abstain=composed.abstain,
            ).model_dump(mode="json"),
            parts=extract_parts_dict("NULL", None, [], None),
        )
    )
    pg_session.commit()

    row = pg_session.execute(
        text(  # noqa: S608 - internal identifier
            f"SELECT failed, provenance"
            f' FROM "{read_schema_name_for(schema_name_for(WS_ID))}".current_groundings'
            f" WHERE concept = 'accounts_payable'"
        )
    ).one()

    assert row[0] is True
    # The verifier's generic text alone would send a reader hunting the SQL; the cause
    # rides beside it, unchanged, on the same row.
    assert row[1]["failure_reason"] == "no support: aggregation returned NULL"
    assert row[1]["composition_abstain"] == reason
