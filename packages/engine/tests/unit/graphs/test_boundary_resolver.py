"""Point-in-time period binding — the substrate-independent pieces (DAT-887).

The full resolution reads ``og_columns`` + the DAT-730 calendar ladder off the
Postgres read surface and is exercised in
``tests/integration/graphs/test_boundary_resolver.py``. These cases pin what holds on
any substrate: the grain-aware fiscal-close arithmetic, the reviewer's probe set over a
real DuckDB relation (both stamping conventions, exactly-one-complete-FY under each,
data ending short, daily grain, multi-year, a declared non-calendar year), the fall-loud
reasons, the abstention contract, the recorded observable, and the short-circuits.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from dataraum.graphs import boundary_resolver
from dataraum.graphs.additivity_resolver import ServedRelation
from dataraum.graphs.boundary_resolver import (
    PeriodBinding,
    ReportingCalendar,
    _advance,
    _bind_to_close,
    _latest_close,
    _StockAxis,
    compose_period_binding,
    resolve_period_binding,
)
from dataraum.graphs.models import ExtractGroundingOutput, GraphProvenanceOutput
from dataraum.pipeline.phases.metrics_phase import _LOW_CONFIDENCE_FLOOR

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

_CALENDAR_YEAR = ReportingCalendar(fiscal_year_start_month=1, source="default")


def _balance_sheet(conn: duckdb.DuckDBPyConnection, periods: list[str]) -> str:
    """A minimal snapshot relation carrying exactly ``periods`` on its ``period`` axis."""
    conn.execute("CREATE OR REPLACE TABLE bs (period DATE, balance DOUBLE)")
    for i, period in enumerate(periods):
        conn.execute("INSERT INTO bs VALUES (?, ?)", [period, float(i)])
    return "bs"


# ---------------------------------------------------------------------------
# The fiscal-close arithmetic — grain-aware reachability
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("coverage_end", "start_month", "expected"),
    [
        # Coverage running past a close → that close applies.
        (datetime(2026, 3, 1), 1, datetime(2026, 1, 1)),
        # Coverage ending exactly ON a close → that close (the year IS complete).
        (datetime(2026, 1, 1), 1, datetime(2026, 1, 1)),
        # Coverage stopping short → the last close that actually happened.
        (datetime(2025, 7, 1), 1, datetime(2025, 1, 1)),
        # A non-January fiscal year: closes are April 1st.
        (datetime(2026, 3, 1), 4, datetime(2025, 4, 1)),
        (datetime(2026, 4, 1), 4, datetime(2026, 4, 1)),
    ],
)
def test_latest_close(coverage_end: datetime, start_month: int, expected: datetime | None) -> None:
    """The close is the latest fiscal boundary the data's COVERAGE reaches."""
    assert _latest_close(coverage_end, start_month) == expected


@pytest.mark.parametrize(
    ("grain", "expected"),
    [
        ("month", datetime(2026, 1, 1)),
        ("quarter", datetime(2026, 3, 1)),
        ("year", datetime(2026, 12, 1)),
        ("day", datetime(2025, 12, 2)),
        ("week", datetime(2025, 12, 8)),
    ],
)
def test_advance_one_period(grain: str, expected: datetime) -> None:
    """Calendar grains advance in months so month length can never drift the close."""
    assert _advance(datetime(2025, 12, 1), grain) == expected


def test_advance_rejects_a_grain_with_no_period_length() -> None:
    """An irregular/unknown cadence has no period to advance by — never a guess."""
    assert _advance(datetime(2025, 12, 1), "irregular") is None


# ---------------------------------------------------------------------------
# The probe set — binding under every stamping convention and coverage shape
# ---------------------------------------------------------------------------

_MONTHS_2025 = [f"2025-{m:02d}-01" for m in range(1, 13)]
_MONTH_ENDS_2025 = [
    "2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30",
    "2025-05-31", "2025-06-30", "2025-07-31", "2025-08-31",
    "2025-09-30", "2025-10-31", "2025-11-30", "2025-12-31",
]  # fmt: skip


def test_binds_december_not_the_trailing_period(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """THE DAT-887 defect: 14 start-stamped periods against a 12-month fiscal year.

    A label is the period's START, so the row labelled 2025-12-01 carries DECEMBER'S
    closing level — the level at the 2026-01-01 fiscal close. Binding 2026-01-01 would
    read JANUARY's close (the first falsification of this lane); binding MAX(period)
    would read February's.
    """
    relation = _balance_sheet(duckdb_conn, [*_MONTHS_2025, "2026-01-01", "2026-02-01"])

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 1)
    assert bound.as_of != datetime(2026, 1, 1)  # the January-level error
    assert bound.as_of != datetime(2026, 2, 1)  # the naked-MAX error
    assert bound.window_close == datetime(2026, 1, 1)


def test_binds_december_under_end_stamping(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """An end-stamped relation resolves the SAME fiscal instant — no convention needed."""
    relation = _balance_sheet(duckdb_conn, [*_MONTH_ENDS_2025, "2026-01-31"])

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 31)
    assert bound.window_close == datetime(2026, 1, 1)


def test_exactly_one_complete_fiscal_year_start_stamped(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Twelve start-stamped months ARE a complete fiscal year — it must not fall loud.

    max(period) is 2025-12-01, which alone looks short of the 2026-01-01 close. Coverage
    reaches 2026-01-01 because December's period ENDS there — the grain-aware step.
    """
    relation = _balance_sheet(duckdb_conn, _MONTHS_2025)

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 1)
    assert bound.window_close == datetime(2026, 1, 1)


def test_exactly_one_complete_fiscal_year_end_stamped(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The same completeness under end stamping."""
    relation = _balance_sheet(duckdb_conn, _MONTH_ENDS_2025)

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 31)


def test_daily_grain_binds_the_last_day_of_the_year(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Daily grain lands on the 31st, not a day late."""
    relation = _balance_sheet(
        duckdb_conn, ["2025-12-29", "2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02"]
    )

    bound = _bind_to_close(duckdb_conn, relation, "period", "day", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 31)


def test_multi_year_binds_the_latest_complete_year(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Three years of history bind the LATEST fiscal close the data reaches."""
    periods = [f"{y}-{m:02d}-01" for y in (2024, 2025) for m in range(1, 13)]
    relation = _balance_sheet(duckdb_conn, [*periods, "2026-01-01", "2026-02-01"])

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 1)


def test_declared_non_calendar_fiscal_year(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """A DECLARED April fiscal year binds MARCH — the period closing at the April 1 instant."""
    relation = _balance_sheet(duckdb_conn, [*_MONTHS_2025, "2026-01-01", "2026-02-01"])
    calendar = ReportingCalendar(fiscal_year_start_month=4, source="declared")

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", calendar)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 3, 1)
    assert bound.window_close == datetime(2025, 4, 1)
    assert bound.calendar_source == "declared"


# ---------------------------------------------------------------------------
# Absence falls loud
# ---------------------------------------------------------------------------


def test_yearly_end_stamped_falls_loud_rather_than_overshooting(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The reviewer's repro: yearly END-stamped through 2025-12-31 with a July FY start.

    One grain past the last label is 2026-12-31, which places the close at 2026-07-01 —
    but an end-stamped label's coverage truly ends 2026-01-01, six months earlier. The
    close was never reached. Silently binding it would be the ORIGINAL defect's class,
    so the end-stamping is detected and disclosed instead.
    """
    relation = _balance_sheet(duckdb_conn, ["2023-12-31", "2024-12-31", "2025-12-31"])
    calendar = ReportingCalendar(fiscal_year_start_month=7, source="declared")

    bound = _bind_to_close(duckdb_conn, relation, "period", "year", calendar)

    assert isinstance(bound, str)
    assert "END-stamped" in bound


def test_quarterly_end_stamped_falls_loud(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Quarter grain over-reaches by up to 2 day-1 instants under end stamping."""
    relation = _balance_sheet(duckdb_conn, ["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"])
    calendar = ReportingCalendar(fiscal_year_start_month=2, source="declared")

    bound = _bind_to_close(duckdb_conn, relation, "period", "quarter", calendar)

    assert isinstance(bound, str)
    assert "END-stamped" in bound


def test_coarse_grain_start_stamped_still_binds(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The check must not fire on START-stamped labels — day 1 is never a month's last.

    Quarter grain remains exact there, so the binding proceeds normally.
    """
    relation = _balance_sheet(duckdb_conn, ["2025-01-01", "2025-04-01", "2025-07-01", "2025-10-01"])

    bound = _bind_to_close(duckdb_conn, relation, "period", "quarter", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 10, 1)
    assert bound.window_close == datetime(2026, 1, 1)


def test_month_grain_end_stamped_is_unaffected(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """At month grain the over-reach interval holds NO day-1 instant, so no check fires.

    End stamping stays fully supported there — the restriction is exactly as wide as the
    arithmetic requires, no wider.
    """
    relation = _balance_sheet(duckdb_conn, _MONTH_ENDS_2025)

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 31)


def test_data_ending_short_of_the_close_falls_loud(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Jan–Jun cannot report a year-end level: the fiscal year never closed in it."""
    relation = _balance_sheet(duckdb_conn, _MONTHS_2025[:6])

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, str)
    assert "no period before the fiscal close" in bound


def test_data_before_any_usable_close_falls_loud(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Data lying entirely after the only close it reaches has no period to bind."""
    relation = _balance_sheet(duckdb_conn, ["2024-02-01", "2024-03-01"])

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, str)
    assert "no period before the fiscal close" in bound


def test_empty_axis_falls_loud(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """An all-NULL period axis carries no instant to bind to."""
    duckdb_conn.execute("CREATE OR REPLACE TABLE bs (period DATE, balance DOUBLE)")
    duckdb_conn.execute("INSERT INTO bs VALUES (NULL, 1.0)")

    bound = _bind_to_close(duckdb_conn, "bs", "period", "month", _CALENDAR_YEAR)

    assert isinstance(bound, str)
    assert "carries no periods" in bound


def test_unreadable_axis_falls_loud(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """A missing axis column degrades to a reason, never an exception into authoring."""
    relation = _balance_sheet(duckdb_conn, ["2025-01-01"])

    bound = _bind_to_close(duckdb_conn, relation, "period", "month", _CALENDAR_YEAR)
    assert isinstance(bound, PeriodBinding) or isinstance(bound, str)  # sanity: table exists

    missing = _bind_to_close(duckdb_conn, relation, "not_a_column", "month", _CALENDAR_YEAR)
    assert isinstance(missing, str)
    assert "not readable" in missing


def test_timestamptz_axis_binds_in_utc(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """A TZ-aware axis normalizes AT TIME ZONE 'UTC'.

    A bare ``::TIMESTAMP`` would convert using the SESSION time zone, so the persisted
    instant would drift with whichever TZ the worker ran in.
    """
    duckdb_conn.execute("SET TimeZone = 'America/New_York'")
    duckdb_conn.execute("CREATE OR REPLACE TABLE bs (period TIMESTAMPTZ, balance DOUBLE)")
    for period in ["2025-11-01", "2025-12-01", "2026-01-01"]:
        duckdb_conn.execute("INSERT INTO bs VALUES (?::TIMESTAMPTZ, 1.0)", [f"{period} 00:00:00Z"])

    bound = _bind_to_close(duckdb_conn, "bs", "period", "month", _CALENDAR_YEAR)
    duckdb_conn.execute("SET TimeZone = 'UTC'")

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 1)


def test_no_read_surface_makes_no_binding(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """On a non-Postgres bind there is no surface to resolve against — and no disclosure."""
    result = resolve_period_binding(
        session,
        duckdb_conn,
        relation="bs",
        select_expr="SUM(balance)",
        read_schema="ws_test_read",
        calendar=_CALENDAR_YEAR,
    )

    assert result is None


def test_no_relation_makes_no_binding(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A fall-loud grounding (no relation) has nothing to bind."""
    assert (
        resolve_period_binding(
            session,
            duckdb_conn,
            relation=None,
            select_expr="NULL",
            read_schema="ws_test_read",
            calendar=_CALENDAR_YEAR,
        )
        is None
    )


# ---------------------------------------------------------------------------
# The anchor-mismatch guard (DAT-893)
#
# DAT-893's writer fix makes this unreachable on a clean corpus: the anchor is now
# resolved from the measure's OWN table, and enriched views serve fact columns
# under their original names. It is kept — and tested by constructing the mismatch
# directly — because the anchor arrives from a read surface this module does not
# own. Anything that designates an anchor a served relation does not carry must be
# NAMED here, not reported as an absence.
# ---------------------------------------------------------------------------


def _resolve_with_stocks(
    monkeypatch: pytest.MonkeyPatch,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    stocks: list[_StockAxis],
    relation: str = "enriched_balance_sheet",
) -> PeriodBinding | str | None:
    """Drive ``_resolve`` past its read surface with a constructed stock-axis read.

    Only the two reads that need Postgres are replaced — the served-relation lookup and
    the per-view axis read. Everything after them is the real code path.
    """
    monkeypatch.setattr(
        boundary_resolver,
        "served_relation",
        lambda _session, _relation: ServedRelation(
            columns_table_id="view_1", fact_table_id="fact_1"
        ),
    )
    monkeypatch.setattr(
        boundary_resolver,
        "_read_stock_axes",
        lambda _session, _schema, _view_id, _cols: ({"stock"}, stocks),
    )
    return boundary_resolver._resolve(
        session,
        duckdb_conn,
        relation,
        "SUM(balance)",
        "ws_test_read",
        _CALENDAR_YEAR,
    )


def test_an_anchor_not_served_on_the_relation_names_the_mismatch(
    monkeypatch: pytest.MonkeyPatch, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """THE DAT-893 shape: a designated anchor belonging to a DIFFERENT relation.

    The live defect recorded the reconciliation evidence's axis (journal lines'
    ``entry_id__date``) as a balance-sheet stock's anchor. The reason must name the axis
    AND the relation — a reader who sees only "no anchor time axis" goes hunting the
    table's ``time_columns``, which are fine; the fault is at whoever recorded it.
    """
    reason = _resolve_with_stocks(
        monkeypatch,
        session,
        duckdb_conn,
        [_StockAxis(axis=None, grain=None, recorded="entry_id__date")],
    )

    assert isinstance(reason, str)
    assert "entry_id__date" in reason
    assert "enriched_balance_sheet" in reason
    # It must NOT degrade to the absence message — that is a different fact.
    assert "no anchor time axis" not in reason


def test_no_anchor_designated_at_all_still_reports_an_absence(
    monkeypatch: pytest.MonkeyPatch, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """The mirror: nothing was ever designated, so there is no mismatch to name.

    Pins the discrimination in both directions — the guard must not swallow the
    absence case and start inventing a relation mismatch that does not exist.
    """
    reason = _resolve_with_stocks(
        monkeypatch, session, duckdb_conn, [_StockAxis(axis=None, grain=None, recorded=None)]
    )

    assert reason == "point-in-time measure has no anchor time axis to bind a reporting instant on"


def test_one_measures_unservable_anchor_is_not_hidden_by_anothers_good_one(
    monkeypatch: pytest.MonkeyPatch, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A second stock's unservable anchor must not ride along on the first's.

    Binding the instant the resolvable axis yields would silently misstate the operand
    whose own axis this relation cannot carry.
    """
    reason = _resolve_with_stocks(
        monkeypatch,
        session,
        duckdb_conn,
        [
            _StockAxis(axis="period", grain="month", recorded="period"),
            _StockAxis(axis=None, grain=None, recorded="entry_id__date"),
        ],
    )

    assert isinstance(reason, str)
    assert "entry_id__date" in reason


# ---------------------------------------------------------------------------
# Composing the binding onto the grounding's parts
# ---------------------------------------------------------------------------


def _output(where: list[str] | None = None) -> ExtractGroundingOutput:
    return ExtractGroundingOutput(
        grounding="evidence",
        relation="enriched_balance_sheet",
        where=where or [],
        select_expr="SUM(balance)",
        description="d",
        provenance=GraphProvenanceOutput(column_mappings_basis=[]),
        assumptions=[],
    )


def _binding() -> PeriodBinding:
    return PeriodBinding(
        as_of=datetime(2025, 12, 1),
        window_close=datetime(2026, 1, 1),
        axis="period",
        relation="enriched_balance_sheet",
        fiscal_year_start_month=1,
        calendar_source="default",
    )


def test_compose_appends_the_binding_and_records_it(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """An unconstrained period axis gets the typed predicate AND the observable."""
    composed = compose_period_binding(
        _output(["account_type = 'payable'"]),
        ["account_type = 'payable'"],
        _binding(),
        {"period", "balance", "account_type"},
        duckdb_conn,
    )

    assert composed.where == [
        "account_type = 'payable'",
        "\"period\" = TIMESTAMP '2025-12-01 00:00:00'",
    ]
    assert composed.assumptions == []
    assert composed.abstain is None
    assert composed.record is not None
    assert composed.record["as_of"] == "2025-12-01 00:00:00"


def test_compose_defers_when_the_grounding_pins_the_axis_itself(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The model's own pin is never overridden — ANDing two pins yields an empty result.

    The deferral keeps FULL confidence: a defensible predicate is in force, it is simply
    not ours. Only the UNBOUND case drops below the gate's floor.
    """
    model_pin = "period = (SELECT MAX(period) FROM enriched_balance_sheet)"
    composed = compose_period_binding(
        _output([model_pin]), [model_pin], _binding(), {"period", "balance"}, duckdb_conn
    )

    assert composed.where == [model_pin]  # untouched — no deterministic override
    assert composed.record is None
    assert composed.abstain is None
    assert len(composed.assumptions) == 1
    assert composed.assumptions[0].dimension == "period.binding"
    assert composed.assumptions[0].confidence == 1.0
    assert "not applied" in composed.assumptions[0].assumption


def test_compose_abstains_on_an_unresolvable_known_stock(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """A known stock with no resolvable instant ABSTAINS — it must not compose unfiltered.

    The prompt told the model to leave the period axis to the system, so composing what
    it wrote would aggregate EVERY period — worse than the defect this ticket fixes.
    """
    reason = "no period before the fiscal close (2026-01-01) on axis 'period'"
    composed = compose_period_binding(_output(), [], reason, set(), duckdb_conn)

    assert composed.abstain == reason
    assert composed.record is None
    assert len(composed.assumptions) == 1
    assert composed.assumptions[0].dimension == "period.binding"
    assert "ABSTAINED" in composed.assumptions[0].assumption


def test_unbound_disclosure_falls_below_the_confidence_floor() -> None:
    """The gate that surfaces weak groundings must actually catch an unbound stock.

    metrics_phase takes the MIN confidence across assumptions and reports anything below
    its floor; a confident "this number is NOT bound to a reporting window" would sail
    straight through the only automated check.
    """
    composed = compose_period_binding(_output(), [], "unresolvable", set(), None)

    assert composed.assumptions[0].confidence < _LOW_CONFIDENCE_FLOOR


def test_compose_leaves_a_flow_untouched(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """A flow resolves to None — no predicate, no assumption, no record, no abstention."""
    composed = compose_period_binding(_output(), ["x = 1"], None, set(), duckdb_conn)

    assert composed.where == ["x = 1"]
    assert composed.assumptions == []
    assert composed.record is None
    assert composed.abstain is None


def test_binding_renders_a_typed_equality_predicate() -> None:
    """The composed WHERE part pins the axis to the resolved instant."""
    assert _binding().render() == "\"period\" = TIMESTAMP '2025-12-01 00:00:00'"


def test_binding_record_is_self_describing() -> None:
    """The persisted observable names the instant, the window, and the calendar's basis."""
    assert _binding().as_record() == {
        "as_of": "2025-12-01 00:00:00",
        "window_close": "2026-01-01 00:00:00",
        "axis": "period",
        "relation": "enriched_balance_sheet",
        "fiscal_year_start_month": 1,
        "calendar_source": "default",
    }
