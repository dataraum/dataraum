"""Point-in-time period binding — the substrate-independent pieces (DAT-887).

The full resolution reads ``og_columns`` + the DAT-730 calendar ladder off the
Postgres read surface and is exercised in
``tests/integration/graphs/test_boundary_resolver.py``. These cases pin what holds on
any substrate: the fiscal-close arithmetic, the snap-to-present-period behaviour over a
real DuckDB relation (including the exact DAT-887 defect shape), the fall-loud reasons,
the recorded observable's shape, and the no-read-surface short-circuit.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from dataraum.graphs.boundary_resolver import (
    PeriodBinding,
    ReportingCalendar,
    _bind_to_close,
    _latest_close,
    resolve_period_binding,
)

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
# The fiscal-close arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("max_period", "start_month", "expected"),
    [
        # Data runs PAST this year's close → that close applies (the DAT-887 shape).
        (datetime(2026, 2, 1), 1, datetime(2026, 1, 1)),
        # Data stops exactly ON the close → the close itself, not the prior year's.
        (datetime(2026, 1, 1), 1, datetime(2026, 1, 1)),
        # Data stops BEFORE this year's close → the last one that actually happened.
        (datetime(2025, 12, 1), 1, datetime(2025, 1, 1)),
        # A non-January fiscal year: closes are April 1st.
        (datetime(2026, 2, 1), 4, datetime(2025, 4, 1)),
        (datetime(2026, 4, 1), 4, datetime(2026, 4, 1)),
        (datetime(2026, 5, 15), 4, datetime(2026, 4, 1)),
        # A December fiscal-year start.
        (datetime(2026, 2, 1), 12, datetime(2025, 12, 1)),
    ],
)
def test_latest_close(max_period: datetime, start_month: int, expected: datetime) -> None:
    """The close is the latest fiscal-year boundary at or before the data's max."""
    assert _latest_close(max_period, start_month) == expected


# ---------------------------------------------------------------------------
# Snapping to a period the relation actually carries
# ---------------------------------------------------------------------------


def test_binds_fiscal_close_not_trailing_period(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """THE DAT-887 defect: 14 monthly periods against a 12-month fiscal year.

    ``MAX(period)`` is 2026-02-01, two periods past the fiscal-year end. The binding
    must resolve 2026-01-01 — the close — never the tail the data happens to reach.
    """
    periods = [f"2025-{m:02d}-01" for m in range(1, 13)] + ["2026-01-01", "2026-02-01"]
    relation = _balance_sheet(duckdb_conn, periods)

    bound = _bind_to_close(duckdb_conn, relation, "period", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2026, 1, 1)
    assert bound.window_close == datetime(2026, 1, 1)
    assert bound.axis == "period"


def test_binding_is_stamping_convention_free(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Period-END stamping resolves the SAME fiscal instant as period-START stamping.

    Resolving the close first and snapping to a present period afterwards is what
    makes this hold — a rule keyed on the period's own label could not.
    """
    periods = [
        "2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30",
        "2025-05-31", "2025-06-30", "2025-07-31", "2025-08-31",
        "2025-09-30", "2025-10-31", "2025-11-30", "2025-12-31",
        "2026-01-31",
    ]  # fmt: skip
    relation = _balance_sheet(duckdb_conn, periods)

    bound = _bind_to_close(duckdb_conn, relation, "period", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    # The FY2025 close is the instant 2026-01-01; the latest period at or before it
    # is the 2025-12-31 balance — the year-end level, not the January tail.
    assert bound.as_of == datetime(2025, 12, 31)
    assert bound.window_close == datetime(2026, 1, 1)


def test_binds_declared_non_calendar_fiscal_year(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """A DECLARED April fiscal year binds the April close, not the calendar one."""
    periods = [f"2025-{m:02d}-01" for m in range(1, 13)] + ["2026-01-01", "2026-02-01"]
    relation = _balance_sheet(duckdb_conn, periods)
    calendar = ReportingCalendar(fiscal_year_start_month=4, source="declared")

    bound = _bind_to_close(duckdb_conn, relation, "period", calendar)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 4, 1)
    assert bound.calendar_source == "declared"
    assert bound.fiscal_year_start_month == 4


def test_snaps_back_when_close_itself_is_absent(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """A gap at the close binds the latest period BEFORE it — never one after."""
    relation = _balance_sheet(
        duckdb_conn, ["2025-10-01", "2025-11-01", "2025-12-01", "2026-02-01"]
    )

    bound = _bind_to_close(duckdb_conn, relation, "period", _CALENDAR_YEAR)

    assert isinstance(bound, PeriodBinding)
    assert bound.as_of == datetime(2025, 12, 1)


# ---------------------------------------------------------------------------
# Absence falls loud
# ---------------------------------------------------------------------------


def test_no_period_at_or_before_close_falls_loud(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Data starting after the last close cannot be bound — a reason, never a guess."""
    relation = _balance_sheet(duckdb_conn, ["2026-01-15", "2026-02-01"])

    bound = _bind_to_close(duckdb_conn, relation, "period", _CALENDAR_YEAR)

    assert isinstance(bound, str)
    assert "no period at or before the last fiscal close" in bound


def test_empty_axis_falls_loud(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """An all-NULL period axis carries no instant to bind to."""
    duckdb_conn.execute("CREATE OR REPLACE TABLE bs (period DATE, balance DOUBLE)")
    duckdb_conn.execute("INSERT INTO bs VALUES (NULL, 1.0)")

    bound = _bind_to_close(duckdb_conn, "bs", "period", _CALENDAR_YEAR)

    assert isinstance(bound, str)
    assert "carries no periods" in bound


def test_unreadable_axis_falls_loud(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """A missing axis column degrades to a reason, never an exception into authoring."""
    relation = _balance_sheet(duckdb_conn, ["2025-01-01"])

    bound = _bind_to_close(duckdb_conn, relation, "not_a_column", _CALENDAR_YEAR)

    assert isinstance(bound, str)
    assert "not readable" in bound


def test_no_read_surface_makes_no_binding(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """On a non-Postgres bind there is no surface to resolve against — and no disclosure.

    Nothing failed; there was simply nothing to resolve. A reason here would flag every
    extract on the test substrate as an unresolved window.
    """
    result = resolve_period_binding(
        session,
        duckdb_conn,
        relation="bs",
        select_expr="SUM(balance)",
        read_schema="ws_test_read",
    )

    assert result is None


def test_no_relation_makes_no_binding(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A fall-loud grounding (no relation) has nothing to bind."""
    assert (
        resolve_period_binding(
            session, duckdb_conn, relation=None, select_expr="NULL", read_schema="ws_test_read"
        )
        is None
    )


# ---------------------------------------------------------------------------
# The recorded observable (the ticket's acceptance criterion)
# ---------------------------------------------------------------------------


def test_binding_renders_a_typed_equality_predicate() -> None:
    """The composed WHERE part pins the axis to the resolved instant."""
    binding = PeriodBinding(
        as_of=datetime(2026, 1, 1),
        window_close=datetime(2026, 1, 1),
        axis="period",
        relation="enriched_balance_sheet",
        fiscal_year_start_month=1,
        calendar_source="default",
    )

    assert binding.render() == "\"period\" = TIMESTAMP '2026-01-01 00:00:00'"


def test_binding_record_is_self_describing() -> None:
    """The persisted observable names the instant, the window, and the calendar's basis.

    A consumer (the eval attributing error) must be able to read which period the value
    is for AND whether the fiscal calendar behind it was declared or assumed.
    """
    binding = PeriodBinding(
        as_of=datetime(2025, 12, 31),
        window_close=datetime(2026, 1, 1),
        axis="period",
        relation="enriched_balance_sheet",
        fiscal_year_start_month=1,
        calendar_source="default",
    )

    assert binding.as_record() == {
        "as_of": "2025-12-31 00:00:00",
        "window_close": "2026-01-01 00:00:00",
        "axis": "period",
        "relation": "enriched_balance_sheet",
        "fiscal_year_start_month": 1,
        "calendar_source": "default",
    }
