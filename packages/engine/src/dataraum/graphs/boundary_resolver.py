"""Bind a POINT-IN-TIME extract to a reporting-window boundary (DAT-887).

A stock is a level at an INSTANT, so an extract that reads one must say *which*
instant. Left to author the axis itself a model writes the only thing the data alone
suggests — ``period = (SELECT MAX(period) FROM <relation>)``, the latest period the
table happens to carry — which is unbound to any reporting window. On a relation whose
trailing periods run past the last fiscal close that is simply the wrong instant; where
the measure happens to be flat across the tail it is the right number by luck. Both
states are indistinguishable to a consumer, because nothing records which period the
value is *for*.

**A period label is the instant the period STARTS.** This is the engine's own typing
contract, not an assumption about the data: ``config/phases/typing.yaml`` standardizes
``2025-12`` via ``STRPTIME(col || '-01')`` to ``DATE 2025-12-01`` and ``2025-Q4`` via
``MAKE_DATE(y, (q-1)*3+1, 1)``. So the row labelled ``2025-12-01`` carries DECEMBER'S
CLOSING level, and the level at a fiscal close is carried by the last period STRICTLY
BEFORE the close instant — never by the row labelled with it, which is the *next*
period's close. Everything below follows from that one fact.

**Resolution, in three steps.**

1. ``coverage_end = max_period + one_grain`` — the instant the data's last period ends.
   The grain is the axis's own ``detected_granularity``, so this is the real extent of
   coverage rather than the label of its last row.
2. ``C`` = the latest fiscal close at or before ``coverage_end``. Reachability must be
   grain-aware: a relation whose last label is ``2025-12-01`` COVERS the year ending
   ``2026-01-01``, and a rule reading only ``max(period)`` cannot tell that from data
   that ends eleven months short.
3. Bind the latest period **strictly before** ``C``. Strictness is what makes the rule
   free of any stamping convention — a start-stamped relation (…, 2025-12-01) and an
   end-stamped one (…, 2025-12-31) both resolve December — and it is what makes daily
   grain land on the 31st rather than a day late.

No column names are inspected and no vertical is assumed; the calendar is the
workspace's own declaration, read from DAT-730's ``og_period_grain`` ladder.

This is the sibling-derivation seam ``metric_graph_db_models`` pre-declared for its
first consumer: a fiscal-BOUNDARY rule (which instant) beside ``period_resolver``'s
period-grain WINDOW rule (how many days).

**Scope, stated not hidden.** The boundary resolved here is the fiscal-YEAR close. The
declared metric set that consumes point-in-time extracts is annual and no metric
declares a reporting window of its own today, so the year is the only boundary the
substrate can express. A finer boundary (last complete fiscal quarter) lands as another
rung on the same ladder when a metric can ask for one.

**The prompt coupling — why an unresolvable stock ABSTAINS.** The authoring prompt tells
the model to leave the period axis alone only for a measure the served context shows as
``Materialization: stock`` AND only when the Reporting-calendar section is present; an
unclassified measure keeps the declared ``end_of_period`` fallback and pins its own
period, exactly as before. That covers the branches where this module cannot tell a
stock from a flow. It does NOT cover the branches where the measure IS a known stock but
the instant cannot be resolved — and there, a composed extract with no period predicate
at all aggregates every period, which is far more wrong (~14× on a 14-period relation)
than the unbound ``MAX(period)`` this ticket exists to fix. So a known-stock-but-
unresolvable extract ABSTAINS: it composes to the fall-loud shape and discloses the
reason, rather than emitting a number nobody asked for. Abstaining is not a
deterministic override of the model's judgment — the model authored no period predicate
*because it was told the system would supply one*, and this is the system declining to,
out loud.

**Absence falls loud everywhere else too.** No read surface, no calendar, no anchor
axis, no cadence, an unreadable axis, no period before the close — every one returns a
REASON, never a fabricated instant and never a silent pass-through.
"""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import duckdb
from sqlalchemy import bindparam, text

from dataraum.analysis.temporal.models import DATE_TRUNC_GRAINS
from dataraum.core.logging import get_logger
from dataraum.graphs.additivity import parse_aggregate_calls
from dataraum.graphs.additivity_resolver import served_relation
from dataraum.graphs.grounding_validation import where_filter_columns
from dataraum.graphs.models import AssumptionBasis, GraphAssumptionOutput

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.graphs.models import ExtractGroundingOutput

_log = get_logger(__name__)

# The ``og_columns.materialization`` verdicts. A ``stock`` is point-in-time (a level at
# an instant) and is what this module binds; a ``flow`` accumulates across a window and
# carries its own period through ``period_resolver`` instead.
_STOCK = "stock"
_FLOW = "flow"

# How far one period of each grain advances — the ``coverage_end`` step. Keyed on the
# validated ``DATE_TRUNC_GRAINS`` vocabulary (never caller input). Calendar grains are
# kept in MONTHS so month-length variation can never drift the close; fixed grains
# advance by a timedelta. The ``irregular``/``unknown`` sentinels are absent from
# ``DATE_TRUNC_GRAINS`` by construction, so an axis with no cadence falls loud.
_GRAIN_MONTHS: dict[str, int] = {"month": 1, "quarter": 3, "year": 12}
_GRAIN_DELTA: dict[str, timedelta] = {
    "second": timedelta(seconds=1),
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
    "week": timedelta(days=7),
}

# The unbound disclosure must fall BELOW the metrics phase's low-confidence floor
# (``metrics_phase._LOW_CONFIDENCE_FLOOR``): "this number is not bound to a reporting
# window" is precisely the state that gate exists to surface, so it may not sail through
# at full confidence. The DEFERRAL case keeps a high confidence — there a defensible
# predicate IS in force (the model's own), it simply is not ours.
_UNBOUND_CONFIDENCE = 0.2


@dataclass(frozen=True)
class ReportingCalendar:
    """The workspace's reporting calendar as the read surface serves it (DAT-730).

    ``fiscal_year_start_month`` is 1–12 (1 = January = a calendar year). ``source`` is
    ``'declared'`` when a :class:`~dataraum.analysis.semantic.db_models.WorkspaceCalendar`
    row exists and ``'default'`` when the calendar-year default was stamped in its
    absence — the distinction is carried, never collapsed, so a consumer can tell a
    declared fiscal year from an assumed one.
    """

    fiscal_year_start_month: int
    source: str


@dataclass(frozen=True)
class PeriodBinding:
    """The instant a point-in-time extract resolved to, and how it was decided.

    ``as_of`` is the period value PRESENT on the relation that the extract is bound to —
    the last period whose close IS the fiscal close. ``window_close`` is that fiscal
    close instant; because a label is a period START, ``as_of`` is always strictly
    before it. ``axis`` is the bound column. The calendar fields travel with the binding
    so the recorded observable is self-describing.
    """

    as_of: datetime
    window_close: datetime
    axis: str
    relation: str
    fiscal_year_start_month: int
    calendar_source: str

    def render(self) -> str:
        """The typed equality predicate binding the axis to the resolved instant."""
        return f"\"{self.axis}\" = TIMESTAMP '{self.as_of.isoformat(sep=' ')}'"

    def as_record(self) -> dict[str, str | int]:
        """The recorded observable (DAT-887) — the persisted, machine-readable form."""
        return {
            "as_of": self.as_of.isoformat(sep=" "),
            "window_close": self.window_close.isoformat(sep=" "),
            "axis": self.axis,
            "relation": self.relation,
            "fiscal_year_start_month": self.fiscal_year_start_month,
            "calendar_source": self.calendar_source,
        }


@dataclass(frozen=True)
class BindingComposition:
    """The outcome of composing a resolved binding onto a grounding's clause parts.

    ``abstain`` is set ONLY for a known-stock extract whose instant could not be
    resolved: the caller composes the fall-loud shape instead of an extract that would
    silently aggregate every period. It is never set for a flow or an unclassifiable
    measure — those keep whatever the model authored.
    """

    where: list[str]
    assumptions: list[GraphAssumptionOutput]
    record: dict[str, str | int] | None
    abstain: str | None = None


@dataclass(frozen=True)
class _StockAxis:
    """A stock measure's anchor axis and that axis's detected cadence."""

    axis: str | None
    grain: str | None


def read_reporting_calendar(session: Session, read_schema: str) -> ReportingCalendar | None:
    """The workspace's reporting calendar from the DAT-730 ladder vertex.

    Reads ``og_period_grain``, which already resolves the singleton
    ``workspace_calendar`` to a start month plus a ``calendar_source`` naming whether it
    was declared or defaulted — so the "default is stamped, never silent" discipline has
    ONE home and this module does not re-implement it. The ladder carries one row per
    grain rung, all with the SAME calendar columns, so the read is ``DISTINCT`` and
    ORDERED — never a ``LIMIT 1`` over an unordered scan, which would be an arbitrary
    pick if a rung ever carried different values. Returns ``None`` when the read surface
    is unavailable or serves no calendar.
    """
    rows = session.execute(
        text(  # noqa: S608 - read_schema is an internal identifier
            f"SELECT DISTINCT fiscal_year_start_month, calendar_source"
            f' FROM "{read_schema}".og_period_grain'
            f" ORDER BY fiscal_year_start_month, calendar_source"
        )
    ).all()
    if not rows or rows[0][0] is None:
        return None
    return ReportingCalendar(fiscal_year_start_month=int(rows[0][0]), source=str(rows[0][1]))


def resolve_period_binding(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection | None,
    *,
    relation: str | None,
    select_expr: str,
    read_schema: str,
    calendar: ReportingCalendar | None,
) -> PeriodBinding | str | None:
    """Resolve the reporting instant a point-in-time extract must be bound to.

    Three-valued by design, because "no binding" and "could not bind" are different
    facts and only one of them is a disclosure:

    * ``None`` — this extract is NOT knowably point-in-time (it aggregates a flow, its
      measure is unclassified, or its expression has no resolvable measure at all).
      Nothing to bind and nothing to say: the model's own judgment stands, which is why
      the authoring prompt keeps the ``end_of_period`` fallback for exactly these.
    * ``str`` — it IS a known stock but the instant could not be resolved. The caller
      turns this into an ABSTENTION plus a visible disclosure (see the module docstring).
    * :class:`PeriodBinding` — the resolved instant.

    ``calendar`` is passed in (the served context already read it once per assembly)
    rather than re-read per extract. Every read is best-effort: a failure ROLLS BACK the
    session before degrading to a reason, so a poisoned Postgres transaction can never
    reach the snippet writes that run after this.
    """
    if relation is None or duckdb_conn is None:
        return None
    # Postgres-only by construction, exactly as period_resolver: og_columns lives in the
    # read schema, which the SQLite test substrate has no analogue of. No surface ⇒ no
    # binding and no disclosure (there is nothing to have failed at).
    if session.get_bind().dialect.name != "postgresql":
        return None
    try:
        return _resolve(session, duckdb_conn, relation, select_expr, read_schema, calendar)
    except Exception as exc:  # noqa: BLE001 - degrade-to-disclosure IS the contract
        # A failed statement leaves the Postgres transaction ABORTED; every later
        # statement on this session — notably the snippet writes — would fail too. Roll
        # back before degrading so the disclosure costs only this binding.
        _log.warning("period_binding_failed", relation=relation, error=str(exc))
        try:
            session.rollback()
        except Exception as rollback_exc:  # noqa: BLE001 - nothing left to salvage
            _log.warning("period_binding_rollback_failed", error=str(rollback_exc))
        return f"period binding read failed ({exc})"


def _resolve(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    relation: str,
    select_expr: str,
    read_schema: str,
    calendar: ReportingCalendar | None,
) -> PeriodBinding | str | None:
    """Classify the extract, then resolve its instant (see :func:`resolve_period_binding`)."""
    served = served_relation(session, relation)
    if served is None:
        return None  # outside the analysis — not knowably point-in-time
    try:
        measure_cols = {
            col for call in parse_aggregate_calls(select_expr, duckdb_conn) for col in call.columns
        }
    except ValueError:
        return None  # unparsed expr — the additivity surface reports this class
    if not measure_cols:
        return None
    verdicts, stocks = _read_stock_axes(session, read_schema, served.columns_table_id, measure_cols)
    if not stocks:
        return None  # a flow, or unclassified — not knowably this module's business
    if _FLOW in verdicts:
        # A single extract aggregating BOTH a stock and a flow has no one instant to be
        # "as of" — binding either way would misstate the other half.
        return "extract mixes stock and flow measures — no single point-in-time instant applies"
    axes = {s.axis for s in stocks if s.axis}
    if not axes:
        return "point-in-time measure has no anchor time axis to bind a reporting instant on"
    if len(axes) > 1:
        return f"point-in-time measures disagree on the anchor time axis ({sorted(axes)})"
    axis = str(next(iter(axes)))
    grains = {s.grain for s in stocks if s.grain}
    if not grains:
        return f"anchor axis {axis!r} has no temporal profile — its period length is unknown"
    if len(grains) > 1:
        return f"point-in-time measures disagree on the axis cadence ({sorted(grains)})"
    grain = str(next(iter(grains)))
    if grain not in DATE_TRUNC_GRAINS or _advance(datetime(2000, 1, 1), grain) is None:
        return f"anchor axis {axis!r} cadence {grain!r} has no clean period length"
    if calendar is None:
        return "no reporting calendar on the read surface to place a fiscal boundary with"
    return _bind_to_close(duckdb_conn, relation, axis, grain, calendar)


def _bind_to_close(
    duckdb_conn: duckdb.DuckDBPyConnection,
    relation: str,
    axis: str,
    grain: str,
    calendar: ReportingCalendar,
) -> PeriodBinding | str:
    """Resolve the fiscal close the data reaches, then the last period strictly before it.

    Two live reads in this order — the EXTENT of coverage decides which close applies,
    then the close decides which PRESENT period is bound. Doing it the other way round
    would need a stamping convention; this way needs none.
    """
    max_period = _read_max_period(duckdb_conn, relation, axis)
    if isinstance(max_period, str):
        return max_period

    coverage_end = _advance(max_period, grain)
    if coverage_end is None:  # defensive; grain was validated in _resolve
        return f"anchor axis {axis!r} cadence {grain!r} has no period length to advance by"
    close = _latest_close(coverage_end, calendar.fiscal_year_start_month)
    bound = _read_last_period_before(duckdb_conn, relation, axis, close)
    if isinstance(bound, str):
        return bound
    if bound is None:
        return (
            f"no period before the fiscal close ({close.date().isoformat()}) on axis "
            f"{axis!r} — the reporting year is not covered by this relation"
        )
    return PeriodBinding(
        as_of=bound,
        window_close=close,
        axis=axis,
        relation=relation,
        fiscal_year_start_month=calendar.fiscal_year_start_month,
        calendar_source=calendar.source,
    )


def _axis_expr(axis: str) -> str:
    """The axis as a naive UTC timestamp.

    A TIMESTAMPTZ axis rendered with a bare ``::TIMESTAMP`` is converted using the
    SESSION time zone, so the persisted ``as_of`` would drift with whichever TZ the
    worker process happens to run in — the same data would bind different instants on
    two machines. Normalizing ``AT TIME ZONE 'UTC'`` first makes the resolved instant a
    property of the data alone. Written once, here, so both reads agree by construction.
    """
    return (
        f"CASE WHEN typeof(\"{axis}\") = 'TIMESTAMP WITH TIME ZONE'"
        f" THEN (\"{axis}\"::TIMESTAMPTZ AT TIME ZONE 'UTC')"
        f' ELSE "{axis}"::TIMESTAMP END'
    )


def _read_max_period(
    duckdb_conn: duckdb.DuckDBPyConnection, relation: str, axis: str
) -> datetime | str:
    """The latest period present on the axis, or a fall-loud reason."""
    try:
        row = duckdb_conn.execute(
            f"SELECT MAX({_axis_expr(axis)}) FROM {relation}"  # noqa: S608 - internal catalog names
            f' WHERE "{axis}" IS NOT NULL'
        ).fetchone()
    except duckdb.Error as exc:
        return f"period axis {axis!r} on {relation!r} is not readable as a timestamp ({exc})"
    if row is None or not isinstance(row[0], datetime):
        return f"period axis {axis!r} on {relation!r} carries no periods"
    return row[0]


def _read_last_period_before(
    duckdb_conn: duckdb.DuckDBPyConnection, relation: str, axis: str, close: datetime
) -> datetime | None | str:
    """The latest period STRICTLY before the close — the period whose close IS it."""
    try:
        row = duckdb_conn.execute(
            f"SELECT MAX({_axis_expr(axis)}) FROM {relation}"  # noqa: S608 - internal catalog names
            f" WHERE {_axis_expr(axis)} < $close",
            {"close": close},
        ).fetchone()
    except duckdb.Error as exc:
        return f"period axis {axis!r} on {relation!r} is not comparable to the fiscal close ({exc})"
    return row[0] if row is not None and isinstance(row[0], datetime) else None


def _advance(moment: datetime, grain: str) -> datetime | None:
    """``moment`` advanced by exactly one period of ``grain``.

    Calendar grains advance in MONTHS (so a 28- or 31-day month can never drift the
    close); fixed grains advance by a timedelta. ``None`` for a grain with no period
    length, which the caller turns into a fall-loud reason.
    """
    months = _GRAIN_MONTHS.get(grain)
    if months is not None:
        total = moment.month - 1 + months
        year, month = moment.year + total // 12, total % 12 + 1
        # An END-stamped axis lands on day 28–31, and the target month may be shorter
        # (2026-01-31 + 1 month). Clamp to the target month's last day rather than
        # overflowing: this step only measures how far coverage REACHES, and a clamped
        # day can never move it across a fiscal close (all closes are day 1).
        return moment.replace(
            year=year, month=month, day=min(moment.day, monthrange(year, month)[1])
        )
    delta = _GRAIN_DELTA.get(grain)
    return None if delta is None else moment + delta


def _latest_close(coverage_end: datetime, start_month: int) -> datetime:
    """The latest fiscal-year close at or before ``coverage_end``.

    The fiscal year beginning at ``(Y-1, start_month, 1)`` closes at the instant
    ``(Y, start_month, 1)``. Taking the latest close the data's COVERAGE reaches — not
    the latest one its last LABEL reaches — is what lets a relation whose final label is
    2025-12-01 bind the year ending 2026-01-01, while data that genuinely stops eleven
    months short does not.

    Always TOTAL: ``(Y-1, start_month, 1)`` is at most ``(Y-1, 12, 1)``, which precedes
    any instant in year ``Y``, so a close at or before ``coverage_end`` always exists.
    Data lying entirely before its first usable close is caught downstream, where the
    honest reason is that no PERIOD precedes the close — not that no close exists.
    """
    candidate = datetime(coverage_end.year, start_month, 1)
    if candidate > coverage_end:
        candidate = datetime(coverage_end.year - 1, start_month, 1)
    return candidate


def compose_period_binding(
    output: ExtractGroundingOutput,
    where_parts: list[str],
    binding: PeriodBinding | str | None,
    served_columns: set[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> BindingComposition:
    """Compose a resolved period binding onto a grounding's WHERE parts (DAT-887).

    The DAT-733 shape, for the same reason: the binding rides the SAME parts substrate
    every consumer already reads, and an opt-out is RECORDED rather than silent.

    Outcomes:

    * a resolved instant on an unconstrained axis → the typed predicate is appended and
      the observable recorded;
    * a resolved instant on an axis the grounding pins ITSELF → DEFER. The model's own
      predicate is never overridden (ANDing a second pin would produce an empty result,
      not a correction), and the deferral is recorded at full confidence — a defensible
      predicate IS in force, it simply is not ours;
    * an unresolvable instant on a KNOWN stock → ABSTAIN, disclosed BELOW the
      low-confidence floor. Composing a stock extract with no period predicate would
      aggregate every period, which is worse than the defect this ticket fixes;
    * anything else (a flow, an unclassified measure) → untouched.
    """
    if binding is None:
        return BindingComposition(where_parts, [], None)
    if isinstance(binding, str):
        return BindingComposition(
            where_parts,
            [
                GraphAssumptionOutput(
                    dimension="period.binding",
                    target="extract",
                    assumption=(
                        f"point-in-time extract ABSTAINED — no reporting instant could be "
                        f"resolved to bind it to: {binding}"
                    ),
                    basis=AssumptionBasis.INFERRED,
                    confidence=_UNBOUND_CONFIDENCE,
                )
            ],
            None,
            abstain=binding,
        )
    constrained = where_filter_columns(output, served_columns, duckdb_conn)
    if binding.axis in constrained:
        return BindingComposition(
            where_parts,
            [
                GraphAssumptionOutput(
                    dimension="period.binding",
                    target=f"column:{binding.relation}.{binding.axis}",
                    assumption=(
                        f"reporting-instant binding {binding.render()} not applied — "
                        f"grounding constrains {binding.axis} directly"
                    ),
                    basis=AssumptionBasis.INFERRED,
                    confidence=1.0,
                )
            ],
            None,
        )
    return BindingComposition([*where_parts, binding.render()], [], binding.as_record())


def _read_stock_axes(
    session: Session,
    read_schema: str,
    view_table_id: str,
    measure_cols: set[str],
) -> tuple[set[str], list[_StockAxis]]:
    """Every measure's materialization verdict, plus the STOCK measures' axis + cadence.

    The same served-column read ``period_resolver`` uses to decide which operands carry a
    window — read here to decide the mirror question, which operands need an instant. The
    verdict, the anchor axis and its cadence are all resolved, vertical-neutral values
    (``og_columns``' witness posterior over concept prior, and the axis's own
    ``detected_granularity``); nothing is derived from a column name.

    The joins are LEFT so a stock whose anchor is not served on THIS relation, or whose
    axis was never temporally profiled, still returns a row with ``axis``/``grain``
    ``None`` — the caller falls loud on it rather than silently treating it as a flow.
    """
    stmt = text(
        f"SELECT DISTINCT m.materialization, axis_col.column_name AS axis,"  # noqa: S608
        f"       tp.detected_granularity AS grain"
        f' FROM "{read_schema}".og_columns m'
        f'  LEFT JOIN "{read_schema}".current_enriched_columns axis_col'
        f"         ON axis_col.table_id = :view_id"
        f"        AND axis_col.column_name = m.anchor_time_axis"
        f'  LEFT JOIN "{read_schema}".current_temporal_column_profiles tp'
        f"         ON tp.column_id = axis_col.source_column_id"
        f" WHERE m.table_id = :view_id AND m.column_name IN :measure_cols"
        f" ORDER BY m.materialization, axis, grain"  # deterministic evidence
    ).bindparams(bindparam("measure_cols", expanding=True))
    rows = session.execute(
        stmt, {"view_id": view_table_id, "measure_cols": sorted(measure_cols)}
    ).all()
    verdicts = {str(mat) for mat, _, _ in rows if mat is not None}
    stocks = [
        _StockAxis(
            axis=str(axis) if axis is not None else None,
            grain=str(grain) if grain is not None else None,
        )
        for mat, axis, grain in rows
        if mat == _STOCK
    ]
    return verdicts, stocks
