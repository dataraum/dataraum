"""Bind a POINT-IN-TIME extract to a reporting-window boundary (DAT-887).

A stock is a level at an INSTANT, so an extract that reads one must say *which*
instant. Left to author the axis itself a model writes the only thing the data
alone suggests — ``period = (SELECT MAX(period) FROM <relation>)``, the latest
period the table happens to carry — which is unbound to any reporting window. On
a relation whose trailing periods run past the last fiscal close, that is simply
the wrong instant; where the measure happens to be flat across the tail it is the
right number by luck. Both states are indistinguishable to a consumer, because
nothing records which period the value is *for*.

**This module resolves the instant and records it; it never rewrites SQL.** The
resolved period is appended as one additional typed WHERE part at composition —
the same substrate ``validity_scope`` (DAT-733) appends the analytical-universe
predicate through, so every consumer that already reads ``parts``/
``where_predicates`` sees it — and the binding itself is recorded on the snippet
as the observable DAT-887 requires. The model is moved by SERVED FACTS (the
reporting calendar reaches the authoring prompt, which instructs it to leave the
point-in-time axis to the system), never by post-processing its output.

**The boundary is the last fiscal close the data reaches**, not the last period it
carries. For a fiscal year starting month ``m``, the closes are the instants
``(Y, m, 1)``; ``C`` is the latest one at or before the relation's max period, and
the bound period is the latest period PRESENT at or before ``C``. Resolving the
close first and *then* snapping to a present period is what keeps this free of any
stamping convention: a period-START-stamped relation (…, 2026-01-01, 2026-02-01)
binds 2026-01-01, and a period-END-stamped one (…, 2025-12-31, 2026-01-31) binds
2025-12-31 — the same fiscal instant under both. No column names are inspected and
no vertical is assumed; the calendar is the workspace's own declaration.

This is the sibling-derivation seam ``metric_graph_db_models`` pre-declared for its
first consumer: a fiscal-BOUNDARY rule (which instant) beside ``period_resolver``'s
period-grain WINDOW rule (how many days), reading the DAT-730 substrate's
``og_period_grain.fiscal_year_start_month``.

**Scope, stated not hidden.** The boundary resolved here is the fiscal-YEAR close.
The declared metric set that consumes point-in-time extracts is annual, and no
metric declares a reporting window of its own today, so the year is the only
boundary the substrate can express. A finer boundary (last complete fiscal quarter)
lands as another rung on the same ladder when a metric can ask for one.

**Absence falls loud.** Every unresolvable state — no read surface, no stock
measure to bind, a mixed stock/flow extract, no anchor axis, no period at or
before the close — returns a REASON, never a fabricated instant and never a silent
pass-through of the model's own pin. The caller turns a reason into a visible typed
disclosure on the extract, exactly as ``period_resolver`` flags its fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import duckdb
from sqlalchemy import bindparam, text

from dataraum.core.logging import get_logger
from dataraum.graphs.additivity import parse_aggregate_calls
from dataraum.graphs.additivity_resolver import served_relation
from dataraum.graphs.grounding_validation import where_filter_columns
from dataraum.graphs.models import AssumptionBasis, GraphAssumptionOutput

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.graphs.models import ExtractGroundingOutput

_log = get_logger(__name__)

# The ``og_columns.materialization`` verdicts. A ``stock`` is point-in-time (a level
# at an instant) and is what this module binds; a ``flow`` accumulates across a
# window and carries its own period through ``period_resolver`` instead.
_STOCK = "stock"
_FLOW = "flow"


@dataclass(frozen=True)
class ReportingCalendar:
    """The workspace's reporting calendar as the read surface serves it (DAT-730).

    ``fiscal_year_start_month`` is 1–12 (1 = January = a calendar year).
    ``source`` is ``'declared'`` when a :class:`~dataraum.analysis.semantic.db_models.WorkspaceCalendar`
    row exists and ``'default'`` when the calendar-year default was stamped in its
    absence — the distinction is carried, never collapsed, so a consumer can tell a
    declared fiscal year from an assumed one.
    """

    fiscal_year_start_month: int
    source: str


@dataclass(frozen=True)
class PeriodBinding:
    """The instant a point-in-time extract resolved to, and how it was decided.

    ``as_of`` is the period value PRESENT on the relation that the extract is bound
    to; ``window_close`` is the fiscal close it was snapped back from (the two are
    equal when the relation carries a period exactly on the boundary). ``axis`` is
    the bound column. The calendar fields travel with the binding so the recorded
    observable is self-describing — a consumer reading the snippet can tell which
    fiscal year the value is for, and whether that calendar was declared or assumed.
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


def read_reporting_calendar(session: Session, read_schema: str) -> ReportingCalendar | None:
    """The workspace's reporting calendar from the DAT-730 ladder vertex.

    Reads ``og_period_grain``, which already resolves the singleton
    ``workspace_calendar`` to a start month plus a ``calendar_source`` that names
    whether it was declared or defaulted — so the "default is stamped, never
    silent" discipline has ONE home and this module does not re-implement it.
    Returns ``None`` when the read surface is unavailable (the caller then makes no
    binding at all rather than assuming a calendar).
    """
    row = session.execute(
        text(  # noqa: S608 - read_schema is an internal identifier
            f'SELECT fiscal_year_start_month, calendar_source FROM "{read_schema}".og_period_grain'
            f" LIMIT 1"
        )
    ).first()
    if row is None or row[0] is None:
        return None
    return ReportingCalendar(fiscal_year_start_month=int(row[0]), source=str(row[1]))


def resolve_period_binding(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection | None,
    *,
    relation: str | None,
    select_expr: str,
    read_schema: str,
) -> PeriodBinding | str | None:
    """Resolve the reporting instant a point-in-time extract must be bound to.

    Three-valued by design, because "no binding" and "could not bind" are different
    facts and only one of them is a disclosure:

    * ``None`` — this extract is NOT point-in-time (it aggregates a flow, or it has
      no resolvable measure at all). Nothing to bind; flows keep their own window
      through ``period_resolver`` and are untouched here.
    * ``str`` — it IS point-in-time but the instant could not be resolved. The
      reason is returned for the caller to record as a visible typed disclosure;
      the model's own authored predicate stands rather than being silently
      "corrected" into an empty result.
    * :class:`PeriodBinding` — the resolved instant, for the caller to append as a
      typed WHERE part and record on the snippet.

    Every read is best-effort: a failure degrades to a reason string (a disclosure),
    never an exception into the authoring path.
    """
    if relation is None or duckdb_conn is None:
        return None
    # Postgres-only by construction, exactly as period_resolver: og_columns and the
    # calendar ladder live in the read schema, which the SQLite test substrate has
    # no analogue of. No surface to resolve against ⇒ no binding and no disclosure
    # (there is nothing to have failed at), never an assumed calendar.
    if session.get_bind().dialect.name != "postgresql":
        return None
    try:
        return _resolve(session, duckdb_conn, relation, select_expr, read_schema)
    except Exception as exc:  # noqa: BLE001 - degrade-to-disclosure IS the contract
        _log.warning("period_binding_failed", relation=relation, error=str(exc))
        return f"period binding read failed ({exc})"


def _resolve(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    relation: str,
    select_expr: str,
    read_schema: str,
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
    measures = _read_measure_materialization(
        session, read_schema, served.columns_table_id, measure_cols
    )
    stocks = [m for m in measures if m[0] == _STOCK]
    if not stocks:
        return None  # a flow (or unclassified) extract — not this module's business
    if any(m[0] == _FLOW for m in measures):
        # A single extract aggregating BOTH a stock and a flow has no one instant to
        # be "as of" — binding either way would misstate the other half. Disclose it.
        return "extract mixes stock and flow measures — no single point-in-time instant applies"
    axes = {m[1] for m in stocks if m[1]}
    if not axes:
        return "point-in-time measure has no anchor time axis to bind a reporting instant on"
    if len(axes) > 1:
        return f"point-in-time measures disagree on the anchor time axis ({sorted(axes)})"
    axis = str(next(iter(axes)))

    calendar = read_reporting_calendar(session, read_schema)
    if calendar is None:
        return "no reporting calendar on the read surface to place a fiscal boundary with"
    return _bind_to_close(duckdb_conn, relation, axis, calendar)


def _bind_to_close(
    duckdb_conn: duckdb.DuckDBPyConnection,
    relation: str,
    axis: str,
    calendar: ReportingCalendar,
) -> PeriodBinding | str:
    """Snap the relation's periods to the last fiscal close its data reaches.

    Two live reads, deliberately in this order: the max period decides WHICH close
    applies, then the close decides which PRESENT period is bound. Doing it the
    other way (pick a period, then check it) would need a stamping convention;
    this way needs none.
    """
    try:
        row = duckdb_conn.execute(
            f'SELECT MAX("{axis}"::TIMESTAMP) FROM {relation}'  # noqa: S608 - internal catalog names
            f' WHERE "{axis}" IS NOT NULL'
        ).fetchone()
    except duckdb.Error as exc:
        return f"period axis {axis!r} on {relation!r} is not readable ({exc})"
    if row is None or row[0] is None:
        return f"period axis {axis!r} on {relation!r} carries no periods"
    max_period: datetime = row[0]

    close = _latest_close(max_period, calendar.fiscal_year_start_month)
    try:
        bound = duckdb_conn.execute(
            f'SELECT MAX("{axis}"::TIMESTAMP) FROM {relation}'  # noqa: S608 - internal catalog names
            f' WHERE "{axis}"::TIMESTAMP <= $close',
            {"close": close},
        ).fetchone()
    except duckdb.Error as exc:
        return f"period axis {axis!r} on {relation!r} is not comparable to the fiscal close ({exc})"
    if bound is None or bound[0] is None:
        return (
            f"no period at or before the last fiscal close "
            f"({close.date().isoformat()}) on axis {axis!r}"
        )
    return PeriodBinding(
        as_of=bound[0],
        window_close=close,
        axis=axis,
        relation=relation,
        fiscal_year_start_month=calendar.fiscal_year_start_month,
        calendar_source=calendar.source,
    )


def compose_period_binding(
    output: ExtractGroundingOutput,
    where_parts: list[str],
    binding: PeriodBinding | str | None,
    served_columns: set[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> tuple[list[str], list[GraphAssumptionOutput], dict[str, str | int] | None]:
    """Compose a resolved period binding onto a grounding's WHERE parts (DAT-887).

    The DAT-733 shape, for the same reason: the binding rides the SAME parts substrate
    every consumer already reads, and an opt-out is RECORDED rather than silent.

    Returns ``(where_parts, assumptions, record)``:

    * ``where_parts`` — the grounding's parts plus the typed period predicate, when the
      grounding left the period axis unconstrained;
    * ``assumptions`` — one typed assumption per non-binding outcome: a DEFERRAL when
      the grounding pins the period axis ITSELF, or a DISCLOSURE naming why the instant
      could not be resolved. Both are visible; neither is a silent absence;
    * ``record`` — the observable to persist on the snippet, or ``None`` when nothing
      was bound.

    **The model's own pin is never overridden.** When the grounding constrains the
    period axis, appending a second predicate would not correct it — it would AND two
    incompatible pins into an empty result. So the binding defers and says so, and the
    authoring prompt (which is served the reporting calendar) is what moves the model to
    leave the axis alone in the first place.
    """
    if binding is None:
        return where_parts, [], None
    if isinstance(binding, str):
        return (
            where_parts,
            [
                GraphAssumptionOutput(
                    dimension="period.binding",
                    target="extract",
                    assumption=(
                        f"point-in-time extract is NOT bound to a reporting instant — {binding}; "
                        f"the value is as-of whatever period the grounding itself selects"
                    ),
                    basis=AssumptionBasis.INFERRED,
                    confidence=1.0,
                )
            ],
            None,
        )
    constrained = where_filter_columns(output, served_columns, duckdb_conn)
    if binding.axis in constrained:
        return (
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
    return [*where_parts, binding.render()], [], binding.as_record()


def _latest_close(max_period: datetime, start_month: int) -> datetime:
    """The latest fiscal-year close at or before ``max_period``.

    The close of the fiscal year that BEGAN at ``(Y-1, start_month)`` is the instant
    ``(Y, start_month, 1)``. Take this year's if the data reaches it, else last
    year's — so a relation running past the close (the DAT-887 defect: 14 monthly
    periods against a 12-month fiscal year) resolves to the close, not to its tail.
    """
    candidate = datetime(max_period.year, start_month, 1)
    if candidate > max_period:
        candidate = datetime(max_period.year - 1, start_month, 1)
    return candidate


def _read_measure_materialization(
    session: Session,
    read_schema: str,
    view_table_id: str,
    measure_cols: set[str],
) -> list[tuple[str | None, str | None]]:
    """Each measure column's ``(materialization, anchor_time_axis)`` from ``og_columns``.

    The same served-column read ``period_resolver`` uses to decide which operands
    carry a window — read here to decide the mirror question, which operands need an
    instant. Both the verdict and the axis are the resolved, vertical-neutral
    ``og_columns`` values (witness posterior over concept prior); nothing is derived
    from a column name.
    """
    stmt = text(
        f"SELECT DISTINCT materialization, anchor_time_axis"  # noqa: S608 - internal identifier
        f' FROM "{read_schema}".og_columns'
        f" WHERE table_id = :view_id AND column_name IN :measure_cols"
    ).bindparams(bindparam("measure_cols", expanding=True))
    rows = session.execute(
        stmt, {"view_id": view_table_id, "measure_cols": sorted(measure_cols)}
    ).all()
    return [
        (str(mat) if mat is not None else None, str(axis) if axis is not None else None)
        for mat, axis in rows
    ]
