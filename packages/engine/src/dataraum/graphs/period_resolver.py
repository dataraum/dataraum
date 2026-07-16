"""Derive a metric's ``days_in_period`` from the flow's observed data window (DAT-785).

Some metrics turn a stock/flow ratio into a number of days by multiplying it by
``days_in_period``: ``(stock / flow) × days_in_period``. ``flow / days_in_period`` is
the flow's per-day rate, and ``stock / daily_rate`` is how many days of that flow the
stock represents. So ``days_in_period`` must equal the number of days the flow was
accumulated across — the window the FLOW is measured over — not a config constant.

**The flow is the measure whose resolved stock/flow verdict is ``flow``.** A flow is
accumulated over a period (its value sums movements across a window); a stock is
point-in-time (a level at an instant, with no accumulation window). Which is which is
read from the authoritative, vertical-neutral ``og_columns.materialization`` verdict
(``flow`` | ``stock``) — built in the read surface as the COALESCE of the
aggregation-lineage witness posterior over the concept prior, normalized to
flow/stock. Every EXTRACT operand is a candidate; the ones whose measure resolves to
``flow`` carry the window, and a ``stock`` (or any non-``flow`` measure) is excluded —
point-in-time, it contributes no window. Nothing here reads a domain-specific field:
flow-vs-stock IS the ratio's structure, so the resolved verdict is the exact signal.

**The window is the flow's OWN filtered rows, measured live.** A flow is commonly
grounded by filtering the fact on a discriminator (``SUM(amount) WHERE …``) — the
common shape, not an edge case (the flow blueprint in ``graph_sql_generation.yaml``
filters the fact by a discriminator). So the window cannot be read off the precomputed
``current_temporal_column_profiles.span_days``: that is a WHOLE-COLUMN MIN/MAX over
every row, whereas the SUM scans only the filtered rows. Instead the resolver runs a
live ``MIN/MAX/COUNT(DISTINCT period)`` over the flow's anchor axis **filtered by the
exact same WHERE predicate the SUM applies** (:func:`~dataraum.graphs.formula_composer.compose_where_predicate`
is the single source of that filter), against the SAME grounded relation in DuckDB the
SUM runs on. The window is therefore the flow's own filtered span, by construction.

**Fencepost correction.** ``span = max − min`` measures first-datapoint-to-last, so
it spans ``actual_periods − 1`` inter-period gaps and undercounts the true window by
~one period for period-aggregated flow data (12 month-end rows → ~334 days, should
be ~365). The live query counts the distinct periods (``date_trunc`` at the axis's
detected granularity), and the span is corrected ``span × n / (n − 1)``. This
self-scales — negligible for transaction-grained data (many periods → factor ≈ 1),
~one period for aggregated data (12 → 12/11) — and is data-derived, never the
circular ``detected_granularity → 30`` label. A single period gives no gap to
correct against, so it falls loud rather than fabricate a window.

**The axis has one home.** Which column is the flow's time axis is read from
``og_columns.anchor_time_axis`` (DAT-780 witness-precedence COALESCE), never
re-derived from ``time_columns`` here. Only the axis IDENTITY and its detected
cadence come from the Postgres read surface; the span itself is measured live in
DuckDB over the filtered rows.

**Fall loud (K6 — absence must never resolve to a plausible default).** When no flow
window can be *observed* — no operand resolves to a ``flow`` materialization, the flow
never grounded, its anchor axis is NULL (the DAT-801 header-date facts serve NULL), it
has no temporal profile, its cadence is irregular/unknown, its filtered window is empty
or a single period, or two flows disagree on a window — the config default survives
ONLY as a declared fallback that is flagged in the answer (a verification flag on the
executed artifact), never a silent 30.

The read surface (``og_columns`` + ``current_temporal_column_profiles``) is
Postgres-only by construction (the SQLite test substrate has no read schema —
``read_views.materialize_read_schema`` is Postgres-guarded), so on any non-Postgres
bind the resolver returns ``None`` (no override): there is no data window to observe,
and production always runs on Postgres.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import duckdb
from sqlalchemy import bindparam, text

from dataraum.core.logging import get_logger
from dataraum.graphs.additivity import parse_aggregate_calls
from dataraum.graphs.additivity_resolver import fact_table_id, grounded_select
from dataraum.graphs.formula_composer import compose_where_predicate
from dataraum.graphs.models import StepType
from dataraum.query.snippet_library import SnippetLibrary
from dataraum.server.workspace import schema_name_for
from dataraum.storage.read_views import read_schema_name_for

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.graphs.models import GraphStep, TransformationGraph

_log = get_logger(__name__)

_PARAMETER = "days_in_period"
# The resolved stock/flow verdict that carries the accumulation window. Read from
# ``og_columns.materialization`` — the ONE vertical-neutral flow/stock signal (a
# COALESCE of the aggregation-lineage witness posterior over the concept prior); the
# non-``flow`` measures (stocks, point-in-time levels) are excluded.
_FLOW = "flow"

# The detected-granularity labels that are valid DuckDB ``date_trunc`` period parts —
# exactly the config granularity definitions (config/phases/temporal.yaml). The two
# sentinels ``irregular`` / ``unknown`` (temporal detection's no-cadence fallbacks)
# are deliberately excluded: a column with no clean cadence has no period to count or
# fencepost-correct against, so the resolver falls loud rather than bucket by a
# meaningless grain (or inject an invalid identifier into the window query).
_DATE_TRUNC_GRAINS: frozenset[str] = frozenset(
    {"second", "minute", "hour", "day", "week", "month", "quarter", "year"}
)


@dataclass(frozen=True)
class PeriodResolution:
    """The ``days_in_period`` to inject for a metric, plus how it was decided.

    ``days`` is always the value to feed the constant step: the observed flow-axis
    window when it could be derived, else the graph's declared config default.
    ``flag`` is a loud, user-facing verification flag naming the fallback — set ONLY
    when the default is used because the data could not be observed; ``None`` on a
    clean derivation. ``evidence`` carries structured fields for the log line.
    """

    days: float
    flag: str | None
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def derived(self) -> bool:
        """Whether ``days`` came from the data (vs the declared config fallback)."""
        return self.flag is None


@dataclass(frozen=True)
class _AxisWindow:
    """One flow measure's live-observed, fencepost-corrected window."""

    corrected_days: float
    axis: str
    filtered_span_days: float
    actual_periods: int


@dataclass(frozen=True)
class _MeasureAxis:
    """One measure column's resolved flow/stock verdict and its anchor axis + cadence.

    ``materialization`` is the vertical-neutral ``og_columns`` verdict (``flow`` |
    ``stock`` | ``None``) that decides whether this measure carries an accumulation
    window. ``axis`` / ``grain`` are the anchor time axis and its detected cadence —
    both ``None`` when the measure has no declared anchor (DAT-801) or the axis was
    never temporally profiled, in which case a ``flow`` measure falls loud (its window
    can't be observed) while a non-``flow`` measure is simply excluded.
    """

    materialization: str | None
    axis: str | None
    grain: str | None


def resolve_days_in_period(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    graph: TransformationGraph,
    workspace_id: str,
) -> PeriodResolution | None:
    """Derive ``days_in_period`` for a metric from its flow's observed window.

    Returns ``None`` when the metric has no ``days_in_period`` parameter (nothing to
    derive) or the substrate carries no read surface to observe a window from (a
    non-Postgres bind) — in both cases the caller injects no override and the
    graph's own default stands. Otherwise returns a :class:`PeriodResolution` whose
    ``days`` is the observed window, or the config default with a loud ``flag`` when
    the window cannot be observed.

    Every read of the Postgres surface (the snippet lookup, the fact resolution, and
    the axis/cadence read) plus the live DuckDB window query run inside one SAVEPOINT:
    ANY failure — a missing read surface, a malformed persisted snippet, an unexpected
    bug — rolls back only the nested transaction (never poisoning the outer assembly
    session that runs ``agent.assemble`` next) and degrades to the flagged default
    rather than failing the metric, mirroring the best-effort savepoint convention in
    ``metrics_phase``. The DuckDB scan runs while that Postgres SAVEPOINT is open — a
    deliberate, bounded tradeoff (a metric touches one fact; the parallel dispatch path
    gives each metric its own isolated session so this never compounds).
    """
    default = _default_days(graph)
    if default is None:
        return None
    # No read surface to observe a data window from (SQLite test substrate). Not a
    # real abstention — there is nothing to derive from — so no override, no flag.
    dialect = session.get_bind().dialect.name
    if dialect != "postgresql":
        _log.debug("period_no_read_surface", graph_id=graph.graph_id, dialect=dialect)
        return None

    # Candidate operands: every grounded EXTRACT. Which of them is a FLOW — the one
    # carrying the accumulation window — is decided PER MEASURE by its resolved
    # ``og_columns.materialization == 'flow'`` verdict (read live in _observe_flow_step),
    # never by a vertical convention; a stock (or any non-'flow' measure) is excluded,
    # point-in-time with no window. Holds for the working-capital graphs that use the
    # parameter (dpo/dso/dio/ccc — each flow extract IS a ratio operand): a graph that
    # added a flow extract unrelated to the ratio would fold into reconciliation, so a
    # future non-ratio use of the parameter should scope this to the CONSTANT's
    # dependency cone.
    extract_steps = [
        step
        for step in graph.steps.values()
        if step.step_type == StepType.EXTRACT and step.source is not None
    ]
    if not extract_steps:
        return _fallback(default, "no extract operand to observe a period from")

    library = SnippetLibrary(session, workspace_id=workspace_id)
    read_schema = read_schema_name_for(schema_name_for(workspace_id))
    try:
        with session.begin_nested():
            return _derive_period(
                session, duckdb_conn, library, read_schema, extract_steps, workspace_id, default
            )
    except Exception as exc:  # noqa: BLE001 - degrade-to-flagged-default IS the contract
        # ANY failure inside the savepoint (missing read surface, malformed snippet,
        # unexpected bug) rolled back only the nested transaction — the outer session
        # is intact; fall loud to the flagged default rather than failing (or silently
        # defaulting) the metric. Narrowing this to SQLAlchemyError would quietly turn a
        # non-DB read failure into a metric failure, contradicting this module's own
        # "never fail the metric, only flag it" contract.
        _log.warning("period_read_failed", graph_id=graph.graph_id, error=str(exc))
        return _fallback(default, f"period read surface unavailable ({exc})")


def _derive_period(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    library: SnippetLibrary,
    read_schema: str,
    extract_steps: list[GraphStep],
    workspace_id: str,
    default: float,
) -> PeriodResolution:
    """Observe every flow measure's window and reconcile them to one derived period.

    Each extract operand is classified live by its measure's ``materialization``: a
    ``flow`` yields ≥1 window, a stock yields none (excluded). ccc feeds ONE shared
    days_in_period into dso+dio−dpo, so its two flows (which usually share one fact
    but can carry different per-measure anchor axes, hence different windows) must ALL
    agree on one window — every observation is collected flat and reconciled, never
    keyed by fact (which would silently let the later flow's window overwrite the
    earlier's). A metric with NO flow operand at all falls loud, never a silent 30.
    """
    observations: list[_AxisWindow] = []
    flow_extracts = 0
    for step in extract_steps:
        outcome = _observe_flow_step(session, duckdb_conn, library, read_schema, step, workspace_id)
        if isinstance(outcome, str):
            return _fallback(default, outcome)
        if outcome:  # non-empty ⇒ this extract's measure resolved to 'flow'; a stock ⇒ []
            flow_extracts += 1
            observations.extend(outcome)
    if not observations:
        return _fallback(
            default, "no flow operand to observe a period from (no measure resolved to 'flow')"
        )
    return _reconcile(observations, default, flow_extracts)


def _observe_flow_step(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    library: SnippetLibrary,
    read_schema: str,
    step: GraphStep,
    workspace_id: str,
) -> list[_AxisWindow] | str:
    """Every anchor-axis window a single extract yields, or a fall-loud reason.

    Recovers the extract's grounded ``(select_expr, relation, where)``, resolves the
    fact and its measure columns, and reads each measure's resolved ``materialization``
    verdict + anchor axis + cadence. Returns:

    - the flow measures' live filtered windows (``≥1``) when this extract is a flow;
    - an EMPTY list when no measure resolved to ``flow`` — a stock (or unclassified)
      operand, excluded from the flow set (it contributes no window and is not itself a
      fall-loud reason);
    - a fall-loud reason string when the extract did not ground / is outside the
      analysis / its expr didn't parse, or when a ``flow`` measure has no observable
      window (null anchor axis, no temporal profile, no clean cadence, empty or
      single-period filtered window).
    """
    field_name = step.source.standard_field if step.source else "?"
    resolved = grounded_select(library, workspace_id, step)
    if resolved is None:
        return f"extract '{field_name}' did not ground"
    select_expr, relation, where = resolved
    fact_id = fact_table_id(session, relation)
    if fact_id is None:
        return f"extract relation {relation!r} is outside the analysis"
    try:
        measure_cols = {
            col for call in parse_aggregate_calls(select_expr, duckdb_conn) for col in call.columns
        }
    except ValueError:
        return f"extract select_expr for {relation!r} did not parse"
    if not measure_cols:
        return f"extract on {relation!r} aggregates no column to anchor on"
    measures = _read_measure_axes(session, read_schema, fact_id, measure_cols)
    flows = [m for m in measures if m.materialization == _FLOW]
    if not flows:
        # No measure here resolves to a 'flow' verdict — a stock (point-in-time, no
        # accumulation window) or an unclassified measure. Excluded from the flow set:
        # it contributes no window, and its absence is NOT a fall-loud reason (a metric
        # with no flow operand AT ALL falls loud once, in _derive_period).
        return []
    where_clause = compose_where_predicate(where)
    windows: list[_AxisWindow] = []
    for measure in flows:
        if measure.axis is None or measure.grain is None:
            return (
                f"flow '{field_name}' has no observable anchor-axis span "
                f"(null anchor time axis or no temporal profile)"
            )
        window = _observe_window(duckdb_conn, relation, measure.axis, measure.grain, where_clause)
        if isinstance(window, str):
            return f"flow '{field_name}': {window}"
        windows.append(window)
    return windows


def _observe_window(
    duckdb_conn: duckdb.DuckDBPyConnection,
    relation: str,
    axis: str,
    grain: str,
    where_clause: str | None,
) -> _AxisWindow | str:
    """The live, WHERE-filtered, fencepost-corrected window on one anchor axis.

    Runs ``MIN/MAX/COUNT(DISTINCT date_trunc(grain, axis))`` over the SAME grounded
    relation and the SAME WHERE predicate the executed flow SUM applies — so the
    window is measured over exactly the rows the SUM scans, never the whole column.
    Returns a fall-loud reason string when the grain has no clean period bucket, the
    filtered window is empty (no rows match the predicate), or it collapses to a
    single period (no gap to fencepost-correct against).
    """
    if grain not in _DATE_TRUNC_GRAINS:
        return f"anchor axis {axis!r} cadence {grain!r} has no clean period to correct against"
    sql = (
        f'SELECT MIN("{axis}"), MAX("{axis}"), '  # noqa: S608 - identifiers are internal catalog names
        f"COUNT(DISTINCT date_trunc('{grain}', \"{axis}\")) "
        f"FROM {relation}"
    )
    if where_clause:
        sql += f"\nWHERE {where_clause}"
    try:
        row = duckdb_conn.execute(sql).fetchone()
    except duckdb.Error as exc:
        _log.warning("period_window_query_failed", relation=relation, axis=axis, error=str(exc))
        return f"filtered window query over {relation!r} failed ({exc})"
    if row is None or row[0] is None or row[1] is None:
        return "filtered flow window is empty (no rows match the flow predicate)"
    min_ts, max_ts, periods = row
    span_days = (max_ts - min_ts).total_seconds() / 86400
    corrected = _apply_fencepost(span_days, periods)
    if corrected is None:
        return "single-period or degenerate filtered window (cannot fencepost-correct)"
    return _AxisWindow(
        corrected_days=corrected,
        axis=str(axis),
        filtered_span_days=span_days,
        actual_periods=int(periods),
    )


def _apply_fencepost(span_days: float, actual_periods: int | None) -> float | None:
    """Correct a period-aggregated span for the fencepost undercount (DAT-785).

    ``span = max − min`` spans ``actual_periods − 1`` inter-period gaps, so it
    undercounts the true window by ~one period for period-aggregated flow data (12
    month-end rows → ~334 days, should be ~365). Extrapolating the missing period
    gives ``span × n / (n − 1)``. Self-scaling: negligible for transaction-grained
    data (many periods → factor ≈ 1), ~one period for aggregated data (12 → 12/11).

    Returns ``None`` for a single (or zero) period or a non-positive span — one
    period gives no gap to measure a correction against, so the window can't be
    observed and the caller falls loud rather than fabricate one.
    """
    if actual_periods is None or actual_periods < 2 or span_days <= 0:
        return None
    return span_days * actual_periods / (actual_periods - 1)


def _reconcile(observations: list[_AxisWindow], default: float, flows: int) -> PeriodResolution:
    """One derived period from every flow's window, or fall loud if they disagree."""
    distinct = {round(o.corrected_days, 3) for o in observations}
    if len(distinct) != 1:
        return _fallback(
            default,
            f"flows disagree on the period window (observed {sorted(distinct)} days)",
        )
    rep = observations[0]
    return PeriodResolution(
        days=rep.corrected_days,
        flag=None,
        evidence={
            "derived": True,
            "days": rep.corrected_days,
            "anchor_time_axis": sorted({o.axis for o in observations}),
            "flows": flows,
            "filtered_span_days": round(rep.filtered_span_days, 3),
            "actual_periods": rep.actual_periods,
            "fencepost_factor": round(rep.actual_periods / (rep.actual_periods - 1), 4),
        },
    )


def _default_days(graph: TransformationGraph) -> float | None:
    """The graph's declared ``days_in_period`` default, or ``None`` if it has none."""
    for param in graph.parameters:
        if param.name == _PARAMETER and param.default is not None:
            try:
                return float(param.default)
            except TypeError, ValueError:
                return None
    return None


def _fallback(default: float, reason: str) -> PeriodResolution:
    """Keep the config default, but flag it loudly (never a silent fallback)."""
    return PeriodResolution(
        days=default,
        flag=(
            f"{_PARAMETER} fell back to config default {int(default) if default.is_integer() else default} "
            f"(no observed period: {reason})"
        ),
        evidence={"derived": False, "days": default, "reason": reason},
    )


def _read_measure_axes(
    session: Session,
    read_schema: str,
    fact_table_id_: str,
    measure_cols: set[str],
) -> list[_MeasureAxis]:
    """Resolved ``(materialization, anchor_axis, detected_granularity)`` per measure column.

    Reads each of the extract's measure column(s) from ``og_columns``: its resolved
    ``materialization`` (the vertical-neutral flow/stock verdict — flow measures carry
    the accumulation window, stocks are point-in-time), its DAT-780 anchor axis
    (``anchor_time_axis``), and — via a LEFT self-join to that axis column's own vertex
    and the head-resolved ``current_temporal_column_profiles`` — the axis's
    ``detected_granularity`` (the period bucket the live window query counts distinct
    periods by).

    The joins are LEFT so a flow whose anchor axis is NULL (a header-date fact,
    DAT-801) or whose axis was never temporally profiled STILL returns its
    ``materialization`` row (``axis``/``grain`` ``None``): the caller falls loud on an
    unobservable FLOW while excluding a stock, a distinction an inner join would erase.
    An EMPTY result (the measure columns are absent from the read surface) surfaces no
    ``flow`` verdict, so the extract is excluded like any non-flow; the metric then
    falls loud only if NO extract in the graph yields a flow window
    (:func:`_derive_period`), never a silent default.

    The span itself is deliberately NOT read here: ``span_days`` is a whole-column
    MIN/MAX, but the executed flow SUM is WHERE-filtered, so the window must be
    measured live over the filtered rows (:func:`_observe_window`). Only the flow/stock
    verdict, the axis identity, and its cadence come from the read surface.

    Runs inside the caller's SAVEPOINT (:func:`resolve_days_in_period`), so a read
    failure rolls back the nested transaction and degrades to the flagged default.
    """
    stmt = text(
        # DISTINCT: several measure columns can share one anchor axis (a SUM(a)+SUM(b)
        # extract), but the window is per (materialization, axis, cadence) — collapse
        # duplicates so the identical live window query isn't re-run once per column.
        f"SELECT DISTINCT m.materialization, m.anchor_time_axis, tp.detected_granularity "  # noqa: S608 - read_schema is an internal identifier
        f'FROM "{read_schema}".og_columns m '
        f'LEFT JOIN "{read_schema}".og_columns ax '
        f"  ON ax.table_id = m.table_id AND ax.column_name = m.anchor_time_axis "
        f'LEFT JOIN "{read_schema}".current_temporal_column_profiles tp '
        f"  ON tp.column_id = ax.column_id "
        f"WHERE m.table_id = :fact_id "
        f"  AND m.column_name IN :measure_cols "
        f"ORDER BY m.materialization, m.anchor_time_axis, tp.detected_granularity"  # deterministic evidence
    ).bindparams(bindparam("measure_cols", expanding=True))
    rows = session.execute(
        stmt, {"fact_id": fact_table_id_, "measure_cols": sorted(measure_cols)}
    ).all()
    return [
        _MeasureAxis(
            materialization=str(mat) if mat is not None else None,
            axis=str(axis) if axis is not None else None,
            grain=str(grain) if grain is not None else None,
        )
        for mat, axis, grain in rows
    ]
