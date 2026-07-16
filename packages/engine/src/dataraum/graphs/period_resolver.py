"""Derive a metric's ``days_in_period`` from the flow's observed data window (DAT-785).

The working-capital metrics (dpo/dso/dio/ccc) turn a stock/flow ratio into days by
multiplying it by ``days_in_period``: ``DPO = (AP / COGS) × days_in_period``. The
period is therefore **the window the FLOW is measured over** — ``COGS /
days_in_period`` is the daily purchase rate, and ``AP / daily_rate`` is the days of
purchases outstanding. So ``days_in_period`` must equal the number of days the flow
(COGS, revenue) was accumulated across, not a config constant.

**The flow is the income-statement side.** Income-statement items (revenue, COGS)
are flows by accounting definition — accumulated over a period; balance-sheet items
(AR, AP, inventory) are stocks — point-in-time. The flow extract identifies itself
structurally on the graph: ``source.statement == "income_statement"``. (Generalising
flow identification to the data-grounded ``materialization`` verdict is P7's job;
this ticket is P7's first concrete instance, not its node model.)

**The window is the full corpus span.** The flow extract sums its measure over the
ENTIRE relation with no time filter (``SELECT sum(cogs) FROM <rel>``), so the flow is
accumulated across the whole observed window. The period is that window's length,
read as ``span_days`` (``max_timestamp − min_timestamp``) off the flow fact's anchor
time axis — a single directly-observed data fact (DAT-783 temporal profile). Deriving
from ``detected_granularity`` (``monthly → 30``) would just re-hardcode the constant
behind a label — circular; the *observed* span is what makes it a data fact.

**Fencepost — a deliberate call.** ``span_days`` is exact for transaction-grained
flow data (the finance corpus's GL/journal lines, where min/max tightly track the
true window) and undercounts by ~one period for genuinely period-AGGREGATED flow
data (e.g. 12 month-end rows span ~334 days, not 365). The honest directly-observed
span is chosen over a fencepost correction (``actual_periods × mean_gap``) on
purpose: the correction assumes uniform periods and one-extra-period coverage —
assumptions that are themselves not always true — and "a plausible-but-wrong
derivation is worse than the honest constant." Whether to add the coarse-data
correction is a follow-up call, not a silent default.

**The axis has one home.** Which column is the flow's time axis is read from
``og_columns.anchor_time_axis`` (DAT-780 witness-precedence COALESCE), never
re-derived from ``time_columns`` here.

**Fall loud (K6 — absence must never resolve to a plausible default).** When no flow
axis span can be *observed* — the flow never grounded, its anchor axis is NULL (the
DAT-801 header-date facts serve NULL), it has no temporal profile, its span is
degenerate, or two flows disagree on a fact — the config default survives ONLY as a
declared fallback that is flagged in the answer (a verification flag on the executed
artifact), never a silent 30.

The read surface (``og_columns`` + ``current_temporal_column_profiles``) is
Postgres-only by construction (the SQLite test substrate has no read schema —
``read_views.materialize_read_schema`` is Postgres-guarded), so on any non-Postgres
bind the resolver returns ``None`` (no override): there is no data window to observe,
and production always runs on Postgres.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from dataraum.core.logging import get_logger
from dataraum.graphs.additivity import parse_aggregate_calls
from dataraum.graphs.additivity_resolver import fact_table_id, grounded_select
from dataraum.graphs.models import StepType
from dataraum.query.snippet_library import SnippetLibrary
from dataraum.server.workspace import schema_name_for
from dataraum.storage.read_views import read_schema_name_for

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.graphs.models import TransformationGraph

_log = get_logger(__name__)

_PARAMETER = "days_in_period"
_INCOME_STATEMENT = "income_statement"


@dataclass(frozen=True)
class PeriodResolution:
    """The ``days_in_period`` to inject for a metric, plus how it was decided.

    ``days`` is always the value to feed the constant step: the observed flow-axis
    span when it could be derived, else the graph's declared config default.
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
    ``days`` is the observed span, or the config default with a loud ``flag`` when
    the window cannot be observed.
    """
    default = _default_days(graph)
    if default is None:
        return None
    # No read surface to observe a data window from (SQLite test substrate). Not a
    # real abstention — there is nothing to derive from — so no override, no flag.
    if session.get_bind().dialect.name != "postgresql":
        return None

    flow_steps = [
        step
        for step in graph.steps.values()
        if step.step_type == StepType.EXTRACT
        and step.source is not None
        and step.source.statement == _INCOME_STATEMENT
    ]
    if not flow_steps:
        return _fallback(default, "no income-statement flow extract to observe a period from")

    library = SnippetLibrary(session, workspace_id=workspace_id)
    read_schema = read_schema_name_for(schema_name_for(workspace_id))
    # Every observed (span, axis) across every flow measure, NOT collapsed per fact —
    # ccc feeds ONE shared days_in_period constant into dso+dio-dpo, so revenue and
    # COGS (which usually share the income-statement fact but can carry different
    # per-measure anchor axes, hence different spans) must ALL agree on one window.
    # Keying by fact would silently let the later flow's span overwrite the earlier's.
    observations: list[tuple[float, str]] = []
    for step in flow_steps:
        field_name = step.source.standard_field if step.source else "?"
        resolved = grounded_select(library, workspace_id, step)
        if resolved is None:
            return _fallback(default, f"flow '{field_name}' did not ground")
        select_expr, relation = resolved
        fact_id = fact_table_id(session, relation)
        if fact_id is None:
            return _fallback(default, f"flow relation {relation!r} is outside the analysis")
        try:
            measure_cols = {
                col
                for call in parse_aggregate_calls(select_expr, duckdb_conn)
                for col in call.columns
            }
        except ValueError:
            return _fallback(default, f"flow select_expr for {relation!r} did not parse")
        if not measure_cols:
            return _fallback(default, f"flow on {relation!r} aggregates no column to anchor on")
        spans = _observe_axis_spans(session, read_schema, fact_id, measure_cols)
        if not spans:
            return _fallback(
                default,
                f"flow '{field_name}' has no observable anchor-axis span "
                f"(null anchor time axis or no temporal profile)",
            )
        observations.extend(spans)

    distinct = {round(span, 3) for span, _ in observations}
    if len(distinct) != 1:
        return _fallback(
            default, f"flows disagree on the period window (observed spans {sorted(distinct)} days)"
        )
    days = observations[0][0]
    if days <= 0:
        return _fallback(default, "observed flow span is 0 days (single-timestamp corpus)")

    return PeriodResolution(
        days=days,
        flag=None,
        evidence={
            "derived": True,
            "days": days,
            "anchor_time_axis": sorted({axis for _, axis in observations}),
            "flows": len(flow_steps),
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


def _observe_axis_spans(
    session: Session,
    read_schema: str,
    fact_table_id_: str,
    measure_cols: set[str],
) -> list[tuple[float, str]]:
    """Observed ``(span_days, axis_column)`` per flow measure's anchor time axis.

    Reads the DAT-780 anchor from ``og_columns.anchor_time_axis`` for each of the
    flow's measure column(s), self-joins to that axis column's own vertex to recover
    its ``column_id``, and joins the head-resolved ``current_temporal_column_profiles``
    for ``span_days``. Returns one row per measure column whose anchor axis resolves
    to a profile — EMPTY when the measure columns are absent, their anchor axis is
    NULL (a header-date fact, DAT-801), or the axis has no temporal profile. The
    caller requires the returned spans to agree; an empty list falls loud, never a
    plausible 30.

    The read is wrapped in a SAVEPOINT so a read-surface failure (e.g. views not
    materialised) rolls back only the nested transaction — it can never poison the
    outer assembly session that runs ``agent.assemble`` next on the same session.
    """
    stmt = text(
        f"SELECT tp.span_days, m.anchor_time_axis "  # noqa: S608 - read_schema is an internal identifier
        f'FROM "{read_schema}".og_columns m '
        f'JOIN "{read_schema}".og_columns ax '
        f"  ON ax.table_id = m.table_id AND ax.column_name = m.anchor_time_axis "
        f'JOIN "{read_schema}".current_temporal_column_profiles tp '
        f"  ON tp.column_id = ax.column_id "
        f"WHERE m.table_id = :fact_id "
        f"  AND m.column_name IN :measure_cols "
        f"  AND m.anchor_time_axis IS NOT NULL "
        f"ORDER BY m.column_name"  # deterministic order for the log evidence
    ).bindparams(bindparam("measure_cols", expanding=True))
    try:
        with session.begin_nested():
            rows = session.execute(
                stmt, {"fact_id": fact_table_id_, "measure_cols": sorted(measure_cols)}
            ).all()
    except SQLAlchemyError as exc:
        # Read surface unexpectedly unavailable — the savepoint rolled back, the
        # outer session is intact; fall loud to the flagged default at the call site
        # rather than failing (or silently defaulting) the metric.
        _log.warning("period_axis_read_failed", fact_table_id=fact_table_id_, error=str(exc))
        return []
    return [(float(span), str(axis)) for span, axis in rows]
