"""``days_in_period`` derivation — the guards that hold on any substrate (DAT-785).

The real derivation runs a live WHERE-filtered window query in DuckDB over the flow's
grounded relation and reads the axis/cadence off the Postgres read surface
(``og_columns`` + ``current_temporal_column_profiles``); it is exercised in
``tests/integration/graphs/test_period_resolver.py``. These unit cases pin the pieces
that are substrate-independent: the "no parameter → nothing to derive" and "no read
surface → no override" short-circuits, the fallback string shape, the default read off
the graph, and the pure fencepost correction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

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
from dataraum.graphs.period_resolver import (
    PeriodResolution,
    _apply_fencepost,
    _AxisWindow,
    _default_days,
    _fallback,
    _MeasureAxis,
    _window_from_profile,
    resolve_days_in_period,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session


def _dpo_graph(*, with_period_param: bool = True) -> TransformationGraph:
    """A ratio-of-days-shaped graph: a stock operand, a flow operand, a period
    constant and the ``(stock / flow) × days`` formula. Deliberately vertical-neutral
    — nothing declares a finance ``statement`` (the resolver identifies the flow by the
    resolved ``materialization`` verdict, never a statement field)."""
    steps = {
        "accounts_payable": GraphStep(
            step_id="accounts_payable",
            step_type=StepType.EXTRACT,
            source=StepSource(standard_field="accounts_payable"),
            aggregation="sum",
        ),
        "cost_of_goods_sold": GraphStep(
            step_id="cost_of_goods_sold",
            step_type=StepType.EXTRACT,
            source=StepSource(standard_field="cost_of_goods_sold"),
            aggregation="sum",
        ),
        "days_in_period": GraphStep(
            step_id="days_in_period", step_type=StepType.CONSTANT, parameter="days_in_period"
        ),
        "dpo": GraphStep(
            step_id="dpo",
            step_type=StepType.FORMULA,
            expression="(accounts_payable / cost_of_goods_sold) * days_in_period",
            depends_on=["accounts_payable", "cost_of_goods_sold", "days_in_period"],
            output_step=True,
        ),
    }
    parameters = (
        [ParameterDef(name="days_in_period", param_type="integer", default=30)]
        if with_period_param
        else []
    )
    return TransformationGraph(
        graph_id="dpo",
        version="1",
        metadata=GraphMetadata(
            name="dpo", description="", category="working_capital", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR, metric_id="dpo", unit="days"),
        steps=steps,
        parameters=parameters,
    )


def test_no_days_in_period_parameter_returns_none(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """A metric without the parameter has nothing to derive — no override, no flag."""
    graph = _dpo_graph(with_period_param=False)
    assert resolve_days_in_period(session, duckdb_conn, graph=graph, workspace_id="test") is None


def test_non_postgres_bind_returns_none(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    """The SQLite test substrate has no read surface to observe a window from, so
    the resolver injects no override (the graph default stands) rather than
    fabricating a derivation."""
    graph = _dpo_graph()
    assert session.get_bind().dialect.name == "sqlite"
    assert resolve_days_in_period(session, duckdb_conn, graph=graph, workspace_id="test") is None


def test_default_days_reads_the_declared_default() -> None:
    assert _default_days(_dpo_graph()) == 30.0
    assert _default_days(_dpo_graph(with_period_param=False)) is None


def test_fallback_keeps_the_default_but_flags_it_loudly() -> None:
    resolution = _fallback(
        30.0, "no flow operand to observe a period from (no measure resolved to 'flow')"
    )
    assert resolution.days == 30.0
    assert resolution.derived is False
    assert resolution.flag is not None
    # The flag names the config default AND why it fell back — never a silent 30.
    assert "config default 30" in resolution.flag
    assert "no flow operand" in resolution.flag


def test_derived_resolution_carries_no_flag() -> None:
    resolution = PeriodResolution(days=273.0, flag=None, evidence={"derived": True})
    assert resolution.derived is True
    assert resolution.flag is None


def test_fencepost_corrects_period_aggregated_span() -> None:
    """12 month-end rows span ~334 days between endpoints but cover ~365 — the
    fencepost extrapolates the missing 12th period: 334 × 12/11 ≈ 364.4."""
    corrected = _apply_fencepost(334.0, 12)
    assert corrected == pytest.approx(334.0 * 12 / 11)
    assert corrected == pytest.approx(364.36, abs=0.1)


def test_fencepost_is_negligible_for_transaction_grained_span() -> None:
    """Daily transaction data: many periods → factor ≈ 1, so the corrected window
    barely moves off the raw span (364 × 365/364 = 365)."""
    corrected = _apply_fencepost(364.0, 365)
    assert corrected == pytest.approx(365.0)
    # Proportionally negligible (~0.3%), unlike the ~9% bump on 12-period aggregated
    # data — the correction self-scales with the number of periods.
    assert (corrected - 364.0) / 364.0 < 0.01


def test_fencepost_refuses_a_single_period() -> None:
    """One period gives no inter-period gap to correct against — fall loud, not a
    fabricated window."""
    assert _apply_fencepost(0.0, 1) is None
    assert _apply_fencepost(100.0, 1) is None


def test_fencepost_refuses_zero_or_missing_periods() -> None:
    assert _apply_fencepost(100.0, 0) is None
    assert _apply_fencepost(100.0, None) is None


def test_fencepost_refuses_a_degenerate_span() -> None:
    """A non-positive span (single-timestamp corpus) can't be corrected."""
    assert _apply_fencepost(0.0, 4) is None
    assert _apply_fencepost(-5.0, 4) is None


def _axis(**kw: object) -> _MeasureAxis:
    base = {
        "materialization": "flow",
        "axis": "period_date",
        "grain": "month",
        "persisted_span_days": 334.0,
        "persisted_actual_periods": 12,
    }
    base.update(kw)
    return _MeasureAxis(**base)  # type: ignore[arg-type]


def test_window_from_profile_collapses_to_the_persisted_span() -> None:
    """DAT-812: an UNFILTERED flow's window is the persisted whole-column span,
    fencepost-corrected exactly as the live empty-WHERE scan would — no DuckDB scan."""
    window = _window_from_profile(_axis())
    assert isinstance(window, _AxisWindow)
    assert window.corrected_days == pytest.approx(_apply_fencepost(334.0, 12))
    assert window.axis == "period_date"
    assert window.actual_periods == 12


def test_window_from_profile_falls_loud_on_unbucketable_grain() -> None:
    """An irregular/unknown cadence has no clean period — a reason string, not a window."""
    out = _window_from_profile(_axis(grain="irregular"))
    assert isinstance(out, str) and "no clean period" in out


def test_window_from_profile_falls_loud_on_single_period() -> None:
    """One period gives no gap to fencepost-correct — fall loud, never a fabricated window."""
    out = _window_from_profile(_axis(persisted_actual_periods=1))
    assert isinstance(out, str) and "single-period" in out


def test_window_from_profile_falls_loud_without_a_persisted_profile() -> None:
    """Defensive: a missing persisted span yields a reason (the caller falls back to live)."""
    out = _window_from_profile(_axis(persisted_span_days=None))
    assert isinstance(out, str)
