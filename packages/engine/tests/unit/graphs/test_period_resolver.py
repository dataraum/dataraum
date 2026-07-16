"""``days_in_period`` derivation — the guards that hold on any substrate (DAT-785).

The real derivation reads the Postgres read surface (``og_columns`` +
``current_temporal_column_profiles``) and is exercised in
``tests/integration/graphs/test_period_resolver.py``. These unit cases pin the
pieces that are substrate-independent: the "no parameter → nothing to derive" and
"no read surface → no override" short-circuits, the fallback string shape, and the
default read off the graph.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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
    _default_days,
    _fallback,
    resolve_days_in_period,
)

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session


def _dpo_graph(*, with_period_param: bool = True) -> TransformationGraph:
    """A dpo-shaped graph: a balance-sheet stock, an income-statement flow, a
    period constant and the ratio × days formula."""
    steps = {
        "accounts_payable": GraphStep(
            step_id="accounts_payable",
            step_type=StepType.EXTRACT,
            source=StepSource(standard_field="accounts_payable", statement="balance_sheet"),
            aggregation="sum",
        ),
        "cost_of_goods_sold": GraphStep(
            step_id="cost_of_goods_sold",
            step_type=StepType.EXTRACT,
            source=StepSource(standard_field="cost_of_goods_sold", statement="income_statement"),
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
    resolution = _fallback(30.0, "no income-statement flow extract to observe a period from")
    assert resolution.days == 30.0
    assert resolution.derived is False
    assert resolution.flag is not None
    # The flag names the config default AND why it fell back — never a silent 30.
    assert "config default 30" in resolution.flag
    assert "no income-statement flow extract" in resolution.flag


def test_derived_resolution_carries_no_flag() -> None:
    resolution = PeriodResolution(days=273.0, flag=None, evidence={"derived": True})
    assert resolution.derived is True
    assert resolution.flag is None
