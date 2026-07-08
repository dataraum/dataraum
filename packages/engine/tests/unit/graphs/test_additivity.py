"""Additivity verdict logic (DAT-716).

Parser cases use the REAL grounded ``select_expr`` shapes from the finance
workspace (multi-column signed measures, the ``CASE WHEN COUNT(*)=0`` NULL
guard); classification and roll-up assert the doctrine on representative
metric DAGs.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.graphs.additivity import (
    ADDITIVE,
    AVERAGE,
    DISTINCT_COUNT,
    MIN_MAX,
    RATIO,
    SNAPSHOT_COUNT,
    STOCK,
    AggregateCall,
    AxisClass,
    classify_extract,
    parse_aggregate_calls,
    roll_up_metric,
)
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepType,
    TransformationGraph,
)


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


# --- 1. parse_aggregate_calls over real select_expr shapes -------------------


def test_parse_signed_flow_measure(con):
    """revenue: the CASE COUNT(*) guard + two signed SUMs — all three calls, columns intact."""
    expr = "CASE WHEN COUNT(*) = 0 THEN NULL ELSE COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0) END"
    calls = parse_aggregate_calls(expr, con)
    assert AggregateCall("count_star", ()) in calls
    assert AggregateCall("sum", ("credit",)) in calls
    assert AggregateCall("sum", ("debit",)) in calls
    assert len(calls) == 3


def test_parse_stock_measure(con):
    """current_assets: two SUMs over trial-balance stock columns."""
    calls = parse_aggregate_calls("SUM(debit_balance) - SUM(credit_balance)", con)
    assert set(calls) == {
        AggregateCall("sum", ("debit_balance",)),
        AggregateCall("sum", ("credit_balance",)),
    }


def test_parse_count_distinct_and_avg(con):
    assert parse_aggregate_calls("COUNT(DISTINCT customer_id)", con) == [
        AggregateCall("count_distinct", ("customer_id",))
    ]
    assert parse_aggregate_calls("AVG(amount)", con) == [AggregateCall("avg", ("amount",))]


def test_parse_ignores_non_aggregate_functions(con):
    """COALESCE and arithmetic operators serialize as FUNCTION nodes too — they must not be counted."""
    calls = parse_aggregate_calls("COALESCE(SUM(x), 0) + ABS(y)", con)
    assert calls == [AggregateCall("sum", ("x",))]


def test_parse_multi_column_aggregate(con):
    """An aggregate over an expression collects every base column it touches."""
    calls = parse_aggregate_calls("SUM(a - b)", con)
    assert calls == [AggregateCall("sum", ("a", "b"))]


# --- 2. classify_extract: function symmetry x temporal x snapshot ------------

FLOW = {"credit": "additive", "debit": "additive"}
STOCKCOLS = {"debit_balance": "point_in_time", "credit_balance": "point_in_time"}


def test_sum_flow_is_fully_additive():
    cls = classify_extract([AggregateCall("sum", ("credit",))], FLOW, fact_is_snapshot=False)
    assert cls == AxisClass(True, True)


def test_sum_stock_strips_time_only():
    cls = classify_extract(
        [AggregateCall("sum", ("debit_balance",))], STOCKCOLS, fact_is_snapshot=True
    )
    assert cls.categorical_additive is True
    assert cls.time_additive is False
    assert cls.time_reason == STOCK


def test_count_star_additive_on_event_fact():
    assert classify_extract(
        [AggregateCall("count_star", ())], {}, fact_is_snapshot=False
    ) == AxisClass(True, True)


def test_count_star_strips_time_on_snapshot_fact():
    cls = classify_extract([AggregateCall("count_star", ())], {}, fact_is_snapshot=True)
    assert cls.categorical_additive is True
    assert cls.time_additive is False
    assert cls.time_reason == SNAPSHOT_COUNT


def test_avg_and_distinct_and_minmax_never_reconcile():
    assert classify_extract([AggregateCall("avg", ("x",))], {}, False) == AxisClass(
        False, False, AVERAGE, AVERAGE
    )
    assert classify_extract([AggregateCall("count_distinct", ("x",))], {}, False) == AxisClass(
        False, False, DISTINCT_COUNT, DISTINCT_COUNT
    )
    assert classify_extract([AggregateCall("min", ("x",))], {}, False) == AxisClass(
        False, False, MIN_MAX, MIN_MAX
    )


def test_signed_flow_extract_is_additive():
    """revenue's three calls (count_star + two flow SUMs) on an event fact → fully additive."""
    calls = [
        AggregateCall("count_star", ()),
        AggregateCall("sum", ("credit",)),
        AggregateCall("sum", ("debit",)),
    ]
    assert classify_extract(calls, FLOW, fact_is_snapshot=False) == AxisClass(True, True)


def test_mixed_flow_and_stock_takes_most_restrictive():
    """A SUM touching a stock column strips time even alongside a flow SUM."""
    calls = [AggregateCall("sum", ("credit",)), AggregateCall("sum", ("debit_balance",))]
    cls = classify_extract(calls, {**FLOW, **STOCKCOLS}, fact_is_snapshot=True)
    assert cls.categorical_additive is True
    assert cls.time_additive is False
    assert cls.time_reason == STOCK


# --- 3. roll_up_metric over the DAG ------------------------------------------


def _graph(steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id="g",
        version="1",
        metadata=GraphMetadata(name="m", description="", category="c", source=GraphSource.SYSTEM),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


def _extract(step_id: str) -> GraphStep:
    return GraphStep(step_id=step_id, step_type=StepType.EXTRACT)


def _formula(step_id: str, expr: str, deps: list[str], *, output: bool = True) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.FORMULA,
        expression=expr,
        depends_on=deps,
        output_step=output,
    )


def test_rollup_difference_of_flows_is_additive():
    """gross_profit = revenue - cost_of_goods_sold."""
    graph = _graph(
        {
            "revenue": _extract("revenue"),
            "cost_of_goods_sold": _extract("cost_of_goods_sold"),
            "gp": _formula("gp", "revenue - cost_of_goods_sold", ["revenue", "cost_of_goods_sold"]),
        }
    )
    verdict = roll_up_metric(graph, {"revenue": ADDITIVE, "cost_of_goods_sold": ADDITIVE})
    assert verdict.categorical_additive is True
    assert verdict.time_additive is True


def test_rollup_ratio_is_non_additive_everywhere():
    """current_ratio = current_assets / current_liabilities — non-additive whatever the operands."""
    graph = _graph(
        {
            "current_assets": _extract("current_assets"),
            "current_liabilities": _extract("current_liabilities"),
            "cr": _formula(
                "cr",
                "current_assets / current_liabilities",
                ["current_assets", "current_liabilities"],
            ),
        }
    )
    # Even though the operands are only semi-additive, the ratio verdict is non-additive on BOTH axes.
    stock = AxisClass(True, False, None, STOCK)
    verdict = roll_up_metric(graph, {"current_assets": stock, "current_liabilities": stock})
    assert verdict.categorical_additive is False
    assert verdict.time_additive is False
    assert verdict.time_reason == RATIO


def test_rollup_ratio_times_constant_stays_non_additive():
    """dso = (accounts_receivable / revenue) * days_in_period — the constant scale doesn't rescue it."""
    graph = _graph(
        {
            "accounts_receivable": _extract("accounts_receivable"),
            "revenue": _extract("revenue"),
            "days_in_period": GraphStep(
                step_id="days_in_period", step_type=StepType.CONSTANT, value=365
            ),
            "dso": _formula(
                "dso",
                "(accounts_receivable / revenue) * days_in_period",
                ["accounts_receivable", "revenue", "days_in_period"],
            ),
        }
    )
    verdict = roll_up_metric(
        graph, {"accounts_receivable": AxisClass(True, False, None, STOCK), "revenue": ADDITIVE}
    )
    assert verdict.categorical_additive is False
    assert verdict.time_additive is False


def test_rollup_scale_by_constant_preserves_additivity():
    """A measure scaled by a literal stays additive."""
    graph = _graph(
        {"revenue": _extract("revenue"), "scaled": _formula("scaled", "revenue * 1.1", ["revenue"])}
    )
    verdict = roll_up_metric(graph, {"revenue": ADDITIVE})
    assert verdict.categorical_additive is True
    assert verdict.time_additive is True


def test_rollup_sum_of_flow_and_stock_is_semi_additive():
    """A stock operand in an additive combination strips time but keeps categorical."""
    graph = _graph(
        {
            "flow": _extract("flow"),
            "stock": _extract("stock"),
            "total": _formula("total", "flow + stock", ["flow", "stock"]),
        }
    )
    verdict = roll_up_metric(
        graph, {"flow": ADDITIVE, "stock": AxisClass(True, False, None, STOCK)}
    )
    assert verdict.categorical_additive is True
    assert verdict.time_additive is False
    assert verdict.time_reason == STOCK
