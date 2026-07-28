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
    CLASSIFIED_REASONS,
    DISTINCT_COUNT,
    MIN_MAX,
    RATIO,
    SNAPSHOT_COUNT,
    STOCK,
    UNKNOWN_AGGREGATE,
    UNKNOWN_TEMPORAL,
    AbstainReason,
    AdditivityStatus,
    AggregateCall,
    AxisAdditivity,
    AxisClass,
    AxisKind,
    AxisVerdict,
    axis_additivity,
    classify_extract,
    parse_aggregate_calls,
    roll_up_metric,
    select_expr_is_ratio,
)
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
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


def test_parse_distinct_flag(con):
    assert parse_aggregate_calls("COUNT(DISTINCT customer_id)", con) == [
        AggregateCall("count", ("customer_id",), distinct=True)
    ]
    assert parse_aggregate_calls("SUM(DISTINCT amount)", con) == [
        AggregateCall("sum", ("amount",), distinct=True)
    ]
    assert parse_aggregate_calls("AVG(amount)", con) == [AggregateCall("avg", ("amount",))]


def test_parse_window_function_yields_no_calls(con):
    """A window aggregate is not an aggregate FUNCTION node — no calls (refused downstream, F3)."""
    assert parse_aggregate_calls("SUM(x) OVER ()", con) == []


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
    assert classify_extract(
        [AggregateCall("count", ("x",), distinct=True)], {}, False
    ) == AxisClass(False, False, DISTINCT_COUNT, DISTINCT_COUNT)
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


def test_rollup_without_output_step_is_refused():
    """A graph missing its output marker can't reveal ratio-vs-sum structure → refuse (F5)."""
    graph = _graph({"e": _extract("e")})  # no step marked output_step
    verdict = roll_up_metric(graph, {"e": ADDITIVE})
    assert verdict.categorical_additive is False
    assert verdict.time_additive is False
    assert verdict.time_reason == UNKNOWN_AGGREGATE


# --- 4. conservatism when a signal is missing (reviewer findings) ------------


def test_sum_of_unresolved_column_strips_time():
    """A column with no resolved temporal_behavior can't be confirmed a flow — strip time."""
    cls = classify_extract([AggregateCall("sum", ("mystery",))], {}, fact_is_snapshot=False)
    assert cls.categorical_additive is True
    assert cls.time_additive is False
    assert cls.time_reason == UNKNOWN_TEMPORAL


def test_count_on_unknown_grain_strips_time():
    """An unknown fact grain (no TableEntity) denies COUNT the time axis, not offers it."""
    cls = classify_extract([AggregateCall("count_star", ())], {}, fact_is_snapshot=None)
    assert cls.categorical_additive is True
    assert cls.time_additive is False
    assert cls.time_reason == UNKNOWN_TEMPORAL


def test_is_ratio_flag_overrides_to_non_additive():
    cls = classify_extract([AggregateCall("sum", ("credit",))], FLOW, False, is_ratio=True)
    assert cls == AxisClass(False, False, RATIO, RATIO)


def test_count_star_guard_does_not_taint_stock_reason():
    """The CASE COUNT(*)=0 guard alongside SUM(stock): time stripped for STOCK, not snapshot_count."""
    calls = [AggregateCall("count_star", ()), AggregateCall("sum", ("debit_balance",))]
    cls = classify_extract(calls, STOCKCOLS, fact_is_snapshot=True)
    assert cls.categorical_additive is True
    assert cls.time_additive is False
    assert cls.time_reason == STOCK  # the measure's reason, not the guard's


def test_count_star_alone_is_the_measure():
    """A COUNT(*) with no co-occurring measure IS the measure — snapshot rule applies."""
    cls = classify_extract([AggregateCall("count_star", ())], {}, fact_is_snapshot=True)
    assert cls.time_additive is False
    assert cls.time_reason == SNAPSHOT_COUNT


def test_sum_distinct_never_reconciles():
    """SUM(DISTINCT) is non-additive — the distinct set overlaps across slices (F1)."""
    cls = classify_extract([AggregateCall("sum", ("amount",), distinct=True)], FLOW, False)
    assert cls == AxisClass(False, False, DISTINCT_COUNT, DISTINCT_COUNT)


def test_count_1_guard_also_excluded():
    """A COUNT(1) NULL-guard (parses as count/no-columns) is dropped like COUNT(*) (F2)."""
    calls = [AggregateCall("count", ()), AggregateCall("sum", ("debit_balance",))]
    cls = classify_extract(calls, STOCKCOLS, fact_is_snapshot=True)
    assert cls.time_additive is False
    assert cls.time_reason == STOCK  # not tainted with snapshot_count by the guard


def test_no_aggregate_is_refused():
    """No aggregate call (bare passthrough / unparsed window) → refused, never additive (F3)."""
    assert classify_extract([], {}, False) == AxisClass(
        False, False, UNKNOWN_AGGREGATE, UNKNOWN_AGGREGATE
    )


# --- 5. intra-extract ratio detection (select_expr_is_ratio) -----------------


def test_ratio_detection_division_of_measures(con):
    assert select_expr_is_ratio("SUM(numerator) / SUM(denominator)", con) is True


def test_ratio_detection_product_of_measures(con):
    assert select_expr_is_ratio("SUM(a) * SUM(b)", con) is True


def test_ratio_detection_scaling_by_constant_is_not_ratio(con):
    assert select_expr_is_ratio("SUM(revenue) / 12", con) is False
    assert select_expr_is_ratio("SUM(revenue) * 1.1", con) is False


def test_ratio_detection_difference_is_not_ratio(con):
    assert select_expr_is_ratio("COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0)", con) is False


# --- 6. roll-up cycle guard --------------------------------------------------


def test_rollup_formula_cycle_is_refused():
    """A FORMULA referencing itself is refused, not recursed unbounded."""
    graph = _graph({"a": _formula("a", "a + 1", ["a"])})
    verdict = roll_up_metric(graph, {})
    assert verdict.categorical_additive is False
    assert verdict.time_additive is False
    assert verdict.time_reason == UNKNOWN_AGGREGATE


# --- 7. typed per-axis verdicts (DAT-857 / DAT-868) --------------------------
#
# The projection is TOTAL over the classifier: every branch above lands on
# exactly one of ADDITIVE / SEMI_ADDITIVE / NON_ADDITIVE_RECOMPUTE / ABSTAINED.


def test_flow_sum_is_additive_on_both_axes():
    cls = classify_extract([AggregateCall("sum", ("revenue",))], {"revenue": "additive"}, False)
    for kind in (AxisKind.TIME, AxisKind.CATEGORICAL):
        got = axis_additivity(cls, kind)
        assert got.status is AdditivityStatus.CLASSIFIED
        assert got.verdict is AxisVerdict.ADDITIVE
        assert got.reason is None


def test_stock_is_semi_additive_across_time_but_additive_across_categories():
    """The distinction the boolean model could not express: a balance sums across
    accounts, and per-period it is meaningful — only the SUM across periods is not."""
    cls = classify_extract([AggregateCall("sum", ("balance",))], {"balance": "point_in_time"}, None)
    time = axis_additivity(cls, AxisKind.TIME)
    assert time.status is AdditivityStatus.CLASSIFIED
    assert time.verdict is AxisVerdict.SEMI_ADDITIVE
    assert time.reason == STOCK
    assert axis_additivity(cls, AxisKind.CATEGORICAL).verdict is AxisVerdict.ADDITIVE


def test_snapshot_count_is_semi_additive_across_time():
    cls = classify_extract([AggregateCall("count_star", ())], {}, True)
    got = axis_additivity(cls, AxisKind.TIME)
    assert got.verdict is AxisVerdict.SEMI_ADDITIVE
    assert got.reason == SNAPSHOT_COUNT


def test_ratio_is_recompute_on_both_axes_not_a_refusal():
    """The pinned case: a ratio is RECOMPUTABLE per bucket, not un-bucketable."""
    cls = classify_extract([], {}, False, is_ratio=True)
    for kind in (AxisKind.TIME, AxisKind.CATEGORICAL):
        got = axis_additivity(cls, kind)
        assert got.status is AdditivityStatus.CLASSIFIED
        assert got.verdict is AxisVerdict.NON_ADDITIVE_RECOMPUTE
        assert got.reason == RATIO


def test_average_and_distinct_count_and_min_max_are_recompute():
    for call, reason in (
        (AggregateCall("avg", ("amount",)), AVERAGE),
        (AggregateCall("count", ("customer",), distinct=True), DISTINCT_COUNT),
        (AggregateCall("max", ("amount",)), MIN_MAX),
    ):
        cls = classify_extract([call], {}, False)
        got = axis_additivity(cls, AxisKind.TIME)
        assert got.verdict is AxisVerdict.NON_ADDITIVE_RECOMPUTE, call
        assert got.reason == reason


def test_unresolved_temporal_behaviour_ABSTAINS_rather_than_refusing():
    """'We can't tell' is not 'no' — it must be a typed abstention so the drill
    renders a reason instead of silently withholding."""
    cls = classify_extract([AggregateCall("sum", ("revenue",))], {}, False)
    got = axis_additivity(cls, AxisKind.TIME)
    assert got.status is AdditivityStatus.ABSTAINED
    assert got.abstain_reason is AbstainReason.UNKNOWN_TEMPORAL
    assert got.verdict is None and got.reason is None
    # ...while the categorical axis of the same extract is still a real verdict.
    assert axis_additivity(cls, AxisKind.CATEGORICAL).verdict is AxisVerdict.ADDITIVE


def test_unknown_aggregate_abstains():
    cls = classify_extract([AggregateCall("median", ("amount",))], {}, False)
    got = axis_additivity(cls, AxisKind.TIME)
    assert got.status is AdditivityStatus.ABSTAINED
    assert got.abstain_reason is AbstainReason.UNKNOWN_AGGREGATE


def test_metric_rollup_projects_the_same_way():
    """A ratio METRIC (the gross-margin shape) rolls up to recompute, not refusal."""
    graph = _graph(
        {
            "rev": _extract("rev"),
            "cogs": _extract("cogs"),
            "gm": _formula("gm", "(rev - cogs) / rev", ["rev", "cogs"]),
        }
    )
    flow = classify_extract([AggregateCall("sum", ("x",))], {"x": "additive"}, False)
    verdict = roll_up_metric(graph, {"rev": flow, "cogs": flow})
    got = axis_additivity(verdict, AxisKind.TIME)
    assert got.verdict is AxisVerdict.NON_ADDITIVE_RECOMPUTE
    assert got.reason == RATIO


def test_classified_reasons_vocabulary_excludes_the_unknowns():
    """The DB CHECK vocab: an `unknown_*` is an abstention, never a reason."""
    assert UNKNOWN_AGGREGATE not in CLASSIFIED_REASONS
    assert UNKNOWN_TEMPORAL not in CLASSIFIED_REASONS
    assert set(CLASSIFIED_REASONS) == {
        AVERAGE,
        DISTINCT_COUNT,
        MIN_MAX,
        RATIO,
        SNAPSHOT_COUNT,
        STOCK,
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": AdditivityStatus.CLASSIFIED},  # no verdict
        {"status": AdditivityStatus.CLASSIFIED, "verdict": AxisVerdict.ADDITIVE, "reason": STOCK},
        {
            "status": AdditivityStatus.CLASSIFIED,
            "verdict": AxisVerdict.SEMI_ADDITIVE,
            "reason": UNKNOWN_TEMPORAL,
        },
        {
            "status": AdditivityStatus.CLASSIFIED,
            "verdict": AxisVerdict.ADDITIVE,
            "abstain_reason": AbstainReason.MISSING_EXTRACT,
        },
        {"status": AdditivityStatus.ABSTAINED},  # no abstain_reason
        {
            "status": AdditivityStatus.ABSTAINED,
            "abstain_reason": AbstainReason.MISSING_EXTRACT,
            "verdict": AxisVerdict.ADDITIVE,
        },
    ],
)
def test_invalid_status_pairings_are_refused_at_the_chokepoint(kwargs):
    with pytest.raises(ValueError):
        AxisAdditivity(**kwargs)


class TestGroundedSelectCarriesTheDeclaredPredicate:
    """``grounded_select`` resolves a step to ITS snippet, not a sibling's (DAT-838).

    This is the shared grounding-resolution primitive: the additivity classifier
    and the period resolver both read the ``(select_expr, relation, where)`` it
    returns. Looked up without the declared ``predicate`` it did not come back
    empty — ``""`` MATCHED the unrestricted sibling — so a restricted step was
    classified over a row population it never reads, and the period resolver
    bound its reporting instant over that same wrong population. Silently, in
    both cases.
    """

    _RESTRICTION = "invoices that are overdue"

    def _step(self, predicate: str) -> GraphStep:
        return GraphStep(
            step_id="ar",
            step_type=StepType.EXTRACT,
            source=StepSource(
                standard_field="accounts_receivable",
                statement="balance_sheet",
                predicate=predicate,
            ),
            aggregation="sum",
        )

    def _save(self, session, *, predicate: str, relation: str, where: list[str]) -> None:
        from dataraum.query.snippet_library import SnippetLibrary

        SnippetLibrary(session, workspace_id="ws-pred").save_snippet(
            snippet_type="extract",
            sql=f"SELECT SUM(amount) AS value FROM {relation}",
            description="ar",
            schema_mapping_id="ws-pred",
            source="graph:ar",
            standard_field="accounts_receivable",
            statement="balance_sheet",
            aggregation="sum",
            predicate=predicate,
            parts={
                "select": [{"expr": "SUM(amount)", "alias": "value"}],
                "from": [relation],
                "where": where,
            },
        )
        session.flush()

    def test_restricted_step_resolves_its_own_rows_not_the_siblings(self, session) -> None:
        from dataraum.graphs.additivity_resolver import grounded_select
        from dataraum.query.snippet_library import SnippetLibrary

        self._save(session, predicate="", relation="ar_all", where=[])
        self._save(
            session,
            predicate=self._RESTRICTION,
            relation="ar_overdue",
            where=['"due_date" < CURRENT_DATE'],
        )

        resolved = grounded_select(
            SnippetLibrary(session), "ws-pred", self._step(self._RESTRICTION)
        )

        assert resolved is not None
        _expr, relation, where = resolved
        assert relation == "ar_overdue"
        assert where == ['"due_date" < CURRENT_DATE']

    def test_restricted_step_with_no_snippet_abstains(self, session) -> None:
        """No row of its own = nothing resolved. The sibling is not a fallback."""
        from dataraum.graphs.additivity_resolver import grounded_select
        from dataraum.query.snippet_library import SnippetLibrary

        self._save(session, predicate="", relation="ar_all", where=[])

        assert (
            grounded_select(SnippetLibrary(session), "ws-pred", self._step(self._RESTRICTION))
            is None
        )
        # ...while the unrestricted step still resolves normally.
        assert grounded_select(SnippetLibrary(session), "ws-pred", self._step("")) is not None
