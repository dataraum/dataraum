"""Unit tests for the topo-warm DAG builder (DAT-629).

The pure layer: dedup every metric step to its global cache key, wire edges from
``depends_on``, fail loud on a cycle, and order into dependency waves. No
execution here — just the graph the warmer will walk.
"""

from __future__ import annotations

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
from dataraum.graphs.node_warming import (
    WarmNode,
    build_mini_graph,
    build_warm_dag,
    node_key,
    warming_generations,
)


def _extract(step_id: str, standard_field: str, *, aggregation: str = "sum") -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field=standard_field, statement="income_statement"),
        aggregation=aggregation,
    )


def _formula(step_id: str, expression: str, depends_on: list[str]) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.FORMULA,
        expression=expression,
        depends_on=depends_on,
        output_step=True,
    )


def _graph(graph_id: str, steps: dict[str, GraphStep], **kw: object) -> TransformationGraph:
    return TransformationGraph(
        graph_id=graph_id,
        version="1.0",
        metadata=GraphMetadata(
            name=graph_id, description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
        **kw,  # type: ignore[arg-type]
    )


class TestNodeKey:
    def test_extract_key_mirrors_cache_key(self) -> None:
        g = _graph("m", {"e": _extract("e", "revenue", aggregation="sum")})
        assert node_key(g.steps["e"], g) == (
            "extract",
            "revenue",
            "income_statement",
            "sum",
        )

    def test_formula_key_is_normalized_expression(self) -> None:
        g = _graph("m", {"f": _formula("f", "revenue - cogs", ["a", "b"])})
        key = node_key(g.steps["f"], g)
        assert key is not None and key[0] == "formula"
        # Identical expressions in different graphs collapse to one node (field
        # names → placeholders, order-preserving for subtraction).
        g2 = _graph("m2", {"f": _formula("f", "revenue - cogs", ["a", "b"])})
        assert node_key(g2.steps["f"], g2) == key
        # ...but differing whitespace is a DIFFERENT key — normalize_expression
        # is whitespace-sensitive (DAT-629: formula-string consistency is a
        # separate validation concern, explicitly out of scope here).
        g3 = _graph("m3", {"f": _formula("f", "revenue  -  cogs", ["a", "b"])})
        assert node_key(g3.steps["f"], g3) != key

    def test_constant_key_uses_parameter_and_value(self) -> None:
        step = GraphStep(step_id="c", step_type=StepType.CONSTANT, parameter="days_in_period")
        g = _graph(
            "m",
            {"c": step},
            parameters=[ParameterDef(name="days_in_period", param_type="integer", default=365)],
        )
        assert node_key(step, g) == ("constant", "days_in_period", "365")

    def test_extract_without_source_is_unkeyable(self) -> None:
        step = GraphStep(step_id="e", step_type=StepType.EXTRACT, aggregation="sum")
        g = _graph("m", {"e": step})
        assert node_key(step, g) is None


class TestBuildWarmDag:
    def test_shared_extract_dedups_across_graphs(self) -> None:
        """Two metrics each extracting cost_of_goods_sold → ONE node."""
        gross = _graph(
            "gross_margin",
            {
                "rev": _extract("rev", "revenue"),
                "cogs": _extract("cogs", "cost_of_goods_sold"),
                "gp": _formula("gp", "revenue - cogs", ["rev", "cogs"]),
            },
        )
        net = _graph(
            "net_income",
            {
                "rev2": _extract("rev2", "revenue"),
                "cogs2": _extract("cogs2", "cost_of_goods_sold"),
                "opex": _extract("opex", "operating_expense"),
                "ni": _formula("ni", "revenue - cogs - opex", ["rev2", "cogs2", "opex"]),
            },
        )

        dag, nodes = build_warm_dag({"gross_margin": gross, "net_income": net})

        cogs_key = ("extract", "cost_of_goods_sold", "income_statement", "sum")
        rev_key = ("extract", "revenue", "income_statement", "sum")
        assert cogs_key in nodes
        assert rev_key in nodes
        # revenue + cogs + opex extracts + 2 distinct formula expressions = 5 nodes.
        extract_nodes = [k for k in nodes if k[0] == "extract"]
        assert len(extract_nodes) == 3  # rev, cogs, opex — deduped
        formula_nodes = [k for k in nodes if k[0] == "formula"]
        assert len(formula_nodes) == 2  # the two distinct expressions

    def test_edges_follow_depends_on(self) -> None:
        gross = _graph(
            "gross_margin",
            {
                "rev": _extract("rev", "revenue"),
                "cogs": _extract("cogs", "cost_of_goods_sold"),
                "gp": _formula("gp", "revenue - cogs", ["rev", "cogs"]),
            },
        )
        dag, _ = build_warm_dag({"gross_margin": gross})

        rev_key = ("extract", "revenue", "income_statement", "sum")
        cogs_key = ("extract", "cost_of_goods_sold", "income_statement", "sum")
        formula_keys = [k for k in dag.nodes if k[0] == "formula"]
        assert len(formula_keys) == 1
        gp_key = formula_keys[0]
        assert dag.has_edge(rev_key, gp_key)
        assert dag.has_edge(cogs_key, gp_key)

    def test_generations_order_extracts_before_formula(self) -> None:
        gross = _graph(
            "gross_margin",
            {
                "rev": _extract("rev", "revenue"),
                "cogs": _extract("cogs", "cost_of_goods_sold"),
                "gp": _formula("gp", "revenue - cogs", ["rev", "cogs"]),
            },
        )
        dag, _ = build_warm_dag({"gross_margin": gross})
        gens = warming_generations(dag)

        assert len(gens) == 2
        gen0 = set(gens[0])
        assert gen0 == {
            ("extract", "revenue", "income_statement", "sum"),
            ("extract", "cost_of_goods_sold", "income_statement", "sum"),
        }
        assert gens[1][0][0] == "formula"

    def test_cycle_is_fail_loud(self) -> None:
        # Two formulas depending on each other (pathological) → cycle.
        a = _formula("a", "x_one - y_two", ["b"])
        b = _formula("b", "y_two - x_one", ["a"])
        g = _graph("cyclic", {"a": a, "b": b})
        with pytest.raises(ValueError, match="cycle"):
            build_warm_dag({"cyclic": g})

    def test_unkeyable_steps_are_skipped(self) -> None:
        g = _graph(
            "m",
            {
                "bad": GraphStep(step_id="bad", step_type=StepType.EXTRACT, aggregation="sum"),
                "good": _extract("good", "revenue"),
            },
        )
        _, nodes = build_warm_dag({"m": g})
        assert len(nodes) == 1
        assert ("extract", "revenue", "income_statement", "sum") in nodes


class TestBuildMiniGraph:
    def test_extract_node_is_single_output_step(self) -> None:
        g = _graph("gross_margin", {"cogs": _extract("cogs", "cost_of_goods_sold")})
        _, nodes = build_warm_dag({"gross_margin": g})
        node = nodes[("extract", "cost_of_goods_sold", "income_statement", "sum")]

        mini = build_mini_graph(node)

        assert set(mini.steps) == {"cogs"}
        assert mini.steps["cogs"].output_step is True
        assert mini.get_output_step() is mini.steps["cogs"]

    def test_formula_node_includes_transitive_deps(self) -> None:
        g = _graph(
            "gross_margin",
            {
                "rev": _extract("rev", "revenue"),
                "cogs": _extract("cogs", "cost_of_goods_sold"),
                "gp": _formula("gp", "revenue - cogs", ["rev", "cogs"]),
            },
        )
        _, nodes = build_warm_dag({"gross_margin": g})
        formula_key = next(k for k in nodes if k[0] == "formula")

        mini = build_mini_graph(nodes[formula_key])

        # Formula + both dep extracts, with only the formula as output.
        assert set(mini.steps) == {"rev", "cogs", "gp"}
        assert mini.steps["gp"].output_step is True
        assert mini.steps["rev"].output_step is False
        assert mini.steps["cogs"].output_step is False
        # depends_on preserved so the agent assembles deps from the warm cache.
        assert set(mini.steps["gp"].depends_on) == {"rev", "cogs"}

    def test_originals_are_not_mutated(self) -> None:
        """The warmed node's output_step flip must not touch the real graph."""
        rev = _extract("rev", "revenue")  # output_step defaults to False
        g = _graph("gross_margin", {"rev": rev})
        _, nodes = build_warm_dag({"gross_margin": g})
        node = nodes[("extract", "revenue", "income_statement", "sum")]

        mini = build_mini_graph(node)

        assert mini.steps["rev"].output_step is True
        assert rev.output_step is False  # original untouched
        assert g.steps["rev"].output_step is False
