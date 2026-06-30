"""Unit tests for the topo-warm DAG builder (DAT-629 / DAT-646).

The pure layer: warm ONLY leaf EXTRACTs (the sole shared LLM surface), dedup each
to its concept key, and order into waves. FORMULA/CONSTANT are NOT warmed — they are
deterministic and metric-specific, composed per-metric in ``assemble`` (DAT-646), so
they never become warm nodes. No execution here — just the graph the warmer walks.
"""

from __future__ import annotations

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
        assert node_key(g.steps["e"], g) == ("extract", "revenue", "income_statement", "sum")

    def test_formula_is_not_keyed(self) -> None:
        """A FORMULA is never warmed (DAT-646) — keying it by shape aliased metrics."""
        g = _graph("m", {"f": _formula("f", "revenue - cogs", ["a", "b"])})
        assert node_key(g.steps["f"], g) is None

    def test_constant_is_not_keyed(self) -> None:
        """A CONSTANT is composed per-metric, not warmed (DAT-646)."""
        step = GraphStep(step_id="c", step_type=StepType.CONSTANT, parameter="days_in_period")
        g = _graph(
            "m",
            {"c": step},
            parameters=[ParameterDef(name="days_in_period", param_type="integer", default=365)],
        )
        assert node_key(step, g) is None

    def test_extract_without_source_is_unkeyable(self) -> None:
        step = GraphStep(step_id="e", step_type=StepType.EXTRACT, aggregation="sum")
        g = _graph("m", {"e": step})
        assert node_key(step, g) is None


class TestBuildWarmDag:
    def test_only_extracts_are_warmed(self) -> None:
        """A metric's formula steps are NOT nodes — only its leaf extracts (DAT-646)."""
        gross = _graph(
            "gross_margin",
            {
                "rev": _extract("rev", "revenue"),
                "cogs": _extract("cogs", "cost_of_goods_sold"),
                "gp": _formula("gp", "rev - cogs", ["rev", "cogs"]),
            },
        )
        _, nodes = build_warm_dag({"gross_margin": gross})
        assert set(nodes) == {
            ("extract", "revenue", "income_statement", "sum"),
            ("extract", "cost_of_goods_sold", "income_statement", "sum"),
        }
        assert all(k[0] == "extract" for k in nodes)

    def test_shared_extract_dedups_across_graphs(self) -> None:
        """Two metrics each extracting cost_of_goods_sold + revenue → deduped to one each."""
        gross = _graph(
            "gross_margin",
            {
                "rev": _extract("rev", "revenue"),
                "cogs": _extract("cogs", "cost_of_goods_sold"),
                "gp": _formula("gp", "rev - cogs", ["rev", "cogs"]),
            },
        )
        net = _graph(
            "net_income",
            {
                "rev2": _extract("rev2", "revenue"),
                "cogs2": _extract("cogs2", "cost_of_goods_sold"),
                "opex": _extract("opex", "operating_expense"),
                "ni": _formula("ni", "rev2 - cogs2 - opex", ["rev2", "cogs2", "opex"]),
            },
        )
        _, nodes = build_warm_dag({"gross_margin": gross, "net_income": net})
        # 3 distinct extract concepts, deduped across the two metrics; no formula nodes.
        assert set(nodes) == {
            ("extract", "revenue", "income_statement", "sum"),
            ("extract", "cost_of_goods_sold", "income_statement", "sum"),
            ("extract", "operating_expense", "income_statement", "sum"),
        }

    def test_extracts_are_leaves_no_edges_one_generation(self) -> None:
        """Extracts have no deps → the DAG is edgeless and warms in ONE wave."""
        gross = _graph(
            "gross_margin",
            {
                "rev": _extract("rev", "revenue"),
                "cogs": _extract("cogs", "cost_of_goods_sold"),
                "gp": _formula("gp", "rev - cogs", ["rev", "cogs"]),
            },
        )
        dag, _ = build_warm_dag({"gross_margin": gross})
        assert dag.number_of_edges() == 0
        gens = warming_generations(dag)
        assert len(gens) == 1
        assert set(gens[0]) == {
            ("extract", "revenue", "income_statement", "sum"),
            ("extract", "cost_of_goods_sold", "income_statement", "sum"),
        }

    def test_unkeyable_steps_are_skipped(self) -> None:
        g = _graph(
            "m",
            {
                "bad": GraphStep(step_id="bad", step_type=StepType.EXTRACT, aggregation="sum"),
                "good": _extract("good", "revenue"),
            },
        )
        _, nodes = build_warm_dag({"m": g})
        assert set(nodes) == {("extract", "revenue", "income_statement", "sum")}


class TestBuildMiniGraph:
    def test_extract_node_is_single_output_step(self) -> None:
        g = _graph("gross_margin", {"cogs": _extract("cogs", "cost_of_goods_sold")})
        _, nodes = build_warm_dag({"gross_margin": g})
        node = nodes[("extract", "cost_of_goods_sold", "income_statement", "sum")]

        mini = build_mini_graph(node)

        assert set(mini.steps) == {"cogs"}
        assert mini.steps["cogs"].output_step is True
        assert mini.get_output_step() is mini.steps["cogs"]

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
