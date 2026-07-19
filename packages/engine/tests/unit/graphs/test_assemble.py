"""Unit tests for GraphAgent.assemble — the no-LLM per-metric path (DAT-636).

The authoring pass decides every node once and records it in the binding map;
assemble composes a metric from those decisions and NEVER re-authors. The key
guarantee under test: a metric with an ungroundable dependency honest-fails
immediately (naming the dep), with no LLM call — so the same concept can no
longer ground three different ways across dependent metrics.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dataraum.graphs.agent import ExecutionContext, GraphAgent
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
from dataraum.graphs.node_warming import NodeDecision, node_key


def _extract(step_id: str, standard_field: str) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field=standard_field, statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )


def _graph(graph_id: str, steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id=graph_id,
        version="1.0",
        metadata=GraphMetadata(
            name=graph_id, description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


def _bare_agent(provider: MagicMock) -> GraphAgent:
    """A GraphAgent with only what assemble's no-LLM path touches."""
    agent = GraphAgent.__new__(GraphAgent)
    agent.provider = provider  # type: ignore[attr-defined]
    return agent


def test_assemble_honest_fails_on_ungroundable_dep_without_llm() -> None:
    cogs = _extract("cost_of_goods_sold", "cost_of_goods_sold")
    graph = _graph("gross_profit", {"cost_of_goods_sold": cogs})
    bindings = {
        node_key(cogs, graph): NodeDecision(
            grounded=False, reason="no support: filter matched no rows"
        )
    }
    provider = MagicMock()
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws")

    result = _bare_agent(provider).assemble(MagicMock(), graph, ctx, bindings, workspace_id="ws")

    assert result.success is False
    assert "cost_of_goods_sold" in (result.error or "")
    assert "ungroundable" in (result.error or "")
    assert "no support" in (result.error or "")
    # Born-loud BEFORE any execution — pure assembly never calls the LLM.
    provider.converse.assert_not_called()


def test_assemble_honest_fails_on_empty_binding_map() -> None:
    """An empty binding map (cyclic/empty authoring pass) → every keyable dep is
    'not authored' → honest-fail born-loud at the dependency loop, no LLM."""
    rev = _extract("revenue", "revenue")
    graph = _graph("revenue_only", {"revenue": rev})
    provider = MagicMock()
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws")

    result = _bare_agent(provider).assemble(MagicMock(), graph, ctx, {}, workspace_id="ws")

    assert result.success is False
    assert "revenue" in (result.error or "")
    assert "not authored" in (result.error or "")
    provider.converse.assert_not_called()


def test_assemble_fails_when_grounded_but_absent_from_cache() -> None:
    """A node grounded per the map but missing from the cache is an internal
    inconsistency — honest-fail, never silently re-authored."""
    rev = _extract("revenue", "revenue")
    graph = _graph("revenue_only", {"revenue": rev})
    bindings = {node_key(rev, graph): NodeDecision(grounded=True)}
    provider = MagicMock()
    agent = _bare_agent(provider)
    # No snippet in the (mocked, empty) cache.
    agent._lookup_snippets = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._resolve_parameters = MagicMock(return_value={})  # type: ignore[method-assign]
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws")

    result = agent.assemble(MagicMock(), graph, ctx, bindings, workspace_id="ws")

    assert result.success is False
    assert "absent from the snippet cache" in (result.error or "")
    provider.converse.assert_not_called()


class TestSaveSnippetsCallerAssignedIds:
    """Step ids in GeneratedCode are assigned by OUR code since DAT-603 —
    authoring binds the graph's leaf id, compose copies graph ids — so
    _save_snippets' name-keyed lookup always hits. (The DAT-664 positional
    rebind for model-paraphrased ids was deleted with the single-extract output
    model; the model no longer names steps at all.) A mismatch can now only
    mean an internal regression, which must skip LOUD, never silently."""

    def _generated(self, step_id: str) -> MagicMock:
        code = MagicMock()
        code.steps = [
            {
                "step_id": step_id,
                "sql": "SELECT 1 AS value",
                "description": "d",
                "parts": {
                    "select": [{"expr": "1", "alias": "value"}],
                    "from": [],
                    "where": [],
                },
            }
        ]
        code.summary = "s"
        code.llm_model = "claude-test"
        code.provenance = None
        code.assumptions = []
        return code

    def _save(self, graph, code) -> MagicMock:
        agent = GraphAgent.__new__(GraphAgent)
        with patch("dataraum.query.snippet_library.SnippetLibrary") as lib_cls:
            GraphAgent._save_snippets(
                agent,
                session=MagicMock(),
                graph=graph,
                generated_code=code,
                schema_mapping_id="ws",
                workspace_id="ws",
            )
            return lib_cls.return_value

    def test_leaf_id_saves(self) -> None:
        rev = _extract("revenue", "revenue")
        graph = _graph("revenue_only", {"revenue": rev})
        library = self._save(graph, self._generated("revenue"))
        library.save_snippet.assert_called_once()
        assert library.save_snippet.call_args.kwargs["standard_field"] == "revenue"

    def test_mismatched_id_skips_loud_never_saves_wrong_sql(self) -> None:
        rev = _extract("revenue", "revenue")
        graph = _graph("revenue_only", {"revenue": rev})
        library = self._save(graph, self._generated("someone_elses_id"))
        library.save_snippet.assert_not_called()


def _leaf(step_id: str) -> GraphStep:
    """A NON-output extract leaf — real metric graphs have exactly one output."""
    return GraphStep(
        step_id=step_id,
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field=step_id, statement="income_statement"),
        aggregation="sum",
    )


def test_assemble_executes_the_groundable_subgraph_and_reports_per_step() -> None:
    """DAT-699: an ungroundable dep no longer aborts the whole graph silently.
    The groundable extracts EXECUTE (real values in the reason), the holes are
    named with their reasons, and downstream formulas read as blocked — the
    per-step story the drill-down renders. The metric itself still fails."""
    import duckdb

    revenue = _leaf("revenue")
    cogs = _leaf("cost_of_goods_sold")
    gross = GraphStep(
        step_id="gross_profit",
        step_type=StepType.FORMULA,
        expression="revenue - cost_of_goods_sold",
        depends_on=["revenue", "cost_of_goods_sold"],
        output_step=True,
    )
    graph = _graph(
        "gross_profit",
        {"revenue": revenue, "cost_of_goods_sold": cogs, "gross_profit": gross},
    )
    bindings = {
        node_key(revenue, graph): NodeDecision(grounded=True),
        node_key(cogs, graph): NodeDecision(
            grounded=False, reason="no support: it aggregated to NULL"
        ),
    }
    provider = MagicMock()
    agent = _bare_agent(provider)
    config = MagicMock()
    config.features.sql_repair = None  # default repair attempts (int), never a mock
    agent.config = config  # type: ignore[attr-defined]
    agent._lookup_snippets = MagicMock(  # type: ignore[method-assign]
        return_value={"revenue": {"sql": "SELECT 5925920163.0 AS value"}}
    )
    agent._resolve_parameters = MagicMock(return_value={})  # type: ignore[method-assign]
    ctx = ExecutionContext(duckdb_conn=duckdb.connect(), schema_mapping_id="ws")

    result = agent.assemble(MagicMock(), graph, ctx, bindings, workspace_id="ws")

    assert result.success is False
    error = result.error or ""
    # The hole, named with its honest reason:
    assert "dependency 'cost_of_goods_sold' is ungroundable" in error
    assert "no support" in error
    # The groundable extract EXECUTED — its measured value is in the reason:
    assert "revenue = 5,925,920,163.00 ✓" in error
    # The formula downstream of the hole is blocked, not silently absent:
    assert "gross_profit blocked (needs cost_of_goods_sold)" in error
    # Still no LLM anywhere in assembly.
    provider.converse.assert_not_called()


def test_assemble_partial_report_names_every_hole_not_just_the_first() -> None:
    """Two ungroundable deps → both named (the old path aborted on the first)."""
    revenue = _leaf("revenue")
    cogs = _leaf("cost_of_goods_sold")
    graph = _graph("both_dead", {"revenue": revenue, "cost_of_goods_sold": cogs})
    bindings = {
        node_key(revenue, graph): NodeDecision(grounded=False, reason="reason-a"),
        node_key(cogs, graph): NodeDecision(grounded=False, reason="reason-b"),
    }
    provider = MagicMock()
    agent = _bare_agent(provider)
    agent._lookup_snippets = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._resolve_parameters = MagicMock(return_value={})  # type: ignore[method-assign]
    ctx = ExecutionContext(duckdb_conn=MagicMock(), schema_mapping_id="ws")

    result = agent.assemble(MagicMock(), graph, ctx, bindings, workspace_id="ws")

    assert result.success is False
    error = result.error or ""
    assert "'cost_of_goods_sold', 'revenue' are ungroundable" in error
    assert "reason-a" in error and "reason-b" in error
    provider.converse.assert_not_called()


def test_assemble_executes_and_flags_a_violated_declared_expectation() -> None:
    """DAT-699: a declared expectation is a 'should', not a gate — the metric
    EXECUTES and the violation rides execution.verification_flags to the
    artifact's state_reason (the amber pattern). The old gate refused the
    number ('composed but not executed: declared validation failed')."""
    import duckdb

    from dataraum.graphs.models import StepValidation

    cogs = GraphStep(
        step_id="cost_of_goods_sold",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="cost_of_goods_sold", statement="income_statement"),
        aggregation="sum",
        output_step=True,
        validations=[StepValidation(condition="value >= 0", message="COGS should not be negative")],
    )
    graph = _graph("cogs_only", {"cost_of_goods_sold": cogs})
    bindings = {node_key(cogs, graph): NodeDecision(grounded=True)}
    provider = MagicMock()
    agent = _bare_agent(provider)
    config = MagicMock()
    config.features.sql_repair = None
    agent.config = config  # type: ignore[attr-defined]
    agent._lookup_snippets = MagicMock(  # type: ignore[method-assign]
        return_value={"cost_of_goods_sold": {"sql": "SELECT -4200000.0 AS value"}}
    )
    agent._resolve_parameters = MagicMock(return_value={})  # type: ignore[method-assign]
    agent._track_snippet_usage = MagicMock()  # type: ignore[method-assign]
    agent._save_composed_snippets = MagicMock()  # type: ignore[method-assign]
    ctx = ExecutionContext(duckdb_conn=duckdb.connect(), schema_mapping_id="ws")

    result = agent.assemble(MagicMock(), graph, ctx, bindings, workspace_id="ws")

    assert result.success  # the number is never refused
    execution = result.unwrap()
    assert execution.output_value == -4200000.0
    assert len(execution.verification_flags) == 1
    assert "declared expectation not met" in execution.verification_flags[0]
    assert "COGS should not be negative" in execution.verification_flags[0]
    provider.converse.assert_not_called()


def test_cached_off_vocabulary_assumption_basis_degrades_never_wedges() -> None:
    """Contract-v2 tightened GraphAssumptionOutput.basis to the AssumptionBasis
    enum, but rows written BEFORE the cut persisted the model's raw string.
    The cache-read reconstruction must coerce (warn + INFERRED), never raise —
    a ValidationError here would wedge a HEALTHY snippet forever (first-writer-
    wins keeps the row; the crash happens before any failed-save could flag it)."""
    from dataraum.graphs.models import AssumptionBasis

    revenue = _extract("revenue", "revenue")
    graph = _graph("revenue", {"revenue": revenue})
    cached = {
        "revenue": {
            "sql": "SELECT 1 AS value",
            "description": "revenue",
            "snippet_id": "s1",
            "assumptions": [
                {
                    "dimension": "semantic.units",
                    "target": "column:t.amount",
                    "assumption": "currency is EUR",
                    "basis": "vibes",  # pre-enum row: off-vocabulary raw string
                    "confidence": 0.4,
                }
            ],
        }
    }
    agent = _bare_agent(MagicMock())

    code = agent._compose_metric_from_dag(graph, cached, {})

    assert code is not None
    assert [a.basis for a in code.assumptions] == [AssumptionBasis.INFERRED]
    assert code.assumptions[0].confidence == 0.4  # the signal itself is kept
