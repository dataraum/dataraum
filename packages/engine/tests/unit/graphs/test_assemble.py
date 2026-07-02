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


class TestSaveSnippetsStepIdRebind:
    """_save_snippets must tolerate the model renaming its single step
    (Sonnet 5 echoes `revenue` back as `revenue_extract` — 2026-07-02): the
    authoring path grounds exactly ONE extract leaf (DAT-646), so an
    unambiguous one-leaf/one-step pair binds positionally. Without the rebind
    the snippet silently never persists, assembly finds an empty cache, and
    every metric composed from the leaf dies 'absent from the snippet cache'."""

    def _generated(self, step_id: str) -> MagicMock:
        code = MagicMock()
        code.steps = [{"step_id": step_id, "sql": "SELECT 1", "description": "d"}]
        code.summary = "s"
        code.column_mappings = {}
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
                step_results=None,
                workspace_id="ws",
            )
            return lib_cls.return_value

    def test_renamed_single_step_is_rebound_and_saved(self) -> None:
        rev = _extract("revenue", "revenue")
        graph = _graph("revenue_only", {"revenue": rev})
        library = self._save(graph, self._generated("revenue_extract"))
        library.save_snippet.assert_called_once()
        assert library.save_snippet.call_args.kwargs["standard_field"] == "revenue"

    def test_exact_echo_still_saves(self) -> None:
        rev = _extract("revenue", "revenue")
        graph = _graph("revenue_only", {"revenue": rev})
        library = self._save(graph, self._generated("revenue"))
        library.save_snippet.assert_called_once()

    def test_ambiguous_multi_step_drift_skips_loud(self) -> None:
        # Two generated steps for one leaf: no unambiguous positional bind —
        # nothing saved (the loud warning path), never a wrong-SQL snippet.
        rev = _extract("revenue", "revenue")
        graph = _graph("revenue_only", {"revenue": rev})
        code = self._generated("revenue_extract")
        code.steps = [
            {"step_id": "a", "sql": "SELECT 1", "description": ""},
            {"step_id": "b", "sql": "SELECT 2", "description": ""},
        ]
        library = self._save(graph, code)
        library.save_snippet.assert_not_called()
