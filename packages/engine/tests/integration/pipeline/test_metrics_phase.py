"""Tests for the metrics phase — the third lifecycle family (DAT-456).

Mirrors the cycles/validation lifecycle tests: the LLM machinery (the graph
agent's compose+execute call) is mocked at its boundary; everything else — the
declared-set load, the parse gate, lifecycle artifacts, supersession — runs
against the real session fixture. There is NO field-mapping pre-gate: every
parseable metric is handed to the agent, which is mocked here to model the two
real outcomes (executes cleanly → executed / cannot materialize runnable SQL →
grounded with the reason). Born-loud lives at the agent, not in a heuristic skip.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.models.base import Result
from dataraum.lifecycle import ArtifactState, LifecycleArtifact
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import metrics_phase as gep
from dataraum.pipeline.phases.metrics_phase import MetricsPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb

_SESSION_ID = "sess-metrics-phase"
_WORKSPACE_ID = "ws-metrics"


@pytest.fixture()
def _mock_llm():
    """Patch LLM infrastructure so the phase can initialize without config."""
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}

    with (
        patch(
            "dataraum.pipeline.phases.metrics_phase.load_llm_config",
            return_value=mock_config,
        ),
        patch(
            "dataraum.pipeline.phases.metrics_phase.create_provider",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.metrics_phase.PromptRenderer",
            return_value=MagicMock(),
        ),
    ):
        yield


@pytest.fixture
def workspace_table(session: Session) -> Table:
    """A typed table with a column."""
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="invoices",
        layer="typed",
        duckdb_path="typed_invoices",
        row_count=10,
    )
    session.add(table)
    session.flush()
    session.add(
        Column(
            table_id=table.table_id,
            column_name="amount",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="DOUBLE",
        )
    )
    session.commit()
    return table


def _make_ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str = "run-om-1",
    config: dict[str, Any] | None = None,
) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        run_id=run_id,
        config=config
        if config is not None
        else {"vertical": "finance", "base_runs": {}, "workspace_id": _WORKSPACE_ID},
    )


def _metric_def(graph_id: str, field: str = "accounts_receivable") -> dict[str, Any]:
    """A minimal, parseable transformation-graph definition with one extract field."""
    return {
        "graph_id": graph_id,
        "metadata": {"name": graph_id.upper(), "category": "test"},
        "output": {"type": "scalar"},
        "dependencies": {
            "extract_step": {"type": "extract", "source": {"standard_field": field}},
        },
    }


def _artifacts(session: Session, run_id: str) -> dict[str, LifecycleArtifact]:
    rows = (
        session.execute(select(LifecycleArtifact).where(LifecycleArtifact.run_id == run_id))
        .scalars()
        .all()
    )
    return {a.artifact_key: a for a in rows}


class TestMetricsPhaseOutcomes:
    def test_fails_without_table_ids(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, table_ids=[]))
        assert result.status == PhaseStatus.FAILED
        assert "No tables in session scope" in (result.error or "")

    def test_no_vertical_is_loud_explicit_outcome(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, workspace_table: Table
    ) -> None:
        ctx = _make_ctx(session, duckdb_conn, [workspace_table.table_id], config={})
        result = MetricsPhase()._run(ctx)
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_vertical"
        assert _artifacts(session, "run-om-1") == {}

    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_no_declared_metrics_is_loud_explicit_outcome(
        self,
        mock_defs: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        mock_defs.return_value = {}
        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_declared_metrics"

    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_missing_base_runs_pin_fails_loud(
        self,
        mock_defs: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        """No per-phase head resolution (ADR-0008): an unthreaded pin is a wiring bug."""
        mock_defs.return_value = {"dso": _metric_def("dso")}
        ctx = _make_ctx(
            session,
            duckdb_conn,
            [workspace_table.table_id],
            config={"vertical": "finance", "workspace_id": _WORKSPACE_ID},  # no base_runs
        )
        result = MetricsPhase()._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "base_runs missing" in (result.error or "")

    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_missing_workspace_id_fails_loud(
        self,
        mock_defs: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        """The snippet base needs a workspace-stable schema_mapping_id, threaded by the activity."""
        mock_defs.return_value = {"dso": _metric_def("dso")}
        ctx = _make_ctx(
            session,
            duckdb_conn,
            [workspace_table.table_id],
            config={"vertical": "finance", "base_runs": {}},  # no workspace_id
        )
        result = MetricsPhase()._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "workspace_id missing" in (result.error or "")


def _fake_warm():
    """GraphAgent.execute stand-in for the AUTHORING pass: every node grounds.

    The per-metric outcome is modelled separately on ``assemble`` (DAT-636); the
    authoring pass just needs to ground the nodes. Patched on the class → called
    unbound, no ``self``.
    """

    def _execute(session, graph, context, *args, inspiration_sql=None, workspace_id="", **kw):  # noqa: ANN001
        execution = MagicMock()
        execution.assumptions = []
        return Result.ok(execution)

    return _execute


def _fake_assemble(fail_ids: set[str]):
    """GraphAgent.assemble stand-in: a metric composes cleanly unless its graph_id
    is in fail_ids (modelling an ungroundable dependency / failed composition).

    A clean execution carries assumptions (empty here = plainly executed, no
    low-confidence flag — DAT-631); a bare MagicMock reads truthy but iterates
    empty, so set it explicitly. Patched on the class → called unbound, no ``self``.
    """

    def _assemble(session, graph, context, bindings, parameters=None, *, workspace_id=""):  # noqa: ANN001
        if graph.graph_id in fail_ids:
            return Result.fail("SQL execution failed against the workspace")
        execution = MagicMock()
        execution.assumptions = []
        return Result.ok(execution)

    return _assemble


class TestMetricLifecycleFlow:
    @patch("dataraum.graphs.agent.GraphAgent.assemble")
    @patch("dataraum.graphs.agent.ExecutionContext.with_rich_context")
    @patch("dataraum.graphs.agent.GraphAgent.execute")
    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_declare_compose_execute_flow(
        self,
        mock_defs: MagicMock,
        mock_execute: MagicMock,
        mock_ctx: MagicMock,
        mock_assemble: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """Every parseable metric is composed by the agent: one assembles cleanly,
        one cannot materialize runnable SQL and stays grounded with the reason.
        No pre-gate — the agent's outcome decides, not a field-mapping check."""
        mock_defs.return_value = {
            "m_exec": _metric_def("m_exec", field="accounts_receivable"),
            "m_unexec": _metric_def("m_unexec", field="revenue"),
        }
        mock_execute.side_effect = _fake_warm()
        mock_assemble.side_effect = _fake_assemble(fail_ids={"m_unexec"})
        mock_ctx.return_value = MagicMock()

        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        artifacts = _artifacts(session, "run-om-1")

        # Executed: composed + ran cleanly.
        assert artifacts["m_exec"].state == ArtifactState.EXECUTED.value
        assert artifacts["m_exec"].grounded_against is not None
        assert artifacts["m_exec"].teaches["vertical"] == "finance"
        # Composed but unexecutable: the agent could not materialize runnable SQL,
        # so it stays grounded with the reason — born-loud at the agent.
        assert artifacts["m_unexec"].state == ArtifactState.GROUNDED.value
        assert "execution failed" in (artifacts["m_unexec"].state_reason or "")

        assert result.outputs["executed"] == 1
        assert result.outputs["stuck_grounded"] == 1
        # No metric stays declared via a heuristic gate — declared is parse-only.
        assert result.outputs["stuck_declared"] == 0

    @patch("dataraum.graphs.agent.GraphAgent.assemble")
    @patch("dataraum.graphs.agent.ExecutionContext.with_rich_context")
    @patch("dataraum.graphs.agent.GraphAgent.execute")
    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_malformed_definition_stays_declared(
        self,
        mock_defs: MagicMock,
        mock_execute: MagicMock,
        mock_ctx: MagicMock,
        mock_assemble: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """A definition that won't parse stays declared with the parse error — the
        one legitimate pre-gate — never dropped."""
        mock_defs.return_value = {
            "ok_metric": _metric_def("ok_metric", field="accounts_receivable"),
            "broken": {"graph_id": "broken", "output": {"type": "scalar"}},  # missing metadata.name
        }
        mock_execute.side_effect = _fake_warm()
        mock_assemble.side_effect = _fake_assemble(fail_ids=set())
        mock_ctx.return_value = MagicMock()

        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        artifacts = _artifacts(session, "run-om-1")
        assert artifacts["ok_metric"].state == ArtifactState.EXECUTED.value
        assert artifacts["broken"].state == ArtifactState.DECLARED.value
        assert "malformed" in (artifacts["broken"].state_reason or "")

    @patch("dataraum.graphs.agent.GraphAgent.assemble")
    @patch("dataraum.graphs.agent.ExecutionContext.with_rich_context")
    @patch("dataraum.graphs.agent.GraphAgent.execute")
    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_rerun_supersedes_under_fresh_run_id(
        self,
        mock_defs: MagicMock,
        mock_execute: MagicMock,
        mock_ctx: MagicMock,
        mock_assemble: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """No skip-if-already-ran: the re-run re-flows everything; runs coexist."""
        mock_defs.return_value = {"dso": _metric_def("dso", field="accounts_receivable")}
        mock_execute.side_effect = _fake_warm()
        mock_assemble.side_effect = _fake_assemble(fail_ids=set())
        mock_ctx.return_value = MagicMock()

        phase = MetricsPhase()
        r1 = phase._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id], "run-1"))
        session.flush()
        r2 = phase._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id], "run-2"))
        session.flush()

        assert r1.status == r2.status == PhaseStatus.COMPLETED
        session.expire_all()
        artifacts = session.execute(select(LifecycleArtifact)).scalars().all()
        assert {(a.run_id, a.state) for a in artifacts} == {
            ("run-1", "executed"),
            ("run-2", "executed"),
        }


# ---------------------------------------------------------------------------
# DAT-629: warming primes the cache so the per-metric fan-out is race-free
# ---------------------------------------------------------------------------

from dataraum.graphs.agent import ExecutionContext, GeneratedCode, GraphAgent  # noqa: E402
from dataraum.graphs.models import (  # noqa: E402
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    TransformationGraph,
)


def _wm_extract(step_id: str, standard_field: str) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field=standard_field, statement="income_statement"),
        aggregation="sum",
    )


def _wm_formula(step_id: str, expression: str, depends_on: list[str]) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.FORMULA,
        expression=expression,
        depends_on=depends_on,
        output_step=True,
    )


def _wm_graph(graph_id: str, steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id=graph_id,
        version="1.0",
        metadata=GraphMetadata(
            name=graph_id, description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


class _WarmStubCtx:
    """PhaseContext stand-in: manager=None forces the serial warm path."""

    def __init__(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        self.manager = None
        self.session = session
        self.duckdb_conn = duckdb_conn


def _fake_generate(authored: list[str]):
    """Stand in for GraphAgent._generate_sql: author every step as a trivial,
    runnable SELECT, recording each authoring so we can prove the per-metric
    fan-out re-authors nothing once the cache is warm. The point under test is
    the warm→cache→assemble plumbing, not arithmetic — every step yields a
    supported (non-NULL) value, so the verifier passes.
    """

    def _gen(session, graph, context, parameters, cached_snippets=None):  # noqa: ANN001
        authored.append(graph.graph_id)
        steps = [
            {"step_id": sid, "sql": "SELECT 1 AS value", "description": sid} for sid in graph.steps
        ]
        out = graph.get_output_step()
        if out and out.step_type == StepType.FORMULA and out.depends_on:
            # A formula's final_sql composes over its DEP steps (never the output step
            # itself). The snippet is now saved FROM final_sql (DAT-636 round-trip), so a
            # self-referential `SELECT value FROM <out>` would fold to an invalid CTE in
            # assembly — mirror real composition: reference the dependency CTEs.
            terms = " + ".join(f"(SELECT value FROM {d})" for d in out.depends_on)
            final = f"SELECT {terms} AS value"
        elif out:
            final = f"SELECT value FROM {out.step_id}"
        else:
            final = "SELECT 1 AS value"
        return Result.ok(
            GeneratedCode(
                code_id=str(uuid4()),
                graph_id=graph.graph_id,
                summary="fake",
                steps=steps,
                final_sql=final,
                column_mappings={},
                llm_model="fake",
                prompt_hash="x",
                generated_at=datetime.now(UTC),
            )
        )

    return _gen


class TestWarmingPrimesCache:
    def test_shared_node_authored_once_both_metrics_assemble_from_warm_cache(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The DAT-629 fix end-to-end: warming authors each unique node once;
        the per-metric execute then assembles every step from the warm cache
        with no further authoring — so a shared extract (cost_of_goods_sold)
        cannot diverge or ground to an empty filter across siblings."""
        from dataraum.query.snippet_models import SQLSnippetRecord

        # gross_profit and net_income both extract cost_of_goods_sold + revenue;
        # net_income adds operating_expense. Five unique nodes total.
        gross = _wm_graph(
            "gross_profit",
            {
                "rev": _wm_extract("rev", "revenue"),
                "cogs": _wm_extract("cogs", "cost_of_goods_sold"),
                # A formula's operands ARE its dependency step_ids (the deterministic
                # composer references each as `(SELECT value FROM <step_id>)`) — exactly
                # as real metric graphs author them (gross_profit.yaml: `revenue -
                # cost_of_goods_sold` over deps named revenue/cost_of_goods_sold).
                "gp": _wm_formula("gp", "rev - cogs", ["rev", "cogs"]),
            },
        )
        net = _wm_graph(
            "net_income",
            {
                "rev2": _wm_extract("rev2", "revenue"),
                "cogs2": _wm_extract("cogs2", "cost_of_goods_sold"),
                "opex": _wm_extract("opex", "operating_expense"),
                "ni": _wm_formula("ni", "rev2 - cogs2 - opex", ["rev2", "cogs2", "opex"]),
            },
        )

        agent = GraphAgent(config=MagicMock(), provider=MagicMock(), prompt_renderer=MagicMock())
        authored: list[str] = []
        # Patch the LLM authoring boundary on the instance.
        agent._generate_sql = _fake_generate(authored)  # type: ignore[method-assign]
        # The serial warm + manual execute both build their context through
        # with_rich_context; give them a minimal context over the real cursor.
        monkeypatch.setattr(
            ExecutionContext,
            "with_rich_context",
            classmethod(
                lambda cls, **kw: ExecutionContext(
                    duckdb_conn=duckdb_conn, schema_mapping_id=kw["schema_mapping_id"]
                )
            ),
        )

        ctx = _WarmStubCtx(session, duckdb_conn)
        bindings = gep._warm_shared_nodes(
            {"gross_profit": gross, "net_income": net},
            ctx,  # type: ignore[arg-type]
            agent,
            "ws-warm",
            ["t1"],
            "finance",
            om_run_id="run-warm",
        )
        session.flush()
        assert len(authored) > 0, "the authoring pass should author the unique nodes"
        # Every unique node was decided ONCE and is grounded in the binding map.
        assert bindings and all(d.grounded for d in bindings.values())

        # The shared cost_of_goods_sold extract is cached exactly once.
        cogs = [
            s
            for s in session.execute(select(SQLSnippetRecord)).scalars().all()
            if s.snippet_type == "extract" and s.standard_field == "cost_of_goods_sold"
        ]
        assert len(cogs) == 1

        # The per-metric path is now pure ASSEMBLY (DAT-636): both metrics compose
        # from the binding map + warm cache, NEITHER re-authors anything.
        authored.clear()
        exec_ctx = ExecutionContext(duckdb_conn=duckdb_conn, schema_mapping_id="ws-warm")
        r_gross = agent.assemble(session, gross, exec_ctx, bindings, workspace_id="ws-warm")
        r_net = agent.assemble(session, net, exec_ctx, bindings, workspace_id="ws-warm")

        assert r_gross.success and r_net.success
        assert authored == [], "assembly must make the per-metric fan-out LLM-free"
