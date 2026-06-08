"""Metrics phase — the operating_model stage's third lifecycle family (DAT-456).

Source-free and session-scoped, mirroring validation and cycles: operates on
``ctx.table_ids`` (the session's typed tables), never a ``source_id``. The
declared set is the vertical's ``metrics/`` transformation graphs ⊕ ``metric``
overlay teach rows — one ``metric`` lifecycle artifact per ``graph_id``. The
engine induces nothing (declares come from the vertical; user declares arrive
via frame-2 teach rows). Each declared metric flows through the typed artifact
lifecycle:

* **declare** — every loaded ``graph_id`` becomes a ``declared`` artifact.
* **compose** (``metric.compose``) — the metric grounds when its graph's inputs
  resolve to real columns/concepts of the workspace (the ``can_execute_metric``
  field-mapping gate). A metric whose required ``standard_field`` references are
  unmapped STAYS ``declared`` with the reason on the row — visibly impossible,
  never a silent best-effort LLM guess at a number. A definition that won't even
  parse stays ``declared`` with the parse error recorded.
* **execute** (``metric.execute``) — the graph agent composes the metric SQL and
  runs it cleanly → ``executed``; the working SQL is saved as reusable snippets
  (the durable, cross-run executable knowledge ``query`` later consumes). A
  composed metric whose SQL fails to run stays ``grounded`` with the reason —
  composed but not executable, never reported as executed.

A re-run supersedes: everything is re-declared and re-flowed under the fresh
``run_id`` (no skip-if-already-ran — the prior run's artifacts coexist untouched,
and the promoted head names the current run). The snippet base is NOT
run-versioned — it is the cross-run reuse cache shared with ``query``; a re-run
reuses healthy snippets and self-heals failed ones. With no vertical or no
declared metrics the phase succeeds loudly with an explicit outcome.

Per-metric LLM calls are independent — dispatched concurrently via
asyncio.to_thread + gather when the phase context exposes a ConnectionManager
(each parallel call gets its own SQLAlchemy session + DuckDB cursor). Falls back
to a serial loop in unit tests where the manager isn't wired.
"""

from __future__ import annotations

import asyncio
from types import ModuleType
from typing import TYPE_CHECKING

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.graphs.loader import GraphLoadError
from dataraum.lifecycle import BaseRunMap, declare_artifact, transition
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

_log = get_logger(__name__)

# The journey stage this phase runs under — the lifecycle guard authorizes
# metric.declare/compose/execute for this stage only.
_STAGE = "operating_model"

# Cap concurrent metric LLM calls. Sonnet 4.6 tier-3+ workspaces handle
# 4000 RPM (~67 RPS) comfortably; with ~30-60s LLM latencies, 10 concurrent
# is ~10 RPS at peak — well under the limit.
_MAX_CONCURRENT_METRICS = 10

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager
    from dataraum.graphs.agent import ExecutionContext as _ExecutionContext
    from dataraum.graphs.agent import GraphAgent
    from dataraum.graphs.models import GraphExecution, TransformationGraph
    from dataraum.lifecycle import LifecycleArtifact

    MetricPrep = tuple[str, TransformationGraph, str | None, str | None]
    MetricResult = tuple[str, Result[GraphExecution], str | None]


@analysis_phase
class MetricsPhase(BasePhase):
    """Compute metric graphs through the artifact lifecycle (DAT-456).

    Declares the vertical's metric graphs, composes each against the workspace
    (grounding its inputs), and executes the composed SQL — each declared metric
    moving through declare → compose → execute. Mirrors :class:`ValidationPhase`
    and :class:`BusinessCyclesPhase`.

    Requires: a begin_session workspace (typed tables, relationships, enriched
    views, slices) reachable through the run's pinned base heads.
    """

    @property
    def name(self) -> str:
        return "metrics"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.lifecycle import db_models as lifecycle_db_models
        from dataraum.query import snippet_models

        return [snippet_models, lifecycle_db_models]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Declare → compose → execute every declared metric graph."""
        from dataraum.graphs.agent import ExecutionContext, GraphAgent
        from dataraum.graphs.config import get_metric_definitions
        from dataraum.graphs.field_mapping import can_execute_metric, load_semantic_mappings
        from dataraum.graphs.loader import GraphLoader
        from dataraum.query.snippet_library import SnippetLibrary

        table_ids = ctx.table_ids
        if not table_ids:
            return PhaseResult.failed(
                "No tables in session scope — metric computation operates on the "
                "session's typed table selection (ctx.table_ids)."
            )

        # Declared set: the vertical's metric graphs ⊕ metric overlay teach rows.
        # No vertical / no declared metrics is a LOUD explicit outcome, not a
        # silent skip (the engine induces nothing now).
        vertical: str | None = ctx.config.get("vertical")
        declared_defs = get_metric_definitions(vertical) if vertical else {}
        if not vertical or not declared_defs:
            outcome = "no_vertical" if not vertical else "no_declared_metrics"
            _log.warning("metrics_nothing_declared", vertical=vertical, outcome=outcome)
            return PhaseResult.success(
                outputs={"outcome": outcome, "declared": 0, "executed": 0},
                records_processed=0,
                records_created=0,
                summary=f"0 declared metrics ({outcome}) — nothing to compose or execute",
            )

        session_id = ctx.require_session_id()
        run_id = ctx.require_run_id()
        # Pinned upstream heads (ADR-0008 in-run mode): resolved ONCE by the
        # workflow's pre-flight ``operating_model_resolve`` activity and threaded
        # here through the phase config. No per-phase head resolution — a missing
        # pin is a wiring bug, fail loud.
        raw_base_runs = ctx.config.get("base_runs")
        if raw_base_runs is None:
            return PhaseResult.failed(
                "base_runs missing from the phase config — OperatingModelWorkflow's "
                "resolve activity pins the base-run map before this phase runs "
                "(ADR-0008 in-run mode; no per-phase head resolution)."
            )
        base_runs = BaseRunMap.model_validate(raw_base_runs)

        # The snippet base is keyed by the WORKSPACE (source-free): snippets are
        # the cross-run reuse cache shared with the query agent, stable across
        # sessions/runs of one workspace's schema. Threaded into the phase config
        # by the run_metrics activity from the run identity.
        schema_mapping_id = ctx.config.get("workspace_id")
        if not schema_mapping_id:
            return PhaseResult.failed(
                "workspace_id missing from the phase config — the run_metrics "
                "activity threads it as the snippet base's schema_mapping_id "
                "(source-free, workspace-stable for cross-run reuse)."
            )

        # Initialize LLM infrastructure
        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        renderer = PromptRenderer()
        agent = GraphAgent(config=config, provider=provider, prompt_renderer=renderer)
        snippet_library = SnippetLibrary(ctx.session, session_id=session_id)

        # declare: every declared graph_id becomes a declared artifact for THIS
        # run — supersession across runs, UNIQUE identity within one.
        artifacts: dict[str, LifecycleArtifact] = {}
        for graph_id, defn in declared_defs.items():
            artifact = declare_artifact(
                session_id=session_id,
                artifact_type="metric",
                artifact_key=graph_id,
                run_id=run_id,
                stage=_STAGE,
                teaches={
                    "graph_id": graph_id,
                    "vertical": vertical,
                    "category": (defn.get("metadata") or {}).get("category"),
                },
            )
            ctx.session.add(artifact)
            artifacts[graph_id] = artifact

        # Parse declared definitions into graphs. A definition that won't parse
        # stays declared with the parse error recorded — visibly impossible.
        loader = GraphLoader(vertical=vertical)
        graphs: dict[str, TransformationGraph] = {}
        for graph_id, defn in declared_defs.items():
            try:
                graphs[graph_id] = loader.graphs_from_definitions({graph_id: defn})[graph_id]
            except GraphLoadError as e:
                artifacts[graph_id].state_reason = f"malformed metric definition: {e.message}"
                _log.warning("metric_definition_malformed", graph_id=graph_id, error=e.message)

        # compose: a metric grounds when its required field mappings resolve. The
        # gate is fail-loud — an unmappable required input leaves the artifact
        # declared with the reason, NEVER a best-effort LLM execution over a
        # missing input (the silent-wrong-number path is gone). Composed metrics
        # are queued for execution.
        field_mappings = load_semantic_mappings(ctx.session, table_ids)
        grounded_against = base_runs.model_dump(mode="json")
        prep: list[MetricPrep] = []
        for graph_id, graph in graphs.items():
            required_fields = [
                step.source.standard_field
                for step in graph.steps.values()
                if step.source and step.source.standard_field
            ]
            _, missing = can_execute_metric(field_mappings, required_fields)
            if missing:
                artifacts[graph_id].state_reason = (
                    "ungroundable: required field mappings missing in this "
                    f"workspace ({', '.join(sorted(missing))})"
                )
                _log.info("metric_ungroundable", graph_id=graph_id, missing=missing)
                continue

            transition(
                artifacts[graph_id],
                operation="compose",
                stage=_STAGE,
                grounded_against=grounded_against,
            )

            hint_sql: str | None = None
            inspiration_id = graph.metadata.inspiration_snippet_id
            if inspiration_id:
                hint_snippet = snippet_library.find_by_id(inspiration_id)
                if hint_snippet:
                    hint_sql = hint_snippet.sql
            prep.append((graph_id, graph, hint_sql, inspiration_id))

        # execute: run each composed metric. Parallel when the manager is wired,
        # serial fallback otherwise.
        if ctx.manager is not None:
            results = _execute_metrics_parallel(
                prep, ctx.manager, agent, schema_mapping_id, table_ids, session_id=session_id
            )
        else:
            exec_ctx = ExecutionContext.with_rich_context(
                session=ctx.session,
                duckdb_conn=ctx.duckdb_conn,
                table_ids=table_ids,
                schema_mapping_id=schema_mapping_id,
                session_id=session_id,
            )
            results = _execute_metrics_serial(
                prep, ctx.session, exec_ctx, agent, session_id=session_id
            )

        # A composed metric that ran cleanly reaches executed; one whose SQL
        # failed stays grounded with the reason (born loud, never silently absent).
        for graph_id, result, inspiration_id in results:
            artifact = artifacts[graph_id]
            if result.success:
                transition(artifact, operation="execute", stage=_STAGE)
                _log.info("metric_executed", graph_id=graph_id)
                # Snippet promotion: drop the ad-hoc snippet once the metric it
                # inspired executes cleanly.
                if inspiration_id:
                    ad_hoc = snippet_library.find_by_id(inspiration_id)
                    if ad_hoc:
                        ctx.session.delete(ad_hoc)
                        _log.info("snippet_promoted", graph_id=graph_id, snippet_id=inspiration_id)
            else:
                artifact.state_reason = f"composed but execution failed: {result.error}"
                _log.warning("metric_execution_failed", graph_id=graph_id, error=result.error)

        executed = sum(1 for a in artifacts.values() if a.state == "executed")
        grounded_stuck = sum(1 for a in artifacts.values() if a.state == "grounded")
        declared_stuck = sum(1 for a in artifacts.values() if a.state == "declared")

        previews = [
            f"{graph_id} executed" for graph_id, a in artifacts.items() if a.state == "executed"
        ]

        return PhaseResult.success(
            outputs={
                "declared": len(artifacts),
                "executed": executed,
                "stuck_grounded": grounded_stuck,
                "stuck_declared": declared_stuck,
            },
            records_processed=len(table_ids),
            records_created=len(artifacts),
            warnings=previews,
            summary=(
                f"{executed}/{len(artifacts)} metrics executed; "
                f"{declared_stuck} ungroundable, {grounded_stuck} composed but unexecutable"
            ),
        )


# ---------------------------------------------------------------------------
# Per-metric dispatch
# ---------------------------------------------------------------------------


def _execute_metrics_serial(
    prep: list[MetricPrep],
    session: Session,
    exec_ctx: _ExecutionContext,
    agent: GraphAgent,
    *,
    session_id: str,
) -> list[MetricResult]:
    """Fallback path: shared session + cursor, sequential dispatch.

    Used in unit tests where PhaseContext.manager is None.
    """
    out: list[MetricResult] = []
    for graph_id, graph, hint_sql, inspiration_id in prep:
        result = agent.execute(
            session, graph, exec_ctx, inspiration_sql=hint_sql, session_id=session_id
        )
        out.append((graph_id, result, inspiration_id))
    return out


def _execute_metrics_parallel(
    prep: list[MetricPrep],
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    *,
    session_id: str,
) -> list[MetricResult]:
    """Concurrent path: per-call session + cursor, gathered via asyncio.

    Each metric runs `agent.execute` on a thread with its own SQLAlchemy
    session (auto-commit via session_scope) and its own DuckDB cursor.
    A semaphore caps in-flight LLM calls to _MAX_CONCURRENT_METRICS.
    """

    async def _run_all() -> list[MetricResult]:
        sem = asyncio.Semaphore(_MAX_CONCURRENT_METRICS)

        async def _run_one(
            graph_id: str,
            graph: TransformationGraph,
            hint_sql: str | None,
            inspiration_id: str | None,
        ) -> MetricResult:
            async with sem:
                # Capture unexpected exceptions as Result.fail so one worker
                # raising doesn't abort siblings via gather propagation.
                try:
                    result = await asyncio.to_thread(
                        _execute_isolated,
                        graph,
                        hint_sql,
                        manager,
                        agent,
                        schema_mapping_id,
                        table_ids,
                        session_id,
                    )
                except Exception as exc:
                    result = Result.fail(f"Unexpected error executing {graph_id}: {exc}")
            return graph_id, result, inspiration_id

        return await asyncio.gather(*(_run_one(gid, g, hsql, iid) for gid, g, hsql, iid in prep))

    return asyncio.run(_run_all())


def _execute_isolated(
    graph: TransformationGraph,
    hint_sql: str | None,
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    session_id: str,
) -> Result[GraphExecution]:
    """Run one metric with an isolated session + cursor pair.

    Wraps the call in manager.session_scope() so writes commit on success
    and roll back on exception. The DuckDB cursor is independent — the
    underlying connection is shared with other cursors safely.
    """
    from dataraum.graphs.agent import ExecutionContext

    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        exec_ctx = ExecutionContext.with_rich_context(
            session=session,
            duckdb_conn=cursor,
            table_ids=table_ids,
            schema_mapping_id=schema_mapping_id,
            session_id=session_id,
        )
        return agent.execute(
            session, graph, exec_ctx, inspiration_sql=hint_sql, session_id=session_id
        )
