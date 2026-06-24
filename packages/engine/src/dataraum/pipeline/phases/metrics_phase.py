"""Metrics phase — the operating_model stage's third lifecycle family (DAT-456).

Source-free and session-scoped, mirroring validation and cycles: operates on
``ctx.table_ids`` (the session's typed tables), never a ``source_id``. The
declared set is the vertical's ``metrics/`` transformation graphs ⊕ ``metric``
overlay teach rows — one ``metric`` lifecycle artifact per ``graph_id``. The
engine induces nothing (declares come from the vertical; user declares arrive
via frame-2 teach rows). Each declared metric flows through the typed artifact
lifecycle:

* **declare** — every loaded ``graph_id`` becomes a ``declared`` artifact.
* **compose** (``metric.compose``) — EVERY parseable metric is composed by the
  graph agent: it inspects the workspace (tables, columns, existing snippets) and
  materializes the metric's SQL. There is NO field-mapping pre-gate — whether an
  input like ``revenue`` is derivable from the data (e.g. from the GL via
  chart_of_accounts) is the agent's job to discover, not a heuristic dict-key
  check in front of the prompt. A definition that won't even parse stays
  ``declared`` with the parse error recorded (the one legitimate pre-gate).
* **execute** (``metric.execute``) — the agent runs the composed SQL cleanly →
  ``executed``, and the working SQL is materialized as reusable snippets (the
  durable, cross-run executable knowledge ``query`` later consumes). The snippet
  is gated on SUCCESSFUL execution — never a guess. A metric the agent cannot
  materialize into runnable SQL stays ``grounded`` with the reason: born-loud at
  the agent, not pre-empted by a gate.

A re-run supersedes: everything is re-declared and re-flowed under the fresh
``run_id`` (no skip-if-already-ran — the prior run's artifacts coexist untouched,
and the promoted head names the current run). The snippet base is NOT
run-versioned — it is the cross-run reuse cache shared with ``query``; a re-run
reuses healthy snippets and self-heals failed ones. With no vertical or no
declared metrics the phase succeeds loudly with an explicit outcome.

**Sanctioned multi-commit exception (DAT-502):** unlike every other phase
(one commit at ``session_scope`` exit), the parallel path commits once PER
METRIC (``_execute_isolated``). That is under the failure contract because
every per-metric write converges under at-least-once redelivery: snippet
state is first-writer-wins (``SnippetLibrary.save_snippet`` keeps a healthy
existing row, replaces only failed ones — the DAT-485 app-level dedup), and
the ``snippet_usage`` rows / ``execution_count`` counters are the documented
TELEMETRY exception — ``sql_snippets``/``snippet_usage`` are not run-stamped,
so a redelivery can inflate usage telemetry; nothing gates on it (write-only
since DAT-487/488).

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
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager
    from dataraum.graphs.agent import ExecutionContext as _ExecutionContext
    from dataraum.graphs.agent import GraphAgent
    from dataraum.graphs.models import GraphExecution, TransformationGraph
    from dataraum.graphs.node_warming import WarmNode
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
        snippet_library = SnippetLibrary(ctx.session)

        # declare: every declared graph_id becomes a declared artifact for THIS
        # run — supersession across runs; a success-redelivery RESETS the same
        # run's row to declared (declare-or-reuse, DAT-502).
        artifacts: dict[str, LifecycleArtifact] = {}
        for graph_id, defn in declared_defs.items():
            artifacts[graph_id] = declare_artifact(
                ctx.session,
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

        # Parse declared definitions into graphs. A definition that won't parse
        # stays declared with the parse error recorded — visibly impossible.
        loader = GraphLoader()
        graphs: dict[str, TransformationGraph] = {}
        for graph_id, defn in declared_defs.items():
            try:
                graphs[graph_id] = loader.graphs_from_definitions({graph_id: defn})[graph_id]
            except GraphLoadError as e:
                artifacts[graph_id].state_reason = f"malformed metric definition: {e.message}"
                _log.warning("metric_definition_malformed", graph_id=graph_id, error=e.message)

        # compose: hand EVERY parseable metric to the graph agent. No
        # field-mapping pre-gate — the agent inspects the workspace (and the
        # existing snippet base) and discovers whether a required input is
        # derivable; that is the agentic job, not a dict-key check in front of the
        # prompt. Born-loud lives at execute (an agent that cannot materialize
        # runnable SQL stays grounded with the reason) and at snippet
        # materialization (gated on a clean run) — never a heuristic skip here.
        grounded_against = base_runs.model_dump(mode="json")
        prep: list[MetricPrep] = []
        for graph_id, graph in graphs.items():
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

        # warm (DAT-629): before the per-metric fan-out, author each UNIQUE
        # cache-keyed node once, in dependency order. metrics_phase executes
        # metrics in parallel; a sub-node shared by several metrics (e.g. the
        # cost_of_goods_sold extract) is cold for all of them at once, so each
        # independently LLM-authors it — they diverge and some ground to an empty
        # filter (born-loud "no support"). Warming the shared node-set first lets
        # the execute below assemble those nodes from the now-warm snippet cache —
        # consistent, no within-run race. Best-effort: a node that fails to warm
        # just falls back to per-metric authoring (the pre-DAT-629 behavior).
        _warm_shared_nodes(
            graphs, ctx, agent, schema_mapping_id, table_ids, vertical, om_run_id=run_id
        )

        # execute: run each composed metric. Parallel when the manager is wired,
        # serial fallback otherwise.
        if ctx.manager is not None:
            results = _execute_metrics_parallel(
                prep,
                ctx.manager,
                agent,
                schema_mapping_id,
                table_ids,
                vertical,
                om_run_id=run_id,
            )
        else:
            exec_ctx = ExecutionContext.with_rich_context(
                session=ctx.session,
                duckdb_conn=ctx.duckdb_conn,
                table_ids=table_ids,
                schema_mapping_id=schema_mapping_id,
                om_run_id=run_id,
                vertical=vertical,
            )
            results = _execute_metrics_serial(prep, ctx.session, exec_ctx, agent, schema_mapping_id)

        # A composed metric that ran cleanly AND verified reaches executed; one
        # whose SQL failed OR whose result was inconclusive (no support / a
        # declared condition violated — DAT-616 verifier) stays grounded with the
        # reason (born loud, never silently green).
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
                artifact.state_reason = f"composed but not executed: {result.error}"
                _log.warning("metric_not_executed", graph_id=graph_id, error=result.error)

        executed = sum(1 for a in artifacts.values() if a.state == "executed")
        grounded_stuck = sum(1 for a in artifacts.values() if a.state == "grounded")
        declared_stuck = sum(1 for a in artifacts.values() if a.state == "declared")

        # Surface every artifact's outcome — executed, plus each stuck one WITH
        # its reason — so the cockpit shows "dso: declared — ungroundable
        # (missing: accounts_receivable)" rather than a bare count, distinguishing
        # the failure modes (ungroundable vs malformed vs composed-but-unexecutable).
        previews: list[str] = []
        for graph_id, a in artifacts.items():
            if a.state == "executed":
                previews.append(f"{graph_id}: executed")
            else:
                previews.append(f"{graph_id}: {a.state} — {a.state_reason or 'no reason recorded'}")

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
                f"{declared_stuck} ungroundable, {grounded_stuck} composed but inconclusive/failed"
            ),
        )


# ---------------------------------------------------------------------------
# Node warming pre-pass (DAT-629)
# ---------------------------------------------------------------------------


def _warm_shared_nodes(
    graphs: dict[str, TransformationGraph],
    ctx: PhaseContext,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    *,
    om_run_id: str,
) -> None:
    """Topo-warm the unique cache-keyed nodes before the per-metric fan-out.

    Builds the cross-metric DAG, then warms each unique node once in dependency
    order: a generation runs concurrently (independent nodes), with a barrier
    between generations so a formula node sees its dep extracts already cached.
    The per-metric ``execute`` that follows assembles the warmed nodes from the
    snippet cache with no LLM call — consistent and race-free.

    This is purely an optimization of the cache-warming order. A cyclic metric
    set or a node that fails to warm is non-fatal: warming is skipped/best-effort
    and the per-metric execute surfaces any real failure born-loud as before.
    """
    from dataraum.graphs.node_warming import build_warm_dag, warming_generations

    try:
        dag, nodes = build_warm_dag(graphs)
    except ValueError as e:
        _log.warning("metric_warm_dag_failed", error=str(e))
        return

    generations = warming_generations(dag)
    if not generations:
        return

    _log.info(
        "metrics_warming_start",
        nodes=sum(len(g) for g in generations),
        generations=len(generations),
    )

    if ctx.manager is not None:
        _warm_generations_parallel(
            generations, nodes, ctx.manager, agent, schema_mapping_id, table_ids, vertical, om_run_id
        )
    else:
        _warm_generations_serial(
            generations,
            nodes,
            ctx.session,
            ctx.duckdb_conn,
            agent,
            schema_mapping_id,
            table_ids,
            vertical,
            om_run_id,
        )


def _warm_generations_parallel(
    generations: list[list[tuple[str | None, ...]]],
    nodes: dict[tuple[str | None, ...], WarmNode],
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    om_run_id: str,
) -> None:
    """Warm generations concurrently within each wave, barrier between waves.

    The barrier (``gather`` per generation) is load-bearing: generation N+1's
    formula nodes must see generation N's extracts already committed to the
    cache, so they assemble from the warm cache rather than re-authoring.
    """

    async def _run_all() -> None:
        sem = asyncio.Semaphore(_MAX_CONCURRENT_METRICS)

        async def _warm_one(key: tuple[str | None, ...]) -> None:
            async with sem:
                try:
                    await asyncio.to_thread(
                        _warm_isolated,
                        nodes[key],
                        manager,
                        agent,
                        schema_mapping_id,
                        table_ids,
                        vertical,
                        om_run_id,
                    )
                except Exception as exc:
                    # Never abort siblings or the phase — fall back to per-metric
                    # authoring for this node.
                    _log.warning("metric_node_warm_error", node=str(key), error=str(exc))

        for generation in generations:
            await asyncio.gather(*(_warm_one(key) for key in generation))

    asyncio.run(_run_all())


def _warm_isolated(
    node: WarmNode,
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    om_run_id: str,
) -> None:
    """Warm one node with an isolated session + cursor (mirrors _execute_isolated)."""
    from dataraum.graphs.agent import ExecutionContext
    from dataraum.graphs.node_warming import build_mini_graph

    mini = build_mini_graph(node)
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        exec_ctx = ExecutionContext.with_rich_context(
            session=session,
            duckdb_conn=cursor,
            table_ids=table_ids,
            schema_mapping_id=schema_mapping_id,
            om_run_id=om_run_id,
            vertical=vertical,
        )
        result = agent.execute(session, mini, exec_ctx, workspace_id=schema_mapping_id)
    if not result.success:
        # Inconclusive warm (e.g. an extract with genuinely no support): not an
        # error — the metric using it will surface it born-loud at execute.
        _log.info("metric_node_warm_inconclusive", node=str(node.key), reason=result.error)


def _warm_generations_serial(
    generations: list[list[tuple[str | None, ...]]],
    nodes: dict[tuple[str | None, ...], WarmNode],
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    om_run_id: str,
) -> None:
    """Serial fallback: shared session + cursor, sequential dependency order."""
    from dataraum.graphs.agent import ExecutionContext
    from dataraum.graphs.node_warming import build_mini_graph

    exec_ctx = ExecutionContext.with_rich_context(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        schema_mapping_id=schema_mapping_id,
        om_run_id=om_run_id,
        vertical=vertical,
    )
    for generation in generations:
        for key in generation:
            try:
                result = agent.execute(
                    session, build_mini_graph(nodes[key]), exec_ctx, workspace_id=schema_mapping_id
                )
            except Exception as exc:
                _log.warning("metric_node_warm_error", node=str(key), error=str(exc))
                continue
            if not result.success:
                _log.info("metric_node_warm_inconclusive", node=str(key), reason=result.error)


# ---------------------------------------------------------------------------
# Per-metric dispatch
# ---------------------------------------------------------------------------


def _execute_metrics_serial(
    prep: list[MetricPrep],
    session: Session,
    exec_ctx: _ExecutionContext,
    agent: GraphAgent,
    workspace_id: str,
) -> list[MetricResult]:
    """Fallback path: shared session + cursor, sequential dispatch.

    Used in unit tests where PhaseContext.manager is None.
    """
    out: list[MetricResult] = []
    for graph_id, graph, hint_sql, inspiration_id in prep:
        result = agent.execute(
            session, graph, exec_ctx, inspiration_sql=hint_sql, workspace_id=workspace_id
        )
        out.append((graph_id, result, inspiration_id))
    return out


def _execute_metrics_parallel(
    prep: list[MetricPrep],
    manager: ConnectionManager,
    agent: GraphAgent,
    schema_mapping_id: str,
    table_ids: list[str],
    vertical: str,
    *,
    om_run_id: str,
) -> list[MetricResult]:
    """Concurrent path: per-call session + cursor, gathered via asyncio.

    Each metric runs `agent.execute` on a thread with its own SQLAlchemy
    session (auto-commit via session_scope) and its own DuckDB cursor.
    A semaphore caps in-flight LLM calls to _MAX_CONCURRENT_METRICS.
    ``om_run_id`` is this operating_model run — the graph context reads its
    cycles/validation evidence at this run, not the (not-yet-promoted) head.
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
                        vertical,
                        om_run_id,
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
    vertical: str,
    om_run_id: str,
) -> Result[GraphExecution]:
    """Run one metric with an isolated session + cursor pair.

    Wraps the call in manager.session_scope() so writes commit on success
    and roll back on exception. The DuckDB cursor is independent — the
    underlying connection is shared with other cursors safely.

    Sanctioned multi-commit shape (DAT-502): each metric commits its own
    session, so a phase that fails later does NOT roll these back. That is
    safe because every write here converges under redelivery — snippets are
    first-writer-wins (per-key app-level dedup, DAT-485) and usage counters
    are the documented telemetry exception (see the module docstring).
    """
    from dataraum.graphs.agent import ExecutionContext

    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        exec_ctx = ExecutionContext.with_rich_context(
            session=session,
            duckdb_conn=cursor,
            table_ids=table_ids,
            schema_mapping_id=schema_mapping_id,
            om_run_id=om_run_id,
            vertical=vertical,
        )
        return agent.execute(
            session, graph, exec_ctx, inspiration_sql=hint_sql, workspace_id=schema_mapping_id
        )
