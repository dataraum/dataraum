"""Graph execution phase — compute business metrics via the graph agent.

Loads metric graphs from the active vertical, resolves field mappings
via semantic annotations, and executes each metric through the graph
agent. Results are stored as SQL snippets for reuse by query/search_snippets.

Metrics with unresolvable direct field mappings are still attempted — the
graph agent LLM infers from enriched views and dataset context.
When no metric YAMLs exist, the phase completes with zero records.

Per-metric LLM calls are independent — dispatched concurrently via
asyncio.to_thread + gather when the phase context exposes a ConnectionManager
(so we can give each parallel call its own SQLAlchemy session + DuckDB
cursor). Falls back to a serial loop in unit tests where the manager isn't
wired (shared session is fine without concurrency).
"""

from __future__ import annotations

import asyncio
from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table

_log = get_logger(__name__)

# Cap concurrent metric LLM calls. Sonnet 4.6 tier-3+ workspaces handle
# 4000 RPM (~67 RPS) comfortably; with ~30-60s LLM latencies, 10 concurrent
# is ~10 RPS at peak — well under the limit. Smoke at cap=5 showed 12
# metrics fit in 3 waves (~157s); cap=10 should collapse to ~1.2 waves.
_MAX_CONCURRENT_METRICS = 10

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager
    from dataraum.graphs.agent import ExecutionContext as _ExecutionContext
    from dataraum.graphs.agent import GraphAgent
    from dataraum.graphs.models import GraphExecution, TransformationGraph

    MetricPrep = tuple[str, TransformationGraph, str | None, str | None]
    MetricResult = tuple[str, Result[GraphExecution], str | None]


@analysis_phase
class GraphExecutionPhase(BasePhase):
    """Execute metric graphs and store results as snippets.

    Requires: validation (ensures semantic + relationships are done).
    """

    @property
    def name(self) -> str:
        return "graph_execution"

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        from dataraum.query.snippet_models import SQLSnippetRecord

        # Scope to this source's snippets from graph execution
        result = session.execute(
            delete(SQLSnippetRecord).where(
                SQLSnippetRecord.source.like("graph:%"),
                SQLSnippetRecord.schema_mapping_id == source_id,
            )
        )
        count: int = result.rowcount  # type: ignore[attr-defined]
        return count

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.query import snippet_models

        return [snippet_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if snippets from graph execution already exist for this source."""
        from dataraum.query.snippet_models import SQLSnippetRecord

        existing = ctx.session.execute(
            select(SQLSnippetRecord.snippet_id)
            .where(
                SQLSnippetRecord.source.like("graph:%"),
                SQLSnippetRecord.schema_mapping_id == ctx.source_id,
            )
            .limit(1)
        ).first()
        if existing:
            return "Graph snippets already exist"
        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Load metrics, resolve fields, execute via graph agent."""
        from dataraum.graphs.agent import ExecutionContext, GraphAgent
        from dataraum.graphs.field_mapping import can_execute_metric, load_semantic_mappings
        from dataraum.graphs.loader import GraphLoader
        from dataraum.llm import PromptRenderer, create_provider, load_llm_config

        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()
        if not typed_tables:
            return PhaseResult.failed("No typed tables found")

        table_ids = [t.table_id for t in typed_tables]

        vertical = ctx.config.get("vertical")
        if not vertical:
            return PhaseResult.success(
                summary="No vertical configured — graph execution skipped",
            )

        # Initialize LLM
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

        # --- Metric induction for cold start ---
        if vertical == "_adhoc":
            loader_check = GraphLoader(vertical=vertical)
            existing_metrics = loader_check.load_all()
            if not existing_metrics:
                from dataraum.graphs.induction import (
                    MetricInductionAgent,
                    save_metrics_config,
                )

                induction_agent = MetricInductionAgent(
                    config=config,
                    provider=provider,
                    prompt_renderer=renderer,
                )
                induction_result = induction_agent.induce(
                    session=ctx.session,
                    table_ids=table_ids,
                )
                if not induction_result.success:
                    return PhaseResult.failed(f"Metric induction failed: {induction_result.error}")
                if not induction_result.value:
                    return PhaseResult.failed(
                        "Metric induction returned no metrics. Cold-start "
                        "requires at least one metric for graph execution."
                    )
                save_metrics_config(vertical, induction_result.value)
                _log.info(
                    "metric_induction_complete",
                    metrics=len(induction_result.value),
                )

        # Load metrics
        loader = GraphLoader(vertical=vertical)
        metrics = loader.load_all()

        if not metrics:
            # Cold-start induction (above) hard-fails when LLM call fails or
            # returns empty, so reaching here means: vertical is configured
            # but its metric set is empty. That's a vertical-config issue,
            # not a runtime failure.
            return PhaseResult.success(summary="No metrics configured — graph execution skipped")

        # Load field mappings
        field_mappings = load_semantic_mappings(ctx.session, table_ids)

        # Build execution context
        exec_ctx = ExecutionContext.with_rich_context(
            session=ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_ids=table_ids,
            schema_mapping_id=ctx.source_id,
        )

        # Create graph agent
        agent = GraphAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        # Resolve inspiration snippets for promotion path
        from dataraum.query.snippet_library import SnippetLibrary

        snippet_library = SnippetLibrary(ctx.session)

        # ---- Prep (sequential, cheap reads on main session) ----
        # Build a list of (graph_id, graph, hint_sql, inspiration_id) tuples.
        # Logs advisory warnings for missing direct mappings and resolves
        # inspiration snippet SQL up front so the parallel calls don't need
        # to touch the main session.
        prep: list[MetricPrep] = []
        for graph_id, graph in metrics.items():
            required_fields = [
                step.source.standard_field
                for step in graph.steps.values()
                if step.source and step.source.standard_field
            ]
            if required_fields:
                _, missing = can_execute_metric(field_mappings, required_fields)
                if missing:
                    _log.info(
                        "metric_missing_direct_mappings",
                        graph_id=graph_id,
                        missing=missing,
                    )

            hint_sql: str | None = None
            inspiration_id = graph.metadata.inspiration_snippet_id
            if inspiration_id:
                hint_snippet = snippet_library.find_by_id(inspiration_id)
                if hint_snippet:
                    hint_sql = hint_snippet.sql
                    _log.info(
                        "inspiration_snippet_resolved",
                        graph_id=graph_id,
                        snippet_id=inspiration_id,
                    )

            prep.append((graph_id, graph, hint_sql, inspiration_id))

        # ---- Execute (parallel when manager wired, serial fallback otherwise) ----
        if ctx.manager is not None:
            results = _execute_metrics_parallel(prep, ctx.manager, agent, ctx.source_id, table_ids)
        else:
            results = _execute_metrics_serial(prep, ctx.session, exec_ctx, agent)

        # ---- Post (sequential, snippet promotion on main session) ----
        executed = 0
        failed = 0
        for graph_id, result, inspiration_id in results:
            if result.success:
                executed += 1
                _log.info("metric_executed", graph_id=graph_id)

                # Snippet promotion: delete ad-hoc snippet after successful execution
                if inspiration_id:
                    ad_hoc = snippet_library.find_by_id(inspiration_id)
                    if ad_hoc:
                        ctx.session.delete(ad_hoc)
                        _log.info(
                            "snippet_promoted",
                            graph_id=graph_id,
                            deleted_snippet_id=inspiration_id,
                        )
            else:
                failed += 1
                _log.warning(
                    "metric_execution_failed",
                    graph_id=graph_id,
                    error=result.error,
                )

        # Hard-fail when nothing computed but failures occurred — that's a
        # systemic issue (LLM down, schema mismatch), not per-metric variance.
        # Partial success is fine: some metrics legitimately don't apply.
        if failed and not executed:
            return PhaseResult.failed(
                f"All {failed} metrics failed to execute. Likely a systemic "
                "issue (LLM unavailable, schema mismatch, or missing context)."
            )

        summary_parts = []
        if executed:
            summary_parts.append(f"{executed} computed")
        if failed:
            summary_parts.append(f"{failed} failed")

        summary = f"Metrics: {', '.join(summary_parts)}" if summary_parts else "No metrics"

        return PhaseResult.success(
            outputs={
                "metrics_executed": executed,
                "metrics_failed": failed,
            },
            records_processed=len(metrics),
            records_created=executed,
            summary=summary,
        )


# ---------------------------------------------------------------------------
# Per-metric dispatch
# ---------------------------------------------------------------------------


def _execute_metrics_serial(
    prep: list[MetricPrep],
    session: Session,
    exec_ctx: _ExecutionContext,
    agent: GraphAgent,
) -> list[MetricResult]:
    """Fallback path: shared session + cursor, sequential dispatch.

    Used in unit tests where PhaseContext.manager is None.
    """
    out: list[MetricResult] = []
    for graph_id, graph, hint_sql, inspiration_id in prep:
        result = agent.execute(session, graph, exec_ctx, inspiration_sql=hint_sql)
        out.append((graph_id, result, inspiration_id))
    return out


def _execute_metrics_parallel(
    prep: list[MetricPrep],
    manager: ConnectionManager,
    agent: GraphAgent,
    source_id: str,
    table_ids: list[str],
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
                # agent.execute already returns Result for the happy path —
                # this catches infrastructure failures (session_scope raises,
                # ExecutionContext.with_rich_context raises, etc.).
                try:
                    result = await asyncio.to_thread(
                        _execute_isolated, graph, hint_sql, manager, agent, source_id, table_ids
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
    source_id: str,
    table_ids: list[str],
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
            schema_mapping_id=source_id,
        )
        return agent.execute(session, graph, exec_ctx, inspiration_sql=hint_sql)
