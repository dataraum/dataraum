"""Graph execution phase — compute business metrics via the graph agent.

Loads metric graphs from the active vertical, resolves field mappings
via semantic annotations, and executes each metric through the graph
agent. Results are stored as SQL snippets for reuse by query/search_snippets.

Metrics with unresolvable direct field mappings are still attempted — the
graph agent LLM infers from enriched views and dataset context.
When no metric YAMLs exist, the phase completes with zero records.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Table

_log = get_logger(__name__)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


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
        induction_failed = False
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
                if induction_result.success and induction_result.value:
                    save_metrics_config(vertical, induction_result.value)
                    _log.info(
                        "metric_induction_complete",
                        metrics=len(induction_result.value),
                    )
                else:
                    induction_failed = True
                    _log.warning(
                        "metric_induction_failed",
                        error=induction_result.error if not induction_result.success else "empty",
                    )

        # Load metrics
        loader = GraphLoader(vertical=vertical)
        metrics = loader.load_all()

        if not metrics:
            summary = (
                "Metric induction failed — no metrics to execute"
                if induction_failed
                else "No metrics configured — graph execution skipped"
            )
            return PhaseResult.success(summary=summary)

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

        # Execute metrics sequentially
        executed = 0
        failed = 0

        # Resolve inspiration snippets for promotion path
        from dataraum.query.snippet_library import SnippetLibrary

        snippet_library = SnippetLibrary(ctx.session)

        for graph_id, graph in metrics.items():
            # Collect required standard_fields from extract steps
            required_fields = [
                step.source.standard_field
                for step in graph.steps.values()
                if step.source and step.source.standard_field
            ]

            # Advisory check — log missing direct mappings but let the LLM
            # infer from enriched views and dataset context (as it did pre-DAT-183).
            if required_fields:
                _, missing = can_execute_metric(field_mappings, required_fields)
                if missing:
                    _log.info(
                        "metric_missing_direct_mappings",
                        graph_id=graph_id,
                        missing=missing,
                    )

            # Resolve inspiration snippet SQL for promotion path
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

            # Execute — LLM will infer missing field mappings
            result = agent.execute(ctx.session, graph, exec_ctx, inspiration_sql=hint_sql)
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
