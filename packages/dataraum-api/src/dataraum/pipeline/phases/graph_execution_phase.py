"""Graph execution phase implementation.

Executes transformation graphs (metrics) using the GraphAgent.
The agent uses LLM to generate SQL from graph specifications,
inferring column mappings from semantic annotations.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from dataraum.graphs.agent import ExecutionContext, GraphAgent
from dataraum.graphs.field_mapping import can_execute_metric, load_semantic_mappings
from dataraum.graphs.loader import GraphLoader
from dataraum.llm import create_provider, load_llm_config
from dataraum.llm.cache import LLMCache
from dataraum.llm.prompts import PromptRenderer
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Table


class GraphExecutionPhase(BasePhase):
    """Execute transformation graphs to calculate metrics.

    Uses semantic annotations and LLM inference to map abstract
    graph fields to concrete columns, then executes the SQL.

    Requires: semantic phase (for field mappings).
    """

    @property
    def name(self) -> str:
        return "graph_execution"

    @property
    def description(self) -> str:
        return "Execute metric graphs"

    @property
    def dependencies(self) -> list[str]:
        # Depends on all phases that build_execution_context pulls data from
        return [
            "semantic",  # field mappings, table entities
            "statistics",  # statistical profiles
            "statistical_quality",  # quality metrics
            "temporal",  # temporal profiles
            "relationships",  # table relationships
            "correlations",  # derived columns
            "slicing",  # slice definitions
            "quality_summary",  # quality reports
            "business_cycles",  # detected cycles
            "entropy_interpretation",  # entropy data
        ]

    @property
    def outputs(self) -> list[str]:
        return ["metrics_calculated", "metrics_skipped", "execution_context"]

    @property
    def is_llm_phase(self) -> bool:
        return True

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no typed tables found."""
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Execute transformation graphs."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]

        # Load field mappings from semantic annotations
        field_mappings = load_semantic_mappings(ctx.session, table_ids)

        # Load graph definitions
        loader = GraphLoader()
        loader.load_all()
        metric_graphs = loader.get_metric_graphs()

        if not metric_graphs:
            return PhaseResult.success(
                outputs={
                    "metrics_calculated": [],
                    "metrics_skipped": [],
                    "message": "No metric graphs defined",
                },
                records_processed=0,
                records_created=0,
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
        cache = LLMCache()

        agent = GraphAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
            cache=cache,
        )

        # Find primary fact table for execution
        primary_table = next(
            (
                t
                for t in typed_tables
                if any(
                    kw in t.table_name.lower() for kw in ["txn", "transaction", "fact", "master"]
                )
            ),
            typed_tables[0] if typed_tables else None,
        )

        if not primary_table:
            return PhaseResult.failed("No suitable table found for metric execution")

        # Create execution context with rich metadata
        exec_context = ExecutionContext.with_rich_context(
            session=ctx.session,
            duckdb_conn=ctx.duckdb_conn,
            table_name=f"typed_{primary_table.table_name}",
            table_ids=table_ids,
            entropy_behavior_mode="balanced",
        )

        # Execute each metric graph
        calculated_metrics: list[dict[str, Any]] = []
        skipped_metrics: list[dict[str, Any]] = []

        for graph in metric_graphs:
            # Check which fields are required
            required_fields = []
            for step in graph.steps.values():
                if step.source and step.source.standard_field:
                    required_fields.append(step.source.standard_field)

            # Check if we have direct mappings (LLM can still infer if not)
            can_exec, missing = can_execute_metric(field_mappings, required_fields)

            # Execute the graph (LLM will infer missing fields)
            try:
                exec_result = agent.execute(
                    session=ctx.session,
                    graph=graph,
                    context=exec_context,
                    force_regenerate=False,  # Use cache if available
                )

                if exec_result.success and exec_result.value is not None:
                    execution = exec_result.value
                    calculated_metrics.append(
                        {
                            "graph_id": graph.graph_id,
                            "metric_name": graph.metadata.name,
                            "value": execution.output_value,
                            "interpretation": execution.output_interpretation,
                            "unit": graph.output.unit if graph.output else None,
                            "inferred_fields": missing,  # Fields LLM had to infer
                        }
                    )
                else:
                    skipped_metrics.append(
                        {
                            "graph_id": graph.graph_id,
                            "metric_name": graph.metadata.name,
                            "reason": exec_result.error or "Execution failed",
                            "missing_fields": missing,
                        }
                    )

            except Exception as e:
                skipped_metrics.append(
                    {
                        "graph_id": graph.graph_id,
                        "metric_name": graph.metadata.name,
                        "reason": str(e),
                        "missing_fields": missing,
                    }
                )

        # Extract context statistics (previously from context_phase)
        rich_ctx = exec_context.rich_context
        context_stats = {}
        if rich_ctx:
            total_columns = sum(len(t.columns) for t in rich_ctx.tables)
            context_stats = {
                "tables": len(rich_ctx.tables),
                "columns": total_columns,
                "relationships": len(rich_ctx.relationships),
                "business_cycles": len(rich_ctx.business_cycles),
                "available_slices": len(rich_ctx.available_slices),
                "quality_issues": rich_ctx.quality_issues_by_severity,
                "has_field_mappings": rich_ctx.field_mappings is not None,
                "has_entropy_summary": rich_ctx.entropy_summary is not None,
            }

        return PhaseResult.success(
            outputs={
                "metrics_calculated": calculated_metrics,
                "metrics_skipped": skipped_metrics,
                "total_graphs": len(metric_graphs),
                "primary_table": primary_table.table_name,
                "execution_context": context_stats,
            },
            records_processed=len(metric_graphs),
            records_created=len(calculated_metrics),
        )
