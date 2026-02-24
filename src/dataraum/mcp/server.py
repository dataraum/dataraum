"""MCP Server implementation for DataRaum.

Exposes high-level tools that call library functions directly (no HTTP).
Output directory is resolved from DATARAUM_OUTPUT_DIR env var or passed to create_server().
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.server.experimental.request_context import Experimental
from mcp.server.experimental.task_context import ServerTaskContext
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, CreateTaskResult, TextContent, Tool, ToolExecution

from dataraum.mcp.formatters import (
    format_actions_report,
    format_context_for_llm,
    format_contract_evaluation,
    format_entropy_summary,
    format_pipeline_result,
    format_query_result,
)
from dataraum.pipeline.orchestrator import ProgressCallback

_log = logging.getLogger(__name__)

# Prevent background pipeline tasks from being garbage-collected.
_background_tasks: set[asyncio.Task[Any]] = set()


def _make_task_progress_callback(
    task: ServerTaskContext,
    loop: asyncio.AbstractEventLoop,
) -> ProgressCallback:
    """Create a sync callback that bridges to async task.update_status().

    Called from the pipeline thread (sync context) to push progress updates
    to the MCP task (async context) via run_coroutine_threadsafe.
    """

    def _callback(current: int, total: int, message: str) -> None:
        try:
            future = asyncio.run_coroutine_threadsafe(
                task.update_status(f"[{current}/{total}] {message}"),
                loop,
            )
            future.result(timeout=5.0)
        except Exception:
            pass  # Never let notification failures break the pipeline

    return _callback


def create_server(output_dir: Path | None = None) -> Server:
    """Create and configure the MCP server with DataRaum tools.

    Args:
        output_dir: Pipeline output directory. If not provided, reads from
            DATARAUM_OUTPUT_DIR env var, defaulting to ./pipeline_output.
    """
    if output_dir is None:
        output_dir = Path(os.environ.get("DATARAUM_OUTPUT_DIR", "./pipeline_output"))

    server = Server("dataraum")
    server.experimental.enable_tasks()

    @server.list_tools()  # type: ignore[no-untyped-call, untyped-decorator]
    async def list_tools() -> list[Tool]:
        """List available tools."""
        return [
            Tool(
                name="analyze",
                description=(
                    "Analyze CSV or Parquet data to build metadata context. "
                    "Must be called before other tools if no data has been analyzed yet. "
                    "Takes a path to a file or directory. Runs the full 18-phase pipeline. "
                    "This takes several minutes and returns immediately. "
                    "With task support, progress updates are delivered automatically. "
                    "Otherwise, call `get_context` to check progress and get results."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path to CSV/Parquet file or directory of files",
                        },
                        "name": {
                            "type": "string",
                            "description": "Optional: name for the data source",
                        },
                    },
                    "required": ["path"],
                },
                execution=ToolExecution(taskSupport="optional"),
            ),
            Tool(
                name="get_context",
                description=(
                    "Get the full data context document for AI analysis. "
                    "Returns schema, relationships, semantic annotations, and data quality info."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            Tool(
                name="get_entropy",
                description=(
                    "Get entropy analysis showing data uncertainty by dimension. "
                    "Helps understand what assumptions the system makes and what needs fixing."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Optional: filter to a specific table",
                        },
                    },
                },
            ),
            Tool(
                name="evaluate_contract",
                description=(
                    "Evaluate data quality against a contract. "
                    "Returns compliance status, dimension scores, and violations."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "contract_name": {
                            "type": "string",
                            "description": "Contract to evaluate (e.g., 'aggregation_safe', 'executive_dashboard')",
                        },
                    },
                    "required": ["contract_name"],
                },
            ),
            Tool(
                name="query",
                description=(
                    "Execute a natural language query against the data. "
                    "Returns answer, confidence level, generated SQL, and data."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question about the data",
                        },
                        "contract_name": {
                            "type": "string",
                            "description": "Optional: contract to evaluate against",
                        },
                    },
                    "required": ["question"],
                },
            ),
            Tool(
                name="get_actions",
                description=(
                    "Get prioritized resolution actions to improve data quality. "
                    "Returns actionable steps with priority, effort, affected columns, and expected impact."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "priority": {
                            "type": "string",
                            "enum": ["high", "medium", "low"],
                            "description": "Optional: filter to a specific priority level",
                        },
                        "table_name": {
                            "type": "string",
                            "description": "Optional: filter to actions affecting a specific table",
                        },
                    },
                },
            ),
        ]

    @server.call_tool()  # type: ignore[no-untyped-call, untyped-decorator]
    async def call_tool(
        name: str, arguments: dict[str, Any]
    ) -> list[TextContent] | CallToolResult | CreateTaskResult:
        """Execute a tool and return results."""
        if name == "analyze":
            path = arguments["path"]
            source_name = arguments.get("name")

            # Validate path before doing anything
            source_path = Path(path)
            if not source_path.exists():
                return [TextContent(type="text", text=f"Error: Path not found: {path}")]

            ctx = server.request_context
            experimental: Experimental = ctx.experimental
            if experimental and experimental.is_task:
                # Task-augmented path: return immediately, run in background
                loop = asyncio.get_running_loop()

                async def _work(task: ServerTaskContext) -> CallToolResult:
                    callback = _make_task_progress_callback(task, loop)
                    text = await asyncio.to_thread(
                        _analyze, output_dir, path, source_name, callback
                    )
                    return CallToolResult(
                        content=[TextContent(type="text", text=text)]
                    )

                return await experimental.run_task(
                    _work,
                    model_immediate_response=(
                        f"Pipeline started for: {path}. "
                        f"Running in the background — status updates will follow."
                    ),
                )
            else:
                # No task API: fire-and-forget, client polls get_context
                bg = asyncio.create_task(
                    _run_analyze_background(output_dir, path, source_name)
                )
                _background_tasks.add(bg)
                bg.add_done_callback(_background_tasks.discard)
                result = (
                    f"Pipeline started for: {path}. "
                    f"Call `get_context` to check progress."
                )
        elif name == "get_context":
            result = _get_context(output_dir)
        elif name == "get_entropy":
            table_name = arguments.get("table_name")
            result = _get_entropy(output_dir, table_name)
        elif name == "evaluate_contract":
            contract_name = arguments["contract_name"]
            result = _evaluate_contract(output_dir, contract_name)
        elif name == "query":
            question = arguments["question"]
            contract_name = arguments.get("contract_name")
            result = _query(output_dir, question, contract_name)
        elif name == "get_actions":
            priority = arguments.get("priority")
            table_name = arguments.get("table_name")
            result = _get_actions(output_dir, priority, table_name)
        else:
            result = f"Unknown tool: {name}"

        return [TextContent(type="text", text=result)]

    return server


_NO_DATA_MSG = (
    "No analyzed data found at {path}. Use the `analyze` tool first:\n"
    "  analyze(path='/path/to/your/data.csv')"
)


async def _run_analyze_background(
    output_dir: Path,
    path: str,
    source_name: str | None,
) -> None:
    """Run _analyze in a background thread, logging errors."""
    try:
        await asyncio.to_thread(_analyze, output_dir, path, source_name, None)
    except Exception:
        _log.exception("Background pipeline failed for %s", path)


def _get_pipeline_progress(manager: Any) -> str | None:
    """Check if a pipeline is running and return a progress message.

    Returns:
        Progress message string if running, None if no pipeline is running.
    """
    from sqlalchemy import func, select

    from dataraum.pipeline.db_models import PhaseCheckpoint, PipelineRun
    from dataraum.pipeline.registry import get_registry
    from dataraum.storage import Source

    with manager.session_scope() as session:
        sources_result = session.execute(select(Source))
        sources = sources_result.scalars().all()
        if not sources:
            return None

        source = sources[0]

        running_run = session.execute(
            select(PipelineRun)
            .where(
                PipelineRun.source_id == source.source_id,
                PipelineRun.status == "running",
            )
            .order_by(PipelineRun.started_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        if running_run is None:
            return None

        completed_count: int = (
            session.execute(
                select(func.count()).where(PhaseCheckpoint.run_id == running_run.run_id)
            ).scalar()
            or 0
        )

        registry = get_registry()
        total_phases = len(registry)

        # Determine currently running phases from dependency graph
        completed_names: set[str] = set()
        cp_result = session.execute(
            select(PhaseCheckpoint.phase_name).where(
                PhaseCheckpoint.run_id == running_run.run_id
            )
        )
        for row in cp_result:
            completed_names.add(row[0])

        running_phases: list[str] = []
        for name, cls in registry.items():
            if name in completed_names:
                continue
            instance = cls()
            deps = set(instance.dependencies)
            if deps.issubset(completed_names):
                running_phases.append(name)

        current_detail = ""
        if running_phases:
            current_detail = f" Current: Running {', '.join(running_phases)}..."

        return (
            f"Pipeline is still running ({completed_count}/{total_phases} phases complete)."
            f"{current_detail}"
        )


def _analyze(
    output_dir: Path,
    path: str,
    name: str | None = None,
    progress_callback: ProgressCallback | None = None,
) -> str:
    """Run the pipeline on a data source.

    Args:
        output_dir: Pipeline output directory
        path: Path to CSV/Parquet file or directory
        name: Optional source name
        progress_callback: Optional callback for progress notifications

    Returns:
        Formatted pipeline result summary
    """
    from dataraum.pipeline.runner import RunConfig, run

    source_path = Path(path)
    if not source_path.exists():
        return f"Error: Path not found: {path}"

    config = RunConfig(
        source_path=source_path,
        output_dir=output_dir,
        source_name=name,
        progress_callback=progress_callback,
    )

    result = run(config)

    if not result.success or not result.value:
        return f"Error: Pipeline failed: {result.error}"

    return format_pipeline_result(result.value)


def _get_context(output_dir: Path) -> str:
    """Get formatted context document, or progress status if pipeline is running."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.graphs.context import build_execution_context, format_context_for_prompt
    from dataraum.storage import Source, Table

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        # If pipeline is still running, return progress instead of partial context
        progress = _get_pipeline_progress(manager)
        if progress is not None:
            return f"{progress}\nCall `get_context` again to check for completion."

        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return "Error: No sources found in database"

            source = sources[0]

            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                return "Error: No tables found. Run pipeline first."

            table_ids = [t.table_id for t in tables]

            with manager.duckdb_cursor() as cursor:
                context = build_execution_context(
                    session=session,
                    table_ids=table_ids,
                    duckdb_conn=cursor,
                )

            formatted = format_context_for_prompt(context)
            return format_context_for_llm(source.name, formatted)
    finally:
        manager.close()


def _get_entropy(output_dir: Path, table_name: str | None = None) -> str:
    """Get entropy summary."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.entropy.db_models import (
        EntropySnapshotRecord,
    )
    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
    from dataraum.storage import Source

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return "Error: No sources found"

            source = sources[0]

            # Get snapshot
            snapshot_result = session.execute(
                select(EntropySnapshotRecord)
                .where(EntropySnapshotRecord.source_id == source.source_id)
                .order_by(EntropySnapshotRecord.snapshot_at.desc())
                .limit(1)
            )
            snapshot = snapshot_result.scalar_one_or_none()

            if not snapshot:
                return "Error: No entropy data. Run entropy phase first."

            # Get interpretations
            interp_query = select(EntropyInterpretationRecord).where(
                EntropyInterpretationRecord.source_id == source.source_id
            )

            if table_name:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.table_name == table_name
                )

            interp_query = interp_query.order_by(
                EntropyInterpretationRecord.table_name,
                EntropyInterpretationRecord.column_name,
            )
            interp_result = session.execute(interp_query)
            interpretations = interp_result.scalars().all()

            # Build dimension breakdown from network + direct signals
            dimension_scores: dict[str, float] | None = None
            try:
                from dataraum.entropy.views.network_context import (
                    build_for_network,
                    format_network_context,
                )
                from dataraum.entropy.views.query_context import network_to_column_summaries
                from dataraum.storage import Table

                tables_result = session.execute(
                    select(Table).where(Table.source_id == source.source_id)
                )
                tables = tables_result.scalars().all()
                table_ids = [t.table_id for t in tables]

                network_context = None
                if table_ids:
                    network_context = build_for_network(session, table_ids)
            except Exception:
                network_context = None
                _log.debug("Network context unavailable", exc_info=True)

            # Compute per-dimension averages across columns
            if network_context and network_context.total_columns > 0:
                col_summaries = network_to_column_summaries(network_context)
                dim_totals: dict[str, list[float]] = {}
                for summary in col_summaries.values():
                    for dim_path, score in summary.dimension_scores.items():
                        dim_totals.setdefault(dim_path, []).append(score)
                dimension_scores = {
                    dim: sum(scores) / len(scores)
                    for dim, scores in dim_totals.items()
                }

            result = format_entropy_summary(
                source.name, snapshot, interpretations, table_name, dimension_scores
            )

            # Append network context (inference + evidence + direct signals)
            if network_context and network_context.total_columns > 0:
                result += "\n\n" + format_network_context(network_context)

            return result
    finally:
        manager.close()


def _evaluate_contract(output_dir: Path, contract_name: str) -> str:
    """Evaluate a contract."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.entropy.contracts import evaluate_contract, get_contract
    from dataraum.entropy.views.network_context import build_for_network
    from dataraum.entropy.views.query_context import network_to_column_summaries
    from dataraum.storage import Source, Table

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return "Error: No sources found"

            source = sources[0]

            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                return "Error: No tables found"

            table_ids = [t.table_id for t in tables]

            # Build column summaries via network
            network_ctx = build_for_network(session, table_ids)
            column_summaries = network_to_column_summaries(network_ctx)

            profile = get_contract(contract_name)
            if profile is None:
                return f"Error: Contract not found: {contract_name}"

            evaluation = evaluate_contract(column_summaries, contract_name)
            return format_contract_evaluation(evaluation, profile)
    finally:
        manager.close()


def _query(output_dir: Path, question: str, contract_name: str | None = None) -> str:
    """Execute a natural language query."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.query import answer_question
    from dataraum.storage import Source

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return "Error: No sources found"

            source = sources[0]

            with manager.duckdb_cursor() as cursor:
                result = answer_question(
                    question=question,
                    session=session,
                    duckdb_conn=cursor,
                    source_id=source.source_id,
                    contract=contract_name,
                    manager=manager,
                )

            if not result.success or not result.value:
                return f"Error: {result.error}"

            return format_query_result(result.value)
    finally:
        manager.close()


def _get_actions(
    output_dir: Path,
    priority: str | None = None,
    table_name: str | None = None,
) -> str:
    """Get prioritized resolution actions."""
    from collections import defaultdict

    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.entropy.actions import merge_actions
    from dataraum.entropy.contracts import evaluate_all_contracts
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
    from dataraum.entropy.views.network_context import build_for_network
    from dataraum.entropy.views.query_context import network_to_column_summaries
    from dataraum.storage import Column, Source, Table

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        with manager.session_scope() as session:
            # Get source
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                return "Error: No sources found"

            source = sources[0]

            # Get tables
            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                return "Error: No tables found. Run pipeline first."

            table_ids = [t.table_id for t in tables]

            # Build column_id -> column_key mapping
            col_id_to_key: dict[str, str] = {}
            for tbl in tables:
                cols_result = session.execute(select(Column).where(Column.table_id == tbl.table_id))
                for col in cols_result.scalars().all():
                    col_id_to_key[col.column_id] = f"{tbl.table_name}.{col.column_name}"

            # Build column summaries and network context
            network_context = build_for_network(session, table_ids)
            column_summaries = network_to_column_summaries(network_context)

            # Get LLM interpretations with resolution actions
            interp_result = session.execute(
                select(EntropyInterpretationRecord).where(
                    EntropyInterpretationRecord.source_id == source.source_id,
                    EntropyInterpretationRecord.column_name.isnot(None),
                )
            )
            interp_by_col: dict[str, Any] = {}
            for interp in interp_result.scalars().all():
                col_key = f"{interp.table_name}.{interp.column_name}"
                interp_by_col[col_key] = interp

            # Get entropy objects for evidence
            entropy_objects_by_col: dict[str, list[Any]] = defaultdict(list)
            if table_ids:
                eo_result = session.execute(
                    select(EntropyObjectRecord)
                    .where(EntropyObjectRecord.table_id.in_(table_ids))
                    .order_by(EntropyObjectRecord.score.desc())
                )
                for obj in eo_result.scalars().all():
                    col_key = col_id_to_key.get(obj.column_id, "") if obj.column_id else ""
                    if col_key:
                        entropy_objects_by_col[col_key].append(obj)

            # Get contract violations
            evaluations = evaluate_all_contracts(column_summaries)
            violation_dims: dict[str, list[str]] = {}
            for eval_result in evaluations.values():
                for v in eval_result.violations:
                    if v.dimension:
                        violation_dims.setdefault(v.dimension, []).extend(v.affected_columns)

            # Merge actions from all sources (including network causal impact)
            actions = merge_actions(
                interp_by_col=interp_by_col,
                entropy_objects_by_col=entropy_objects_by_col,
                violation_dims=violation_dims,
                network_context=network_context,
            )

            # Apply filters
            if priority:
                actions = [a for a in actions if a["priority"] == priority]
            if table_name:
                actions = [
                    a
                    for a in actions
                    if any(col.startswith(f"{table_name}.") for col in a["affected_columns"])
                ]

            return format_actions_report(source.name, actions, priority, table_name)
    finally:
        manager.close()


async def run_server() -> None:
    """Run the MCP server using stdio transport."""
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main() -> None:
    """Entry point for dataraum-mcp command."""
    import asyncio

    asyncio.run(run_server())


if __name__ == "__main__":
    main()
