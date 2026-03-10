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
    format_export_result,
    format_pipeline_result,
    format_quality_report,
    format_query_result,
)
from dataraum.pipeline.events import EventCallback, EventType, PipelineEvent

_log = logging.getLogger(__name__)

# Prevent background pipeline tasks from being garbage-collected.
_background_tasks: set[asyncio.Task[Any]] = set()


def _make_task_event_callback(
    task: ServerTaskContext,
    loop: asyncio.AbstractEventLoop,
) -> EventCallback:
    """Create a sync event callback that bridges to async task.update_status().

    Called from the pipeline thread (sync context) to push progress updates
    to the MCP task (async context) via run_coroutine_threadsafe.
    """

    def _callback(event: PipelineEvent) -> None:
        try:
            if event.event_type in (
                EventType.PHASE_STARTED,
                EventType.PHASE_COMPLETED,
                EventType.PHASE_FAILED,
                EventType.PIPELINE_COMPLETED,
            ):
                label = _PHASE_LABELS.get(event.phase or "", event.phase or "")
                if event.event_type == EventType.PHASE_STARTED:
                    msg = f"Phase {event.step}/{event.total}: Running {label}"
                elif event.event_type == EventType.PHASE_COMPLETED:
                    msg = f"Phase {event.step}/{event.total}: Completed {label}"
                elif event.event_type == EventType.PHASE_FAILED:
                    msg = f"Phase {event.step}/{event.total}: Failed {label}"
                else:
                    msg = f"Phase {event.step}/{event.total}: Pipeline complete"
                future = asyncio.run_coroutine_threadsafe(
                    task.update_status(msg),
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
                        "gate_mode": {
                            "type": "string",
                            "description": "How to handle entropy gates: skip (default), pause, fail",
                            "enum": ["skip", "pause", "fail"],
                        },
                    },
                    "required": [],
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
                name="get_quality",
                description=(
                    "Get a unified data quality assessment: entropy scores, contract compliance, "
                    "and resolution actions. Returns all three by default, or specific sections "
                    "via the 'include' parameter."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "contract_name": {
                            "type": "string",
                            "description": "Contract to evaluate (e.g., 'aggregation_safe', 'executive_dashboard'). Auto-detects if omitted.",
                        },
                        "table_name": {
                            "type": "string",
                            "description": "Optional: filter to a specific table",
                        },
                        "priority": {
                            "type": "string",
                            "enum": ["high", "medium", "low"],
                            "description": "Optional: filter actions to a specific priority level",
                        },
                        "include": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["entropy", "contract", "actions"],
                            },
                            "description": "Sections to include. Default: all three.",
                        },
                    },
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
            # --- Export tools ---
            Tool(
                name="export",
                description=(
                    "Export query results or arbitrary SQL to a file (CSV, Parquet, or JSON). "
                    "Provide either a question (runs query agent first) or raw SQL. "
                    "Creates a metadata sidecar with provenance: SQL, entropy, assumptions."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question (runs query agent, then exports results)",
                        },
                        "sql": {
                            "type": "string",
                            "description": "Raw SQL to execute and export (alternative to question)",
                        },
                        "output_path": {
                            "type": "string",
                            "description": "Destination file path (e.g., './exports/revenue.csv')",
                        },
                        "format": {
                            "type": "string",
                            "enum": ["csv", "parquet", "json"],
                            "description": "Export format. Default: csv.",
                        },
                    },
                    "required": ["output_path"],
                },
            ),
            # --- Source management tools ---
            Tool(
                name="discover_sources",
                description=(
                    "Scan the workspace for data files (CSV, Parquet, JSON, XLSX) "
                    "and list existing registered sources. Returns file previews "
                    "with column names and row counts."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Root directory to scan. Defaults to current working directory.",
                        },
                        "recursive": {
                            "type": "boolean",
                            "description": "Scan subdirectories. Default: true.",
                        },
                    },
                },
            ),
            Tool(
                name="add_source",
                description=(
                    "Register a new data source. For files, provide a path. "
                    "For databases, provide a backend type (postgres, mysql, sqlite). "
                    "Database sources validate the connection if credentials are available, "
                    "or return setup instructions if not."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Source name (lowercase, a-z/0-9/_, 2-49 chars).",
                            "pattern": "^[a-z][a-z0-9_]{1,48}$",
                        },
                        "path": {
                            "type": "string",
                            "description": "File path. Mutually exclusive with 'backend'.",
                        },
                        "backend": {
                            "type": "string",
                            "enum": ["postgres", "mysql", "sqlite"],
                            "description": "Database backend. Mutually exclusive with 'path'.",
                        },
                        "tables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Optional table filter for database sources.",
                        },
                        "credential_ref": {
                            "type": "string",
                            "description": "Credential lookup key. Defaults to source name.",
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
            path = arguments.get("path")
            source_name = arguments.get("name")
            gate_mode_arg = arguments.get("gate_mode")

            # Validate path if provided
            if path:
                source_path = Path(path)
                if not source_path.exists():
                    return [TextContent(type="text", text=f"Error: Path not found: {path}")]

            display_label = path or "(registered sources)"
            ctx = server.request_context
            experimental: Experimental = ctx.experimental
            if experimental and experimental.is_task:
                # Task-augmented path: return immediately, run in background
                loop = asyncio.get_running_loop()

                async def _work(task: ServerTaskContext) -> CallToolResult:
                    callback = _make_task_event_callback(task, loop)
                    text = await asyncio.to_thread(
                        _analyze, output_dir, path, source_name, callback, gate_mode_arg
                    )
                    return CallToolResult(content=[TextContent(type="text", text=text)])

                return await experimental.run_task(
                    _work,
                    model_immediate_response=(
                        f"Pipeline started for: {display_label}. "
                        f"This typically takes 3–7 minutes depending on file size. "
                        f"Running in the background — status updates will follow."
                    ),
                )
            else:
                # No task API: fire-and-forget, client polls get_context
                bg = asyncio.create_task(
                    _run_analyze_background(output_dir, path, source_name, gate_mode_arg)
                )
                _background_tasks.add(bg)
                bg.add_done_callback(_background_tasks.discard)
                result = (
                    f"Pipeline started for: {display_label}. "
                    f"This typically takes 3–7 minutes depending on file size. "
                    f"Call `get_context` every ~2 minutes to check progress."
                )
        elif name == "get_context":
            result = _get_context(output_dir)
        elif name == "get_quality":
            result = _get_quality(
                output_dir,
                contract_name=arguments.get("contract_name"),
                table_name=arguments.get("table_name"),
                priority=arguments.get("priority"),
                include=arguments.get("include"),
            )
        elif name == "query":
            question = arguments["question"]
            contract_name = arguments.get("contract_name")
            result = _query(output_dir, question, contract_name)
        elif name == "export":
            result = _export(
                output_dir,
                question=arguments.get("question"),
                sql=arguments.get("sql"),
                export_path=arguments.get("output_path", "./export.csv"),
                fmt=arguments.get("format", "csv"),
            )
        elif name == "discover_sources":
            scan_path = arguments.get("path", ".")
            recursive = arguments.get("recursive", True)
            result = _discover_sources(output_dir, scan_path, recursive)
        elif name == "add_source":
            result = _add_source(output_dir, arguments)
        else:
            result = f"Unknown tool: {name}"

        return [TextContent(type="text", text=result)]

    return server


_NO_DATA_MSG = (
    "No analyzed data found at {path}. "
    "This can happen if the output directory was cleared or never created.\n\n"
    "To fix this, run the `analyze` tool:\n"
    "  analyze(path='/path/to/your/data.csv')\n\n"
    "If analysis results existed earlier in this conversation, "
    "re-run `analyze` with the same source path to regenerate them."
)

# Human-readable phase descriptions for progress reporting.
# Keys must match the `name` property of each phase class.
_PHASE_LABELS: dict[str, str] = {
    "import": "Loading data",
    "typing": "Detecting column types",
    "statistics": "Profiling value distributions",
    "correlations": "Checking for correlations",
    "relationships": "Finding table relationships",
    "semantic": "Understanding business meaning (AI step)",
    "temporal": "Detecting date/time patterns",
    "slicing": "Identifying data slices (AI step)",
    "slice_analysis": "Analyzing slice distributions",
    "temporal_slice_analysis": "Detecting distribution drift",
    "statistical_quality": "Running statistical quality checks",
    "enriched_views": "Creating enriched views",
    "column_eligibility": "Evaluating column eligibility",
    "quality_summary": "Summarizing quality findings (AI step)",
    "entropy": "Measuring data uncertainty",
    "entropy_interpretation": "Writing quality summaries (AI step)",
    "business_cycles": "Detecting business cycles (AI step)",
    "validation": "Running validation checks (AI step)",
    "graph_execution": "Executing metric graphs",
}


async def _run_analyze_background(
    output_dir: Path,
    path: str | None,
    source_name: str | None,
    gate_mode: str | None = None,
) -> None:
    """Run _analyze in a background thread, logging errors."""
    try:
        await asyncio.to_thread(_analyze, output_dir, path, source_name, None, gate_mode)
    except Exception:
        _log.exception("Background pipeline failed for %s", path or "(registered sources)")


def _get_pipeline_progress(manager: Any) -> str | None:
    """Check if a pipeline is running and return a progress message.

    Returns:
        Progress message string if running, None if no pipeline is running.
    """
    from sqlalchemy import func, select

    from dataraum.pipeline.db_models import PhaseLog, PipelineRun
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
                select(func.count()).where(PhaseLog.run_id == running_run.run_id)
            ).scalar()
            or 0
        )

        registry = get_registry()
        total_phases = len(registry)

        # Determine currently running phases from dependency graph
        completed_names: set[str] = set()
        log_result = session.execute(
            select(PhaseLog.phase_name).where(PhaseLog.run_id == running_run.run_id)
        )
        for row in log_result:
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
            labels = [_PHASE_LABELS.get(p, p) for p in running_phases]
            current_detail = f" Current: {', '.join(labels)}."

        return f"Phase {completed_count} of {total_phases} complete.{current_detail}"


def _analyze(
    output_dir: Path,
    path: str | None = None,
    name: str | None = None,
    event_callback: EventCallback | None = None,
    gate_mode: str | None = None,
) -> str:
    """Run the pipeline on a data source.

    Args:
        output_dir: Pipeline output directory
        path: Path to CSV/Parquet file or directory. When None, uses registered sources.
        name: Optional source name
        event_callback: Optional callback for pipeline events
        gate_mode: How to handle entropy gates (skip, pause, fail)

    Returns:
        Formatted pipeline result summary
    """
    from dataraum.pipeline.runner import GateMode, RunConfig, run

    source_path: Path | None = None
    if path:
        source_path = Path(path)
        if not source_path.exists():
            return f"Error: Path not found: {path}"

    # Resolve gate mode
    resolved_gate_mode = GateMode.SKIP
    if gate_mode:
        try:
            resolved_gate_mode = GateMode(gate_mode)
        except ValueError:
            pass  # Fall back to skip

    config = RunConfig(
        source_path=source_path,
        output_dir=output_dir,
        source_name=name,
        event_callback=event_callback,
        gate_mode=resolved_gate_mode,
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
                select(Table).where(
                    Table.source_id == source.source_id,
                    Table.layer == "typed",
                )
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
            result = format_context_for_llm(source.name, formatted)

            # Append snippet knowledge base stats if available
            try:
                from dataraum.query.snippet_library import SnippetLibrary

                library = SnippetLibrary(session)
                stats = library.get_stats(schema_mapping_id=source.source_id)
                if stats.get("total_snippets", 0) > 0:
                    kb_lines = [
                        "",
                        "## SQL Knowledge Base",
                        f"- Total snippets: {stats['total_snippets']}",
                        f"- Validated (used at least once): {stats['validated_snippets']}",
                    ]
                    if stats.get("snippets_by_type"):
                        parts = [f"{t}: {c}" for t, c in stats["snippets_by_type"].items()]
                        kb_lines.append(f"- By type: {', '.join(parts)}")
                    if stats.get("cache_hit_rate", 0) > 0:
                        kb_lines.append(f"- Cache hit rate: {stats['cache_hit_rate']:.1%}")
                    result += "\n".join(kb_lines)
            except Exception:
                pass  # Snippet stats are non-critical

            return result
    finally:
        manager.close()


def _get_quality(
    output_dir: Path,
    contract_name: str | None = None,
    table_name: str | None = None,
    priority: str | None = None,
    include: list[str] | None = None,
) -> str:
    """Get unified data quality report: entropy + contract + actions.

    Opens a single connection and assembles all requested sections.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.storage import Source, Table

    sections_to_include = set(include or ["entropy", "contract", "actions"])

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
                select(Table).where(
                    Table.source_id == source.source_id,
                    Table.layer == "typed",
                )
            )
            tables = tables_result.scalars().all()
            table_ids = [t.table_id for t in tables]

            # Build shared network context (used by entropy + contract + actions)
            network_context = None
            column_summaries = None
            if table_ids:
                try:
                    from dataraum.entropy.views.network_context import build_for_network
                    from dataraum.entropy.views.query_context import (
                        network_to_column_summaries,
                    )

                    network_context = build_for_network(session, table_ids)
                    if network_context and network_context.total_columns > 0:
                        column_summaries = network_to_column_summaries(network_context)
                except Exception:
                    _log.debug("Network context unavailable", exc_info=True)

            sections: dict[str, str] = {}

            # --- Entropy section ---
            if "entropy" in sections_to_include:
                sections["entropy"] = _build_entropy_section(
                    session, source, network_context, column_summaries, table_name
                )

            # --- Contract section ---
            if "contract" in sections_to_include:
                sections["contract"] = _build_contract_section(
                    column_summaries, contract_name
                )

            # --- Actions section ---
            if "actions" in sections_to_include:
                sections["actions"] = _build_actions_section(
                    session, source, priority, table_name
                )

            return format_quality_report(sections)
    finally:
        manager.close()


def _build_entropy_section(
    session: Any,
    source: Any,
    network_context: Any,
    column_summaries: Any,
    table_name: str | None,
) -> str:
    """Build entropy section for quality report."""
    from sqlalchemy import select

    from dataraum.entropy.db_models import EntropySnapshotRecord
    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord

    snapshot_result = session.execute(
        select(EntropySnapshotRecord)
        .where(EntropySnapshotRecord.source_id == source.source_id)
        .order_by(EntropySnapshotRecord.snapshot_at.desc())
        .limit(1)
    )
    snapshot = snapshot_result.scalar_one_or_none()

    if not snapshot:
        return "## Entropy\nNo entropy data available. Run entropy phase first."

    interp_query = select(EntropyInterpretationRecord).where(
        EntropyInterpretationRecord.source_id == source.source_id,
        EntropyInterpretationRecord.column_id.isnot(None),
    )
    if table_name:
        interp_query = interp_query.where(
            EntropyInterpretationRecord.table_name == table_name
        )
    interp_query = interp_query.order_by(
        EntropyInterpretationRecord.table_name,
        EntropyInterpretationRecord.column_name,
    )
    interpretations = session.execute(interp_query).scalars().all()

    # Compute per-dimension averages
    dimension_scores: dict[str, float] | None = None
    if column_summaries:
        dim_totals: dict[str, list[float]] = {}
        for summary in column_summaries.values():
            for dim_path, score in summary.dimension_scores.items():
                dim_totals.setdefault(dim_path, []).append(score)
        dimension_scores = {
            dim: sum(scores) / len(scores) for dim, scores in dim_totals.items()
        }

    result = format_entropy_summary(
        source.name, snapshot, interpretations, table_name, dimension_scores
    )

    if network_context and network_context.total_columns > 0:
        from dataraum.entropy.views.network_context import format_network_context

        result += "\n\n" + format_network_context(network_context)

    return result


def _build_contract_section(
    column_summaries: Any,
    contract_name: str | None,
) -> str:
    """Build contract section for quality report."""
    from dataraum.entropy.contracts import evaluate_contract, get_contract, list_contracts

    if not column_summaries:
        return "## Contract Evaluation\nNo data available for contract evaluation."

    # Auto-detect contract if not specified
    resolved_name = contract_name
    if not resolved_name:
        contracts = list_contracts()
        resolved_name = contracts[0]["name"] if contracts else "aggregation_safe"

    profile = get_contract(resolved_name)
    if profile is None:
        return f"## Contract Evaluation\nContract not found: {resolved_name}"

    evaluation = evaluate_contract(column_summaries, resolved_name)
    return format_contract_evaluation(evaluation, profile)


def _build_actions_section(
    session: Any,
    source: Any,
    priority: str | None,
    table_name: str | None,
) -> str:
    """Build actions section for quality report."""
    from dataraum.entropy.actions import load_actions

    actions = load_actions(session, source)

    if not actions:
        return "## Resolution Actions\nNo actions available."

    if priority:
        actions = [a for a in actions if a["priority"] == priority]
    if table_name:
        actions = [
            a
            for a in actions
            if any(
                col == table_name or col.startswith(f"{table_name}.")
                for col in a["affected_columns"]
            )
        ]

    return format_actions_report(source.name, actions, priority, table_name)


def _query(
    output_dir: Path,
    question: str,
    contract_name: str | None = None,
) -> str:
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
                )

            if not result.success or not result.value:
                return f"Error: {result.error}"

            return format_query_result(result.value)
    finally:
        manager.close()


def _get_or_create_manager(output_dir: Path) -> Any:
    """Get a ConnectionManager, creating the database if it doesn't exist yet."""
    from dataraum.core.connections import ConnectionConfig, ConnectionManager

    config = ConnectionConfig.for_directory(output_dir)
    config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    manager = ConnectionManager(config)
    manager.initialize()
    return manager


def _discover_sources(output_dir: Path, scan_path: str, recursive: bool) -> str:
    """Scan workspace for data files and list existing registered sources."""
    import json

    from dataraum.sources.discovery import discover_sources

    root = Path(scan_path).resolve()
    if not root.is_dir():
        return f"Error: Directory not found: {scan_path}"

    # Get existing source names from the database if available
    existing_names: list[str] = []
    try:
        from sqlalchemy import select

        from dataraum.core.connections import get_manager_for_directory
        from dataraum.storage import Source

        manager = get_manager_for_directory(output_dir)
        try:
            with manager.session_scope() as session:
                sources = (
                    session.execute(select(Source.name).where(Source.archived_at.is_(None)))
                    .scalars()
                    .all()
                )
                existing_names = list(sources)
        finally:
            manager.close()
    except FileNotFoundError:
        pass  # No database yet — that's fine for discovery

    result = discover_sources(root, recursive=recursive, existing_sources=existing_names)

    # Format as structured output for LLM
    output: dict[str, Any] = {
        "scan_root": result.scan_root,
        "files": [
            {
                "path": f.path,
                "format": f.format,
                "size_bytes": f.size_bytes,
                "columns": f.columns,
                "row_count_estimate": f.row_count_estimate,
            }
            for f in result.files
        ],
        "existing_sources": result.existing_sources,
    }

    if not result.files and not result.existing_sources:
        return f"No data files found in {scan_path}. Try a different directory or add files."

    return json.dumps(output, indent=2)


def _add_source(output_dir: Path, arguments: dict[str, Any]) -> str:
    """Register a new data source."""
    import json

    from dataraum.core.credentials import CredentialChain
    from dataraum.sources.manager import SourceManager

    name = arguments["name"]
    path = arguments.get("path")
    backend = arguments.get("backend")

    if not path and not backend:
        return "Error: Provide either 'path' (for files) or 'backend' (for databases)."
    if path and backend:
        return "Error: Provide 'path' or 'backend', not both."

    try:
        manager = _get_or_create_manager(output_dir)
    except Exception as e:
        return f"Error initializing database: {e}"

    try:
        credential_chain = CredentialChain()

        with manager.session_scope() as session:
            if backend:
                with manager.duckdb_cursor() as cursor:
                    src_mgr = SourceManager(
                        session=session,
                        credential_chain=credential_chain,
                        duckdb_conn=cursor,
                    )
                    tables_arg = arguments.get("tables")
                    credential_ref = arguments.get("credential_ref")
                    result = src_mgr.add_database_source(
                        name, backend, tables=tables_arg, credential_ref=credential_ref
                    )
            else:
                src_mgr = SourceManager(
                    session=session,
                    credential_chain=credential_chain,
                )
                assert path is not None  # guarded by validation above
                result = src_mgr.add_file_source(name, path)

            if not result.success:
                return f"Error: {result.error}"

            session.commit()

            info = result.unwrap()
            output: dict[str, Any] = {
                "source": {
                    "name": info.name,
                    "type": info.source_type,
                    "status": info.status,
                }
            }
            if info.path:
                output["source"]["path"] = info.path
            if info.columns:
                output["source"]["preview"] = {
                    "columns": info.columns,
                    "row_count_estimate": info.row_count_estimate,
                }
            if info.credential_source:
                output["source"]["credential_source"] = info.credential_source
            if info.discovered_schema:
                output["source"]["schema_discovered"] = info.discovered_schema
            if info.credential_instructions:
                output["credential_instructions"] = info.credential_instructions

            return json.dumps(output, indent=2)
    finally:
        manager.close()


def _export(
    output_dir: Path,
    question: str | None = None,
    sql: str | None = None,
    export_path: str = "./export.csv",
    fmt: str = "csv",
) -> str:
    """Export query results or SQL output to a file."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.export import ExportFormat, export_query_result, export_sql
    from dataraum.storage import Source

    if not question and not sql:
        return "Error: Provide either 'question' or 'sql'."
    if question and sql:
        return "Error: Provide 'question' or 'sql', not both."

    if fmt not in ("csv", "parquet", "json"):
        return f"Error: Unknown format '{fmt}'. Use csv, parquet, or json."

    export_fmt: ExportFormat = fmt  # type: ignore[assignment]
    dest = Path(export_path)

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        if question:
            # Run query agent, then export the result
            from dataraum.query import answer_question

            with manager.session_scope() as session:
                sources = session.execute(select(Source)).scalars().all()
                if not sources:
                    return "Error: No sources found"
                source = sources[0]

                with manager.duckdb_cursor() as cursor:
                    query_result = answer_question(
                        question=question,
                        session=session,
                        duckdb_conn=cursor,
                        source_id=source.source_id,
                    )

                if not query_result.success or not query_result.value:
                    return f"Error: Query failed: {query_result.error}"

                qr = query_result.value
                if not qr.data or not qr.columns:
                    return f"Error: Query returned no tabular data. Answer: {qr.answer}"

                exported = export_query_result(qr, dest, fmt=export_fmt)
                sidecar = exported.with_suffix(exported.suffix + ".meta.json")
                return format_export_result(
                    str(exported), fmt, len(qr.data), str(sidecar)
                )
        else:
            # Export raw SQL
            assert sql is not None
            with manager.duckdb_cursor() as cursor:
                exported = export_sql(sql, cursor, dest, fmt=export_fmt)
                sidecar = exported.with_suffix(exported.suffix + ".meta.json")
                # Get row count from sidecar
                import json as json_mod

                with open(sidecar) as f:
                    meta = json_mod.load(f)
                return format_export_result(
                    str(exported), fmt, meta.get("row_count", 0), str(sidecar)
                )
    except Exception as e:
        return f"Error: Export failed: {e}"
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
