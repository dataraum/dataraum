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
    format_zone_status,
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
            # --- Zone status tool ---
            Tool(
                name="get_zone_status",
                description=(
                    "Get the current pipeline zone status: which gate was last measured, "
                    "per-column scores, violations against the active contract, available "
                    "fix actions, and skipped detectors. Use this after `analyze` completes "
                    "to understand what needs fixing before calling `continue_pipeline`."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["gate"],
                    "properties": {
                        "gate": {
                            "type": "string",
                            "enum": ["quality_review", "analysis_review"],
                            "description": "Which gate to inspect. quality_review = Gate 1 (Zone 1, after semantic). analysis_review = Gate 2 (Zone 2, after quality_summary).",
                        },
                        "contract_name": {
                            "type": "string",
                            "description": "Contract to evaluate against (e.g., 'aggregation_safe'). Auto-detects if omitted.",
                        },
                    },
                },
            ),
            # --- Continue pipeline tool ---
            Tool(
                name="continue_pipeline",
                description=(
                    "Resume the pipeline from the current position to the next zone boundary. "
                    "After inspecting gate scores with `get_zone_status` and applying fixes "
                    "with `apply_fix`, call this to advance to the next zone. "
                    "Skips already-completed phases automatically."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["target_gate"],
                    "properties": {
                        "target_gate": {
                            "type": "string",
                            "enum": ["analysis_review", "end"],
                            "description": (
                                "Where to stop: "
                                "'analysis_review' = run through Gate 2 (Zone 2). "
                                "'end' = run through the end of the pipeline (Zone 3)."
                            ),
                        },
                        "source_path": {
                            "type": "string",
                            "description": "Path to original source data (needed for pipeline re-runs).",
                        },
                    },
                },
                execution=ToolExecution(taskSupport="optional"),
            ),
            # --- Agent-driven fix tools ---
            Tool(
                name="get_fix_proposal",
                description=(
                    "Start an agent-driven fix for a specific violation. "
                    "The DataRaum document agent analyzes the violation, inspects the data, "
                    "and generates targeted questions that YOU (the calling agent) should answer "
                    "based on your understanding of the data. Returns questions plus context. "
                    "After answering, call `submit_fix_answers` with your responses."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["gate", "dimension"],
                    "properties": {
                        "gate": {
                            "type": "string",
                            "enum": ["quality_review", "analysis_review"],
                            "description": "Which gate this violation was measured at.",
                        },
                        "dimension": {
                            "type": "string",
                            "description": (
                                "The dimension path of the violation to fix "
                                "(e.g., 'value.temporal.temporal_drift'). "
                                "Get this from get_zone_status."
                            ),
                        },
                    },
                },
            ),
            Tool(
                name="submit_fix_answers",
                description=(
                    "Submit your answers to the document agent's questions (from get_fix_proposal). "
                    "The agent interprets your answers, picks the best fix action, validates "
                    "parameters, and applies the fix. Returns the interpretation and result. "
                    "Use `query` or `get_context` first if you need more data to answer well."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["gate", "dimension", "answers"],
                    "properties": {
                        "gate": {
                            "type": "string",
                            "enum": ["quality_review", "analysis_review"],
                            "description": "Same gate as the get_fix_proposal call.",
                        },
                        "dimension": {
                            "type": "string",
                            "description": "Same dimension as the get_fix_proposal call.",
                        },
                        "answers": {
                            "type": "string",
                            "description": (
                                "Your answers to the questions, formatted as:\n"
                                "Q: <question 1>\nA: <your answer>\n\n"
                                "Q: <question 2>\nA: <your answer>"
                            ),
                        },
                        "source_path": {
                            "type": "string",
                            "description": "Path to original source data (needed for pipeline re-runs after fix).",
                        },
                    },
                },
            ),
            # --- Low-level fix tool ---
            Tool(
                name="apply_fix",
                description=(
                    "Apply data quality fixes and re-run affected pipeline phases. "
                    "Takes a list of fix documents (from get_quality actions), applies them, "
                    "cascade-cleans affected phases, re-runs the pipeline, and returns "
                    "before/after gate score deltas. This may take several minutes."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["fixes"],
                    "properties": {
                        "fixes": {
                            "type": "array",
                            "description": "List of fix documents to apply",
                            "items": {
                                "type": "object",
                                "required": [
                                    "target",
                                    "action",
                                    "table_name",
                                    "dimension",
                                    "payload",
                                ],
                                "properties": {
                                    "target": {
                                        "type": "string",
                                        "enum": ["config", "metadata", "data"],
                                        "description": "Which interpreter handles this fix",
                                    },
                                    "action": {
                                        "type": "string",
                                        "description": "Fix action name (e.g. accept_finding, set_column_type)",
                                    },
                                    "table_name": {
                                        "type": "string",
                                        "description": "Table this fix applies to",
                                    },
                                    "column_name": {
                                        "type": "string",
                                        "description": "Column this fix applies to (optional for table-scoped)",
                                    },
                                    "dimension": {
                                        "type": "string",
                                        "description": "Entropy dimension this addresses",
                                    },
                                    "payload": {
                                        "type": "object",
                                        "description": "Target-specific fix data",
                                    },
                                    "description": {
                                        "type": "string",
                                        "description": "Human-readable summary of what this fix does",
                                    },
                                },
                            },
                        },
                        "source_path": {
                            "type": "string",
                            "description": "Path to original source data (needed for pipeline re-runs)",
                        },
                    },
                },
                execution=ToolExecution(taskSupport="optional"),
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
        elif name == "get_zone_status":
            result = _get_zone_status(
                output_dir,
                gate=arguments["gate"],
                contract_name=arguments.get("contract_name"),
            )
        elif name == "get_fix_proposal":
            result = _get_fix_proposal(
                output_dir,
                gate=arguments["gate"],
                dimension=arguments["dimension"],
            )
        elif name == "submit_fix_answers":
            result = _submit_fix_answers(
                output_dir,
                gate=arguments["gate"],
                dimension=arguments["dimension"],
                answers=arguments["answers"],
                source_path=arguments.get("source_path"),
            )
        elif name == "continue_pipeline":
            target_gate = arguments["target_gate"]
            cont_source_path: str | None = arguments.get("source_path")
            ctx = server.request_context
            cont_experimental: Experimental = ctx.experimental
            if cont_experimental and cont_experimental.is_task:
                loop = asyncio.get_running_loop()

                async def _cont_work(task: ServerTaskContext) -> CallToolResult:
                    callback = _make_task_event_callback(task, loop)
                    text = await asyncio.to_thread(
                        _continue_pipeline, output_dir, target_gate, cont_source_path, callback
                    )
                    return CallToolResult(content=[TextContent(type="text", text=text)])

                return await cont_experimental.run_task(
                    _cont_work,
                    model_immediate_response=(
                        f"Continuing pipeline to {target_gate}. "
                        f"Completed phases will be skipped. Progress updates will follow."
                    ),
                )
            else:
                result = _continue_pipeline(output_dir, target_gate, cont_source_path)
        elif name == "apply_fix":
            ctx = server.request_context
            fix_experimental: Experimental = ctx.experimental
            fix_source_path: str | None = arguments.get("source_path")
            if fix_experimental and fix_experimental.is_task:
                loop = asyncio.get_running_loop()

                async def _fix_work(task: ServerTaskContext) -> CallToolResult:
                    await task.update_status("Applying fixes and re-running pipeline...")
                    text = await asyncio.to_thread(
                        _apply_fix, output_dir, arguments["fixes"], fix_source_path
                    )
                    return CallToolResult(content=[TextContent(type="text", text=text)])

                return await fix_experimental.run_task(
                    _fix_work,
                    model_immediate_response=(
                        "Applying fixes and re-running affected pipeline phases. "
                        "This may take several minutes."
                    ),
                )
            else:
                result = _apply_fix(output_dir, arguments["fixes"], fix_source_path)
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
    "analysis_review": "Reviewing enrichment analysis quality",
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
                sections["contract"] = _build_contract_section(column_summaries, contract_name)

            # --- Actions section ---
            if "actions" in sections_to_include:
                sections["actions"] = _build_actions_section(session, source, priority, table_name)

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
        interp_query = interp_query.where(EntropyInterpretationRecord.table_name == table_name)
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
        dimension_scores = {dim: sum(scores) / len(scores) for dim, scores in dim_totals.items()}

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


# ---------------------------------------------------------------------------
# Zone status
# ---------------------------------------------------------------------------

# Maps gate phase names to (zone_name, gate_label).
_GATE_ZONES: dict[str, tuple[str, str]] = {
    "quality_review": ("foundation", "Gate 1"),
    "analysis_review": ("enrichment", "Gate 2"),
}


def _get_zone_status(
    output_dir: Path,
    gate: str,
    contract_name: str | None = None,
) -> str:
    """Read persisted gate scores and format zone status for an agent.

    Args:
        output_dir: Pipeline output directory.
        gate: Gate phase to inspect (quality_review or analysis_review).
        contract_name: Contract to evaluate against. Auto-detects if omitted.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.entropy.contracts import get_contract, get_contracts
    from dataraum.entropy.detectors.base import get_default_registry
    from dataraum.entropy.gate import assess_contracts, match_threshold
    from dataraum.pipeline.db_models import PhaseLog
    from dataraum.storage import Source

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        with manager.session_scope() as session:
            # Find source
            source = session.execute(select(Source)).scalar_one_or_none()
            if not source:
                return "Error: No sources found."

            # Find latest PhaseLog for the requested gate
            gate_phase = gate
            log = session.execute(
                select(PhaseLog)
                .where(
                    PhaseLog.source_id == source.source_id,
                    PhaseLog.phase_name == gate_phase,
                    PhaseLog.status == "completed",
                )
                .order_by(PhaseLog.completed_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            if not log or not log.outputs:
                return (
                    f"No gate measurement found for `{gate_phase}`. "
                    f"The pipeline may not have reached this gate yet.\n\n"
                    f"Run `analyze` first, then call `get_zone_status(gate='{gate_phase}')`."
                )

            outputs = log.outputs
            scores = log.entropy_scores or {}
            column_details = outputs.get("gate_column_details", {})
            id_map = outputs.get("detector_id_map", {})

            # Resolve contract
            if contract_name:
                contract = get_contract(contract_name)
            else:
                contracts = get_contracts()
                contract = next(iter(contracts.values()), None) if contracts else None

            if not contract:
                return "Error: No contract found."

            thresholds = contract.dimension_thresholds

            # Assess violations (accepted targets excluded via contract overrule)
            col_evidence = outputs.get("gate_column_evidence", {})
            issues = assess_contracts(
                scores,
                thresholds,
                column_details,
                gate_phase,
                column_evidence=col_evidence,
            )

            # Build violation entries with fix actions from detector registry
            registry = get_default_registry()
            violation_dims = {i.dimension_path for i in issues}
            violations = []
            for issue in issues:
                detector_id = id_map.get(
                    issue.dimension_path, issue.dimension_path.rsplit(".", 1)[-1]
                )
                # Get fix actions from detector registry
                fix_actions: list[str] = []
                detector = registry.detectors.get(detector_id)
                if detector:
                    fix_actions = [s.action for s in detector.fix_schemas]

                # Build per-target scores from all detail dicts
                affected: list[str] = issue.affected_targets

                violations.append(
                    {
                        "dimension_path": issue.dimension_path,
                        "detector_id": detector_id,
                        "score": issue.score,
                        "threshold": issue.threshold,
                        "fix_actions": fix_actions,
                        "affected_targets": affected,
                    }
                )

            # Build passing entries
            passing = []
            for dim_path, score in sorted(scores.items()):
                if dim_path in violation_dims:
                    continue
                threshold = match_threshold(dim_path, thresholds)
                if threshold is not None:
                    detector_id = id_map.get(dim_path, dim_path.rsplit(".", 1)[-1])
                    passing.append(
                        {
                            "dimension_path": dim_path,
                            "detector_id": detector_id,
                            "score": score,
                            "threshold": threshold,
                        }
                    )

            # Determine skipped detectors
            # Detectors whose required_analyses aren't satisfied at this gate
            _gate_analyses = {
                "quality_review": {"TYPING", "STATISTICS", "RELATIONSHIPS", "SEMANTIC"},
                "analysis_review": {
                    "TYPING",
                    "STATISTICS",
                    "RELATIONSHIPS",
                    "SEMANTIC",
                    "ENRICHED_VIEWS",
                    "SLICING",
                    "CORRELATIONS",
                    "TEMPORAL_SLICING",
                    "QUALITY_SUMMARY",
                },
            }
            available = _gate_analyses.get(gate_phase, set())
            measured_ids = {id_map.get(dp, dp.rsplit(".", 1)[-1]) for dp in scores}
            skipped = []
            for d in registry.get_all_detectors():
                if d.detector_id in measured_ids:
                    continue
                missing = [a.value for a in d.required_analyses if a.value.upper() not in available]
                if missing:
                    skipped.append(
                        {
                            "detector_id": d.detector_id,
                            "reason": f"missing analyses: {', '.join(missing)}",
                        }
                    )

            zone_name, gate_label = _GATE_ZONES.get(gate_phase, ("unknown", "Gate ?"))
            return format_zone_status(
                zone_name,
                gate_label,
                gate_phase,
                violations,
                passing,
                skipped,
                contract.name,
            )
    finally:
        manager.close()


# ---------------------------------------------------------------------------
# Agent-driven fix flow
# ---------------------------------------------------------------------------


def _build_mcp_gate_context(
    session: Any,
    source_id: str,
    dimension: str,
    gate_phase: str,
    outputs: dict[str, Any],
    scores: dict[str, float],
) -> str:
    """Build context for the document agent from persisted gate data.

    Same information as gate_handler.build_gate_context() but reads from
    PhaseLog outputs instead of a live PipelineEvent.
    """
    from dataraum.cli.gate_handler import _build_data_profile
    from dataraum.entropy.contracts import get_contracts
    from dataraum.entropy.detectors.base import get_default_registry
    from dataraum.entropy.gate import match_threshold

    contracts = get_contracts()
    contract = next(iter(contracts.values()), None)
    thresholds = contract.dimension_thresholds if contract else {}

    score = scores.get(dimension, 0.0)
    threshold = match_threshold(dimension, thresholds) or 0.0

    # Get affected targets from gate details
    column_details = outputs.get("gate_column_details", {})
    table_details = outputs.get("gate_table_details", {})
    col_scores = column_details.get(dimension, {})
    tbl_scores = table_details.get(dimension, {})
    all_scores = {**col_scores, **tbl_scores}
    affected_targets = [
        t for t, s in sorted(all_scores.items(), key=lambda x: -x[1]) if s > threshold
    ]

    # Get fix actions from detector registry
    id_map = outputs.get("detector_id_map", {})
    detector_id = id_map.get(dimension, dimension.rsplit(".", 1)[-1])
    registry = get_default_registry()
    detector = registry.detectors.get(detector_id)

    sections: list[str] = []

    # Section 1: Available actions
    action_lines = [
        "<available_actions>",
        f"Dimension: {dimension}",
        f"Score: {score:.2f} (threshold: {threshold:.2f})",
        f"Affected columns: {', '.join(affected_targets)}",
        "",
        "Choose the BEST action for each violating target.",
        "Prefer corrective actions (recalculate, override, add pattern) over accept_finding.",
        "",
    ]
    if detector:
        for i, schema in enumerate(detector.fix_schemas, 1):
            action_lines.append(f"--- Action {i}: {schema.action} ---")
            if schema.requires_rerun:
                action_lines.append(f"Phase: {schema.requires_rerun}")
            if schema.guidance:
                action_lines.append(f"Guidance: {schema.guidance}")
            if schema.fields:
                action_lines.append("Expected parameters:")
                for fname, fdef in schema.fields.items():
                    line = f"  {fname} ({fdef.type}): {fdef.description}"
                    if fdef.enum_values:
                        line += f" [options: {', '.join(fdef.enum_values)}]"
                    action_lines.append(line)
            action_lines.append("")
    action_lines.append("</available_actions>")
    sections.append("\n".join(action_lines))

    # Section 1b: Detector-specific triage guidance
    if detector and detector.triage_guidance:
        sections.append(f"<triage_guidance>\n{detector.triage_guidance}\n</triage_guidance>")

    # Section 2: Entropy evidence with per-column component breakdown
    col_evidence = outputs.get("gate_column_evidence", {}).get(dimension, {})
    evidence_lines = [
        "<entropy_evidence>",
        f"Detector: {detector_id}",
        f"Score: {score:.2f}",
        f"Threshold: {threshold:.2f}",
    ]
    if all_scores:
        evidence_lines.append("")
        evidence_lines.append("Per-column breakdown:")
        for target, col_score in sorted(all_scores.items(), key=lambda x: -x[1]):
            label = "VIOLATING" if col_score > threshold else "passing"
            line = f"  {target}: {col_score:.2f} ({label})"
            ev = col_evidence.get(target, {})
            if ev:
                components = []
                for k in ("ri_entropy", "card_entropy", "semantic_entropy"):
                    if k in ev:
                        components.append(f"{k}={ev[k]:.2f}")
                if ev.get("accepted"):
                    components.append("ACCEPTED")
                if components:
                    line += f" [{', '.join(components)}]"
            evidence_lines.append(line)
    evidence_lines.append("</entropy_evidence>")
    sections.append("\n".join(evidence_lines))

    # Section 3: Data profile
    data_section = _build_data_profile(session, source_id, affected_targets)
    if data_section:
        sections.append(data_section)

    return "\n\n".join(sections)


def _get_fix_proposal(
    output_dir: Path,
    gate: str,
    dimension: str,
) -> str:
    """Generate a batch action plan for a specific violation using the document agent.

    Proposes one action per violating target with ready-to-apply FixDocuments.
    Works for both single-target and multi-target dimensions.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.pipeline.db_models import PhaseLog
    from dataraum.storage import Source

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        with manager.session_scope() as session:
            source = session.execute(select(Source)).scalar_one_or_none()
            if not source:
                return "Error: No sources found."

            log = session.execute(
                select(PhaseLog)
                .where(
                    PhaseLog.source_id == source.source_id,
                    PhaseLog.phase_name == gate,
                    PhaseLog.status == "completed",
                )
                .order_by(PhaseLog.completed_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            if not log or not log.outputs:
                return f"No gate measurement found for `{gate}`."

            outputs = log.outputs
            scores = log.entropy_scores or {}

            if dimension not in scores:
                return f"Dimension `{dimension}` not found in gate scores. Available: {', '.join(sorted(scores))}"

            # Build context
            context = _build_mcp_gate_context(
                session,
                source.source_id,
                dimension,
                gate,
                outputs,
                scores,
            )

            # Detect multi-target: check if >1 target violates
            from dataraum.entropy.contracts import get_contracts
            from dataraum.entropy.gate import match_threshold

            contracts = get_contracts()
            contract = next(iter(contracts.values()), None)
            thresholds = contract.dimension_thresholds if contract else {}
            threshold = match_threshold(dimension, thresholds) or 0.0

            column_details = outputs.get("gate_column_details", {})
            table_details = outputs.get("gate_table_details", {})
            all_col_scores = {
                **column_details.get(dimension, {}),
                **table_details.get(dimension, {}),
            }
            affected = [t for t, s in all_col_scores.items() if s > threshold]

            from dataraum.cli.commands.fix import _create_document_agent

            agent = _create_document_agent()

            return _format_batch_proposal(
                agent, context, dimension, gate, outputs, affected, all_col_scores
            )
    finally:
        manager.close()


def _format_batch_proposal(
    agent: Any,
    context: str,
    dimension: str,
    gate: str,
    outputs: dict[str, Any],
    affected: list[str],
    col_scores: dict[str, float],
) -> str:
    """Format a batch action plan proposal for multi-target dimensions.

    Returns ready-to-apply FixDocuments so the outer agent can pass them
    directly to apply_fix without a submit_fix_answers round trip.
    """
    import json

    from dataraum.entropy.detectors.base import get_default_registry

    plan_result = agent.generate_batch_plan(context)
    if not plan_result.success:
        return f"Error generating batch plan: {plan_result.error}"

    plan = plan_result.unwrap()
    if not plan.items:
        return "No actions proposed for this dimension."

    registry = get_default_registry()
    id_map = outputs.get("detector_id_map", {})

    # Build FixDocuments from plan items
    fix_docs: list[dict[str, Any]] = []
    for item in plan.items:
        fix_doc = _build_fix_doc_from_plan_item(item, dimension, registry, id_map, col_scores)
        if fix_doc:
            fix_docs.append(fix_doc)

    # Format response
    lines = [f"# Batch Fix Plan: {dimension}", ""]
    lines.append(f"**{plan.summary}**\n")

    lines.append("| Target | Score | Action | Reason |")
    lines.append("|--------|-------|--------|--------|")
    for item in plan.items:
        score = col_scores.get(item.target, 0.0)
        lines.append(
            f"| {item.target} | {score:.2f} | `{item.recommended_action}` | {item.reason} |"
        )
    lines.append("")

    if plan.follow_up_questions:
        lines.append("## Follow-up Questions")
        lines.append(
            "These are optional — answer them via `submit_fix_answers` if you want "
            "to add context (e.g., reason for acceptance).\n"
        )
        for i, q in enumerate(plan.follow_up_questions, 1):
            lines.append(f"**{i}. {q.question}**")
            if q.question_type == "multiple_choice" and q.choices:
                for j, choice in enumerate(q.choices, 1):
                    lines.append(f"   {j}. {choice}")
            lines.append("")

    lines.append("## Ready-to-Apply Fix Documents")
    lines.append(f"\nPass these {len(fix_docs)} fixes to `apply_fix` to apply them all at once:")
    lines.append("```json")
    lines.append(json.dumps(fix_docs, indent=2, default=str))
    lines.append("```")
    lines.append("")
    lines.append("## Next Steps")
    lines.append("- Review the plan above, then call `apply_fix(fixes=[...])` with the JSON above")
    lines.append(f'- Or call `get_zone_status(gate="{gate}")` to re-check the current state first')

    return "\n".join(lines)


def _build_fix_doc_from_plan_item(
    item: Any,
    dimension: str,
    registry: Any,
    id_map: dict[str, str],
    col_scores: dict[str, float],
) -> dict[str, Any] | None:
    """Build a FixDocument dict from a batch plan item."""
    from dataraum.cli.gate_handler import _target_to_column_ref

    action_name = item.recommended_action
    schema = registry.get_fix_schema(action_name, dimension)

    col_ref = _target_to_column_ref(item.target)
    parts = col_ref.split(".", 1)
    table_name = parts[0]
    column_name = parts[1] if len(parts) > 1 else None

    fix_doc: dict[str, Any] = {
        "target": schema.target if schema else "config",
        "action": action_name,
        "table_name": table_name,
        "column_name": column_name,
        "dimension": dimension,
        "description": item.reason,
        "payload": {},
    }

    if schema and schema.target == "config":
        value: Any = item.parameters
        if schema.operation == "append" and not schema.fields:
            value = f"{table_name}.{column_name}" if column_name else table_name
        elif schema.operation == "append":
            # append with fields (e.g., accept_finding with reason)
            # value is the column reference; parameters go into the appended entry
            value = f"{table_name}.{column_name}" if column_name else table_name

        fix_doc["payload"] = {
            "config_path": schema.config_path,
            "key_path": list(schema.key_path or []),
            "operation": schema.operation or "set",
            "value": value,
        }

    return fix_doc


def _submit_fix_answers(
    output_dir: Path,
    gate: str,
    dimension: str,
    answers: str,
    source_path: str | None = None,
) -> str:
    """Interpret agent answers and return ready-to-use fix documents.

    Re-generates the batch plan and threads user answers (e.g., acceptance
    reason) into all plan items, returning FixDocuments.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.entropy.detectors.base import get_default_registry
    from dataraum.pipeline.db_models import PhaseLog
    from dataraum.storage import Source

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _NO_DATA_MSG.format(path=output_dir)

    try:
        with manager.session_scope() as session:
            source = session.execute(select(Source)).scalar_one_or_none()
            if not source:
                return "Error: No sources found."

            log = session.execute(
                select(PhaseLog)
                .where(
                    PhaseLog.source_id == source.source_id,
                    PhaseLog.phase_name == gate,
                    PhaseLog.status == "completed",
                )
                .order_by(PhaseLog.completed_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            if not log or not log.outputs:
                return f"No gate measurement found for `{gate}`."

            outputs = log.outputs
            scores = log.entropy_scores or {}

            # Rebuild context (same as get_fix_proposal)
            context = _build_mcp_gate_context(
                session,
                source.source_id,
                dimension,
                gate,
                outputs,
                scores,
            )

            column_details = outputs.get("gate_column_details", {})
            table_details = outputs.get("gate_table_details", {})
            all_col_scores = {
                **column_details.get(dimension, {}),
                **table_details.get(dimension, {}),
            }

            from dataraum.cli.commands.fix import _create_document_agent

            agent = _create_document_agent()
            registry = get_default_registry()
            id_map = outputs.get("detector_id_map", {})

            return _submit_batch_answers(
                agent,
                context,
                dimension,
                gate,
                outputs,
                all_col_scores,
                answers,
                registry,
                id_map,
            )
    finally:
        manager.close()


def _submit_batch_answers(
    agent: Any,
    context: str,
    dimension: str,
    gate: str,
    outputs: dict[str, Any],
    col_scores: dict[str, float],
    answers: str,
    registry: Any,
    id_map: dict[str, str],
) -> str:
    """Handle batch answer submission for multi-target dimensions.

    Re-generates the batch plan, threads user answers into parameters,
    and returns multiple FixDocuments.
    """
    import json

    plan_result = agent.generate_batch_plan(context)
    if not plan_result.success:
        return f"Error re-generating batch plan: {plan_result.error}"

    plan = plan_result.unwrap()
    if not plan.items:
        return "No actions proposed."

    # Extract reason from answers (most common follow-up parameter)
    reason = _extract_reason_from_answers(answers)

    fix_docs: list[dict[str, Any]] = []
    for item in plan.items:
        if reason and "reason" not in item.parameters:
            item.parameters["reason"] = reason
        fix_doc = _build_fix_doc_from_plan_item(item, dimension, registry, id_map, col_scores)
        if fix_doc:
            fix_docs.append(fix_doc)

    lines = [
        "## Batch Fix Interpretation",
        "",
        f"**Plan:** {plan.summary}",
        f"**Items:** {len(fix_docs)} fixes",
        "",
        "## Ready-to-Apply Fix Documents",
        "",
        f"Pass these {len(fix_docs)} fixes to `apply_fix` to apply them all at once:",
        "```json",
        json.dumps(fix_docs, indent=2, default=str),
        "```",
        "",
        "## Next Steps",
        "- Call `apply_fix(fixes=[...])` with the JSON above",
        f'- Or call `get_zone_status(gate="{gate}")` to re-check first',
    ]
    return "\n".join(lines)


def _extract_reason_from_answers(answers: str) -> str:
    """Extract a reason string from formatted Q&A answers.

    Looks for an answer to a question containing "reason" or "why".
    """
    current_q = ""
    for line in answers.split("\n"):
        stripped = line.strip()
        if stripped.startswith("Q:"):
            current_q = stripped.lower()
        elif stripped.startswith("A:") and ("reason" in current_q or "why" in current_q):
            return stripped[2:].strip()
    # Fallback: if only one Q&A pair, use the answer
    lines = [ln.strip() for ln in answers.split("\n") if ln.strip().startswith("A:")]
    if len(lines) == 1:
        return lines[0][2:].strip()
    return ""


def _continue_pipeline(
    output_dir: Path,
    target_gate: str,
    source_path: str | None = None,
    event_callback: EventCallback | None = None,
) -> str:
    """Resume the pipeline from current position to the next zone boundary.

    Args:
        output_dir: Pipeline output directory (must already exist from prior run).
        target_gate: Where to stop — 'analysis_review' (Gate 2) or 'end' (full pipeline).
        source_path: Path to original source data. Needed if pipeline re-run requires it.
        event_callback: Optional callback for progress updates.
    """
    from dataraum.pipeline.runner import GateMode, RunConfig, run

    # Map target_gate to target_phase (None = run to end)
    target_phase: str | None = None if target_gate == "end" else target_gate

    sp: Path | None = Path(source_path) if source_path else None

    config = RunConfig(
        source_path=sp,
        output_dir=output_dir,
        target_phase=target_phase,
        event_callback=event_callback,
        gate_mode=GateMode.SKIP,
    )

    result = run(config)

    if not result.success or not result.value:
        return f"Error: Pipeline failed: {result.error}"

    return format_pipeline_result(result.value)


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
                return format_export_result(str(exported), fmt, len(qr.data), str(sidecar))
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


def _apply_fix(
    output_dir: Path,
    fixes: list[dict[str, Any]],
    source_path: str | None = None,
) -> str:
    """Apply fixes and re-run affected pipeline phases.

    Args:
        output_dir: Pipeline output directory.
        fixes: List of fix document dicts from the MCP input.
        source_path: Optional path to original source data.

    Returns:
        Formatted before/after delta report.
    """
    from dataraum.pipeline.fixes.api import apply_fixes
    from dataraum.pipeline.fixes.models import FixDocument

    fix_documents = [
        FixDocument(
            target=f["target"],
            action=f["action"],
            table_name=f["table_name"],
            column_name=f.get("column_name"),
            dimension=f["dimension"],
            payload=f["payload"],
            description=f.get("description", ""),
        )
        for f in fixes
    ]

    result = apply_fixes(
        output_dir=output_dir,
        fix_documents=fix_documents,
        source_path=Path(source_path) if source_path else None,
    )

    if not result.success:
        return f"Error: Fix application failed: {result.error}"

    # Format before/after delta
    lines = [
        "## Fix Results",
        "",
        f"Applied {len(result.applied_fixes)} fix(es).",
    ]
    if result.phases_rerun:
        lines.append(f"Phases re-run: {', '.join(result.phases_rerun)}")
    lines.append("")

    # Show score deltas for dimensions that changed
    all_dims = set(result.gate_before.keys()) | set(result.gate_after.keys())
    if all_dims:
        lines.append("| Dimension | Target | Before | After | Delta |")
        lines.append("|-----------|--------|--------|-------|-------|")
        for dim in sorted(all_dims):
            before_targets = result.gate_before.get(dim, {})
            after_targets = result.gate_after.get(dim, {})
            all_targets = set(before_targets.keys()) | set(after_targets.keys())
            for target in sorted(all_targets):
                b = before_targets.get(target, 0.0)
                a = after_targets.get(target, 0.0)
                delta = a - b
                if abs(delta) > 0.001:
                    sign = "+" if delta > 0 else ""
                    lines.append(f"| {dim} | {target} | {b:.3f} | {a:.3f} | {sign}{delta:.3f} |")

    return "\n".join(lines)


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
