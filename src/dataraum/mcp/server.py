"""MCP Server implementation for DataRaum.

Exposes high-level tools that call library functions directly (no HTTP).
Output directory is resolved from DATARAUM_OUTPUT_DIR env var or passed to create_server().
"""

from __future__ import annotations

import asyncio
import json
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


def _json_text_content(data: dict[str, Any]) -> list[TextContent]:
    """Serialize a dict to JSON and wrap in MCP TextContent."""
    return [TextContent(type="text", text=json.dumps(data, indent=2, default=str))]


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
                    "Analyze data to build metadata context. Processes all registered "
                    "sources together, or provide a path to a file/directory directly. "
                    "Use target_gate to stop at a quality gate for zone-by-zone review "
                    "(returns inline gate status). Without target_gate, runs all phases. "
                    "Use contract to select evaluation criteria (cached for subsequent calls)."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path to CSV/Parquet file or directory. Omit to use registered sources.",
                        },
                        "name": {
                            "type": "string",
                            "description": "Optional: name for the data source",
                        },
                        "target_gate": {
                            "type": "string",
                            "enum": ["quality_review", "analysis_review", "computation_review"],
                            "description": "Stop at this gate for review instead of running the full pipeline. quality_review = Gate 1 (foundation). analysis_review = Gate 2 (enrichment). computation_review = Gate 3 (interpretation). Default: runs all phases.",
                        },
                        "contract": {
                            "type": "string",
                            "description": "Contract name (e.g. 'executive_dashboard'). Cached for subsequent calls.",
                        },
                    },
                    "required": [],
                },
                execution=ToolExecution(taskSupport="optional"),
            ),
            Tool(
                name="get_context",
                description=(
                    "Get the data context document for AI analysis. "
                    "Without section: returns the full context (schema, relationships, "
                    "semantic annotations, quality). With section: returns focused "
                    "structured data for one or more sections, reducing response size. "
                    "Use section='quality' for quality overview (entropy, grades, "
                    "interpretations). Use get_quality(gate=...) for zone-specific "
                    "violations and fix actions."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "section": {
                            "description": (
                                "Focus on specific section(s) to reduce response size. "
                                "Omit for full context document."
                            ),
                            "anyOf": [
                                {
                                    "type": "string",
                                    "enum": [
                                        "schema",
                                        "semantics",
                                        "quality",
                                        "validations",
                                        "cycles",
                                        "snippets",
                                        "contracts",
                                    ],
                                },
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                        "enum": [
                                            "schema",
                                            "semantics",
                                            "quality",
                                            "validations",
                                            "cycles",
                                            "snippets",
                                            "contracts",
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                },
            ),
            Tool(
                name="get_quality",
                description=(
                    "Get data quality information. Without gate: returns unified report "
                    "(entropy scores, contract compliance, resolution actions). "
                    "With gate: returns zone-specific status — violations, fix actions, "
                    "and skipped detectors for a quality gate."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "gate": {
                            "type": "string",
                            "enum": ["quality_review", "analysis_review", "computation_review"],
                            "description": "When set, returns zone-specific gate status instead of overall report. quality_review = Gate 1. analysis_review = Gate 2. computation_review = Gate 3.",
                        },
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
                    "Answer an analytical question using AI reasoning. "
                    "The query agent understands the data context — column "
                    "semantics, quality issues, business cycles — and writes "
                    "SQL that accounts for them (e.g. excluding one-time items, "
                    "normalizing intervals, handling multi-currency). "
                    "It tracks assumptions explicitly and evaluates confidence "
                    "against the active contract. Each SQL step becomes a "
                    "reusable snippet in the knowledge base. "
                    "Prerequisites: call get_context first so the agent has "
                    "metadata to reason over. Use get_quality to understand "
                    "data quality issues before asking analytical questions. "
                    "Returns: answer, confidence level, assumptions, SQL steps, "
                    "and result data."
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
                    "Scan the workspace for data files (CSV, Parquet, JSON, XLSX). "
                    "Returns file previews with column names and row counts, "
                    "and marks which files are already registered as sources. "
                    "Use this to help users identify what data is available before adding sources."
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
                    "Register a data source for analysis. Add all sources before calling "
                    "analyze — the pipeline processes them together. For files, provide a "
                    "path (file or directory). For databases, provide a backend type. "
                    "Returns the current source count so you can confirm with the user "
                    "whether more sources need to be added."
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
            # --- Continue pipeline tool ---
            Tool(
                name="continue_pipeline",
                description=(
                    "Advance the pipeline to the next zone boundary. "
                    "Call after inspecting gate status and applying fixes. "
                    "Returns inline gate status when stopping at a gate. "
                    "Skips already-completed phases. Source path is auto-resolved."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["target_gate"],
                    "properties": {
                        "target_gate": {
                            "type": "string",
                            "enum": ["analysis_review", "computation_review", "end"],
                            "description": (
                                "Where to stop: "
                                "'analysis_review' = run through Gate 2 (Zone 2). "
                                "'computation_review' = run through Gate 3 (Zone 3 analysis). "
                                "'end' = run through the end of the pipeline."
                            ),
                        },
                        "source_path": {
                            "type": "string",
                            "description": "Path to original source data. Auto-resolved from registered sources if omitted.",
                        },
                    },
                },
                execution=ToolExecution(taskSupport="optional"),
            ),
            # --- Direct SQL execution ---
            Tool(
                name="run_sql",
                description=(
                    "Execute SQL you write directly against the analyzed data. "
                    "Returns rows with per-column quality metadata when available. "
                    "Important: before writing SQL, call get_context to understand "
                    "the schema, column semantics, and quality issues. Blind SQL "
                    "without context understanding leads to wrong results. "
                    "For analytical questions, prefer the query tool — it reasons "
                    "over context automatically. Use run_sql for spot-checks, "
                    "drill-downs, or when you already understand the data shape. "
                    "Prefer structured steps over raw SQL: each step computes one "
                    "business concept, becomes a reusable snippet in the knowledge "
                    "base, and can be referenced by later steps as a temp view. "
                    "Use 'table.column' format in column_mappings for unambiguous "
                    "quality metadata resolution (e.g. 'invoices.amount')."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "steps": {
                            "type": "array",
                            "description": (
                                "Structured SQL steps. Each step becomes a temp view "
                                "and is saved as a reusable snippet in the knowledge base. "
                                "Later steps can reference earlier ones by step_id. "
                                "Decompose into one concept per step for maximum reuse. "
                                "Mutually exclusive with 'sql'."
                            ),
                            "items": {
                                "type": "object",
                                "required": ["step_id", "sql"],
                                "properties": {
                                    "step_id": {
                                        "type": "string",
                                        "description": "Business concept name (e.g. 'monthly_revenue', not 'step_1'). Used as view name by later steps.",
                                    },
                                    "sql": {
                                        "type": "string",
                                        "description": "SQL query for this step",
                                    },
                                    "description": {
                                        "type": "string",
                                        "description": "What this step does",
                                    },
                                    "column_mappings": {
                                        "type": "object",
                                        "description": (
                                            "Maps output column names to source column names "
                                            "for quality metadata lookup"
                                        ),
                                        "additionalProperties": {"type": "string"},
                                    },
                                },
                            },
                        },
                        "sql": {
                            "type": "string",
                            "description": (
                                "Raw SQL for quick one-off queries. Prefer 'steps' for "
                                "multi-stage analysis — steps are cached as snippets. "
                                "CTE queries are auto-decomposed into individual snippets. "
                                "Mutually exclusive with 'steps'."
                            ),
                        },
                        "column_mappings": {
                            "type": "object",
                            "description": (
                                "Maps output column names to source column names "
                                "for quality metadata lookup (raw SQL mode). "
                                "Use 'table.column' for unambiguous resolution."
                            ),
                            "additionalProperties": {"type": "string"},
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max rows to return. Default: 100, max: 10000.",
                            "default": 100,
                        },
                    },
                },
            ),
            # --- Agent-driven fix tools ---
            # --- Fix tool ---
            Tool(
                name="apply_fix",
                description=(
                    "Apply fixes and re-run affected pipeline phases. "
                    "Provide action name + target from get_quality output. "
                    "The system resolves the schema and builds documents internally. "
                    "Returns before/after score deltas. "
                    "Note: document_accepted_* actions acknowledge an issue but do NOT "
                    "lower the entropy score. Prefer corrective actions when possible."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["fixes"],
                    "properties": {
                        "fixes": {
                            "type": "array",
                            "description": "List of fixes to apply",
                            "items": {
                                "type": "object",
                                "required": ["action", "target"],
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "description": (
                                            "Fix action name from get_quality output "
                                            "(e.g. document_accepted_outlier_rate, "
                                            "document_type_override)"
                                        ),
                                    },
                                    "target": {
                                        "type": "string",
                                        "description": (
                                            "Target from get_quality affected_targets "
                                            "(e.g. 'column:orders.amount', 'table:orders')"
                                        ),
                                    },
                                    "parameters": {
                                        "type": "object",
                                        "description": (
                                            "Action-specific parameters "
                                            "(field values from the action schema)"
                                        ),
                                        "default": {},
                                    },
                                    "reason": {
                                        "type": "string",
                                        "description": "Why this fix is being applied",
                                    },
                                },
                            },
                        },
                        "source_path": {
                            "type": "string",
                            "description": (
                                "Path to original source data. Auto-resolved if omitted."
                            ),
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
        """Execute a tool and return JSON results."""
        if name == "analyze":
            path = arguments.get("path")
            source_name = arguments.get("name")
            target_gate = arguments.get("target_gate")
            contract = arguments.get("contract")

            # Validate path if provided
            if path:
                source_path = Path(path)
                if not source_path.exists():
                    return _json_text_content({"error": f"Path not found: {path}"})

            display_label = path or "(registered sources)"
            ctx = server.request_context
            experimental: Experimental = ctx.experimental
            if experimental and experimental.is_task:
                # Task-augmented path: return immediately, run in background
                loop = asyncio.get_running_loop()

                async def _work(task: ServerTaskContext) -> CallToolResult:
                    callback = _make_task_event_callback(task, loop)
                    result_dict = await asyncio.to_thread(
                        _analyze,
                        output_dir,
                        path,
                        source_name,
                        callback,
                        target_gate,
                        contract,
                    )
                    return CallToolResult(
                        content=[
                            TextContent(
                                type="text",
                                text=json.dumps(result_dict, indent=2, default=str),
                            )
                        ]
                    )

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
                    _run_analyze_background(output_dir, path, source_name, target_gate, contract)
                )
                _background_tasks.add(bg)
                bg.add_done_callback(_background_tasks.discard)
                result: dict[str, Any] = {
                    "status": "started",
                    "source": display_label,
                    "message": (
                        "Pipeline started. This typically takes 3-7 minutes depending on file size."
                    ),
                    "hint": "Call get_context every ~2 minutes to check progress.",
                }
        elif name == "get_context":
            result = _get_context(output_dir, section=arguments.get("section"))
        elif name == "get_quality":
            result = _get_quality(
                output_dir,
                gate=arguments.get("gate"),
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
        elif name == "run_sql":
            result = _run_sql(
                output_dir,
                steps=arguments.get("steps"),
                sql=arguments.get("sql"),
                column_mappings=arguments.get("column_mappings"),
                limit=arguments.get("limit", 100),
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
                    result_dict = await asyncio.to_thread(
                        _continue_pipeline, output_dir, target_gate, cont_source_path, callback
                    )
                    return CallToolResult(
                        content=[
                            TextContent(
                                type="text",
                                text=json.dumps(result_dict, indent=2, default=str),
                            )
                        ]
                    )

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
                    result_dict = await asyncio.to_thread(
                        _apply_fix, output_dir, arguments["fixes"], fix_source_path
                    )
                    return CallToolResult(
                        content=[
                            TextContent(
                                type="text",
                                text=json.dumps(result_dict, indent=2, default=str),
                            )
                        ]
                    )

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
            result = {"error": f"Unknown tool: {name}"}

        return _json_text_content(result)

    return server


def _get_pipeline_source(session: Any) -> Any | None:
    """Find the source that has pipeline runs.

    In multi-source mode (MCP onboarding flow), the pipeline runs against a
    synthetic "multi_source" record.  In single-file mode there is only one
    Source row.
    """
    from sqlalchemy import select

    from dataraum.storage import Source

    source = session.execute(
        select(Source).where(Source.name == "multi_source")
    ).scalar_one_or_none()
    if source:
        return source
    # Single-source mode: exactly one source exists
    return session.execute(select(Source).order_by(Source.created_at).limit(1)).scalar_one_or_none()


def _no_data_error(path: Path) -> dict[str, Any]:
    """Build error dict for missing pipeline output."""
    return {
        "error": f"No analyzed data found at {path}.",
        "hint": (
            "Run the analyze tool first: analyze(path='/path/to/your/data.csv'). "
            "If analysis results existed earlier, re-run analyze with the same source path."
        ),
    }


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
    "computation_review": "Reviewing computation quality (Gate 3)",
    "graph_execution": "Executing metric graphs",
}


async def _run_analyze_background(
    output_dir: Path,
    path: str | None,
    source_name: str | None,
    target_gate: str | None = None,
    contract: str | None = None,
) -> None:
    """Run _analyze in a background thread, logging errors."""
    try:
        await asyncio.to_thread(
            _analyze, output_dir, path, source_name, None, target_gate, contract
        )
    except Exception:
        _log.exception("Background pipeline failed for %s", path or "(registered sources)")


def _get_pipeline_progress(manager: Any) -> dict[str, Any] | None:
    """Check if a pipeline is running or recently failed.

    Returns:
        Progress/error dict if running or failed, None if pipeline is idle.
    """
    from datetime import UTC, datetime

    from sqlalchemy import func, select

    from dataraum.pipeline.db_models import PhaseLog, PipelineRun

    # A "running" pipeline older than this is considered stale (process died).
    _STALE_THRESHOLD_MINUTES = 30

    with manager.session_scope() as session:
        source = _get_pipeline_source(session)
        if not source:
            return None

        # Check for a running pipeline
        latest_run = session.execute(
            select(PipelineRun)
            .where(PipelineRun.source_id == source.source_id)
            .order_by(PipelineRun.started_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        if latest_run is None:
            return None

        # Terminal states — no progress to report
        if latest_run.status in ("completed", "stopped"):
            return None

        # Pipeline failed — surface the error
        if latest_run.status == "failed":
            failed_phases = session.execute(
                select(PhaseLog.phase_name, PhaseLog.error).where(
                    PhaseLog.run_id == latest_run.run_id,
                    PhaseLog.status == "failed",
                )
            ).all()
            return {
                "status": "pipeline_failed",
                "error": latest_run.error or "Unknown error",
                "failed_phases": [{"phase": name, "error": err} for name, err in failed_phases],
                "hint": "Fix the issue and re-run analyze.",
            }

        # Status is "running" — check if stale (process crashed without updating status)
        started = latest_run.started_at
        if started.tzinfo is None:
            started = started.replace(tzinfo=UTC)
        age_minutes = (datetime.now(UTC) - started).total_seconds() / 60
        if age_minutes > _STALE_THRESHOLD_MINUTES:
            return None

        # Pipeline is actively running — show progress
        completed_count: int = (
            session.execute(
                select(func.count()).where(PhaseLog.run_id == latest_run.run_id)
            ).scalar()
            or 0
        )

        from dataraum.pipeline.pipeline_config import load_phase_declarations

        declarations = load_phase_declarations()
        total_phases = len(declarations)

        # Determine currently running phases from dependency graph
        completed_names: set[str] = set()
        log_result = session.execute(
            select(PhaseLog.phase_name).where(PhaseLog.run_id == latest_run.run_id)
        )
        for row in log_result:
            completed_names.add(row[0])

        running_phases: list[str] = []
        for name, decl in declarations.items():
            if name in completed_names:
                continue
            deps = set(decl.dependencies)
            if deps.issubset(completed_names):
                running_phases.append(name)

        current_labels = [_PHASE_LABELS.get(p, p) for p in running_phases]

        return {
            "status": "pipeline_running",
            "completed": completed_count,
            "total": total_phases,
            "current_phases": current_labels,
            "hint": "Call get_context again to check for completion.",
        }


def _build_pipeline_status(session: Any, source_id: str) -> dict[str, Any] | None:
    """Build pipeline status dict for get_context.

    Returns None if no pipeline runs exist.
    """
    from sqlalchemy import func, select

    from dataraum.entropy.contracts import get_contracts
    from dataraum.entropy.gate import assess_contracts
    from dataraum.pipeline.db_models import PhaseLog, PipelineRun

    # Latest completed run
    run = session.execute(
        select(PipelineRun)
        .where(
            PipelineRun.source_id == source_id,
            PipelineRun.status.in_(["completed", "stopped"]),
        )
        .order_by(PipelineRun.started_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if not run:
        return None

    # Count completed phases
    phase_count: int = (
        session.execute(
            select(func.count()).where(
                PhaseLog.run_id == run.run_id,
                PhaseLog.status == "completed",
            )
        ).scalar()
        or 0
    )

    contract_name = run.config.get("contract") if run.config else None
    target_phase = run.config.get("target_phase") if run.config else None

    result: dict[str, Any] = {"phases_completed": phase_count}
    if contract_name:
        result["contract"] = contract_name
    if target_phase:
        result["stopped_at"] = target_phase

    # Check gates
    contracts = get_contracts()
    contract = None
    if contract_name:
        contract = contracts.get(contract_name)
    if not contract:
        contract = next(iter(contracts.values()), None) if contracts else None

    gates: dict[str, dict[str, Any]] = {}
    gate_states: dict[str, int | None] = {}
    for gate_phase, (zone_name, gate_label) in _GATE_ZONES.items():
        gate_log = session.execute(
            select(PhaseLog)
            .where(
                PhaseLog.source_id == source_id,
                PhaseLog.phase_name == gate_phase,
                PhaseLog.status == "completed",
            )
            .order_by(PhaseLog.completed_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        if not gate_log or not gate_log.outputs:
            gates[gate_phase] = {
                "zone": zone_name,
                "label": gate_label,
                "status": "not_reached",
            }
            gate_states[gate_phase] = None
            continue

        scores = gate_log.entropy_scores or {}
        if not scores:
            gates[gate_phase] = {
                "zone": zone_name,
                "label": gate_label,
                "status": "measured",
                "violations": 0,
            }
            gate_states[gate_phase] = 0
            continue

        n_violations = 0
        if contract:
            column_details = gate_log.outputs.get("gate_column_details", {})
            accepted_raw = gate_log.outputs.get("accepted_targets", {})
            accepted = {k: set(v) for k, v in accepted_raw.items()}
            issues = assess_contracts(
                scores,
                contract.dimension_thresholds,
                column_details,
                gate_phase,
                accepted_targets=accepted,
            )
            n_violations = len(issues)

        gates[gate_phase] = {
            "zone": zone_name,
            "label": gate_label,
            "status": "violations" if n_violations > 0 else "passing",
            "violations": n_violations,
        }
        gate_states[gate_phase] = n_violations

    result["gates"] = gates

    # Next steps — contextual guidance
    g1 = gate_states.get("quality_review")
    g2 = gate_states.get("analysis_review")
    g3 = gate_states.get("computation_review")

    next_steps: list[str] = []
    if g1 is None:
        next_steps.append("Run analyze(path='...', target_gate='quality_review') to start")
    elif g1 and g1 > 0:
        next_steps.append(
            'Use get_quality(gate="quality_review") to see violations and fix actions'
        )
        next_steps.append("Use apply_fix(fixes=[...]) to apply fixes")
        next_steps.append(
            'Use continue_pipeline(target_gate="analysis_review") to advance after fixing'
        )
    elif g2 is None:
        next_steps.append(
            'Gate 1 clean — use continue_pipeline(target_gate="analysis_review") to advance'
        )
    elif g2 and g2 > 0:
        next_steps.append(
            'Use get_quality(gate="analysis_review") to see violations and fix actions'
        )
        next_steps.append("Use apply_fix(fixes=[...]) to apply fixes")
        next_steps.append('Use continue_pipeline(target_gate="end") to complete after fixing')
    elif g3 is None:
        next_steps.append('Gates 1-2 clean — use continue_pipeline(target_gate="end") to complete')
    elif g3 and g3 > 0:
        next_steps.append(
            'Use get_quality(gate="computation_review") to see violations and fix actions'
        )
        next_steps.append("Use apply_fix(fixes=[...]) to apply fixes")
    else:
        next_steps.append("All gates passing — data is ready for use")
        next_steps.append("Use query to ask questions about the data")
        next_steps.append("Use export to export results")

    result["next_steps"] = next_steps
    return result


def _analyze(
    output_dir: Path,
    path: str | None = None,
    name: str | None = None,
    event_callback: EventCallback | None = None,
    target_gate: str | None = None,
    contract: str | None = None,
) -> dict[str, Any]:
    """Run the pipeline on a data source.

    Args:
        output_dir: Pipeline output directory
        path: Path to CSV/Parquet file or directory. When None, uses registered sources.
        name: Optional source name
        event_callback: Optional callback for pipeline events
        target_gate: Optional gate phase to stop at (e.g. 'quality_review').
        contract: Optional contract name to use for evaluation.

    Returns:
        Dict with pipeline result, optionally with inline gate status.
    """
    from dataraum.pipeline.runner import RunConfig, run

    source_path: Path | None = None
    if path:
        source_path = Path(path)
        if not source_path.exists():
            return {"error": f"Path not found: {path}"}

    # Fall back to cached contract from prior run
    resolved_contract = contract or _get_cached_contract(output_dir)

    config = RunConfig(
        source_path=source_path,
        output_dir=output_dir,
        source_name=name,
        event_callback=event_callback,
        target_phase=target_gate,
        contract=resolved_contract,
    )

    result = run(config)

    if not result.success or not result.value:
        return {"error": f"Pipeline failed: {result.error}"}

    result_dict = format_pipeline_result(result.value)

    # When stopping at a gate, append inline gate status
    if target_gate:
        result_dict["gate_status"] = _get_zone_status(
            output_dir, gate=target_gate, contract_name=contract
        )

    return result_dict


def _get_context(
    output_dir: Path,
    section: str | list[str] | None = None,
) -> dict[str, Any]:
    """Get context document as structured dict, or progress status if pipeline is running.

    Args:
        output_dir: Pipeline output directory.
        section: Optional section name or list of names. When set, returns
            focused structured data instead of the full markdown document.
            Valid: schema, semantics, quality, validations, cycles, snippets, contracts.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.mcp.sections import (
        CONTEXT_SECTIONS,
        VALID_SECTIONS,
        build_contracts_section,
        build_cycles_section,
        build_quality_section,
        build_schema_section,
        build_semantics_section,
        build_snippets_section,
        build_validations_section,
    )
    from dataraum.storage import Table

    # Normalize section parameter
    requested: set[str] | None = None
    if section is not None:
        if isinstance(section, str):
            requested = {section}
        else:
            requested = set(section)
        if not requested:
            return {"error": "section list cannot be empty"}
        invalid = requested - VALID_SECTIONS
        if invalid:
            return {"error": f"Unknown section(s): {', '.join(sorted(invalid))}"}

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _no_data_error(output_dir)

    try:
        # If pipeline is running or failed, return status instead of partial context
        progress = _get_pipeline_progress(manager)
        if progress is not None:
            return progress

        with manager.session_scope() as session:
            source = _get_pipeline_source(session)

            if not source:
                return {"error": "No sources found in database"}

            tables_result = session.execute(
                select(Table).where(
                    Table.source_id == source.source_id,
                    Table.layer == "typed",
                )
            )
            tables = tables_result.scalars().all()

            if not tables:
                return {"error": "No tables found. Run pipeline first."}

            table_ids = [t.table_id for t in tables]

            # --- Full mode (no section) — backward compatible ---
            if requested is None:
                return _get_context_full(session, source, table_ids, manager)

            # --- Sectioned mode ---
            result: dict[str, Any] = {}

            # Build GraphExecutionContext only if needed
            context = None
            if requested & CONTEXT_SECTIONS:
                from dataraum.graphs.context import build_execution_context

                with manager.duckdb_cursor() as cursor:
                    context = build_execution_context(
                        session=session,
                        table_ids=table_ids,
                        duckdb_conn=cursor,
                    )

            # Section builders backed by GraphExecutionContext
            context_builders = {
                "schema": build_schema_section,
                "semantics": build_semantics_section,
                "quality": build_quality_section,
                "validations": build_validations_section,
                "cycles": build_cycles_section,
            }

            for name in sorted(requested & CONTEXT_SECTIONS):
                builder = context_builders[name]
                assert context is not None  # Guarded by CONTEXT_SECTIONS check above
                try:
                    result[name] = builder(context)
                except Exception:
                    _log.debug("Section %s failed", name, exc_info=True)
                    result[name] = {"error": f"Failed to build {name} section"}

            # Independent sections
            if "snippets" in requested:
                try:
                    result["snippets"] = build_snippets_section(session, source.source_id)
                except Exception:
                    _log.debug("Snippets section failed", exc_info=True)
                    result["snippets"] = {"error": "Failed to build snippets section"}

            if "contracts" in requested:
                try:
                    result["contracts"] = build_contracts_section(session, table_ids)
                except Exception:
                    _log.debug("Contracts section failed", exc_info=True)
                    result["contracts"] = {"error": "Failed to build contracts section"}

            return result
    finally:
        manager.close()


def _get_context_full(
    session: Any,
    source: Any,
    table_ids: list[str],
    manager: Any,
) -> dict[str, Any]:
    """Build the full context response (backward compatible, no section parameter)."""
    from dataraum.graphs.context import build_execution_context, format_metadata_document

    with manager.duckdb_cursor() as cursor:
        context = build_execution_context(
            session=session,
            table_ids=table_ids,
            duckdb_conn=cursor,
        )

    # metadata_document stays as markdown — it's a rich narrative
    # document used by multiple callers (query agent, graph agent).
    result: dict[str, Any] = {
        "metadata": format_metadata_document(context, source_name=source.name),
    }

    # Pipeline status as structured data
    try:
        status = _build_pipeline_status(session, source.source_id)
        if status:
            result["pipeline_status"] = status
    except Exception:
        pass  # Pipeline status is non-critical

    # Contract catalog as structured data
    try:
        from dataraum.mcp.sections import build_contracts_section

        catalog = build_contracts_section(session, table_ids)
        if catalog:
            result["contract_catalog"] = catalog
    except Exception:
        pass  # Contract catalog is non-critical

    # Snippet knowledge base stats
    try:
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session)
        stats = library.get_stats(schema_mapping_id=source.source_id)
        if stats.get("total_snippets", 0) > 0:
            kb: dict[str, Any] = {
                "total_snippets": stats["total_snippets"],
                "validated_snippets": stats["validated_snippets"],
            }
            if stats.get("snippets_by_type"):
                kb["by_type"] = stats["snippets_by_type"]
            if stats.get("cache_hit_rate", 0) > 0:
                kb["cache_hit_rate"] = round(stats["cache_hit_rate"], 3)
            result["sql_knowledge_base"] = kb
    except Exception:
        pass  # Snippet stats are non-critical

    return result


def _get_quality(
    output_dir: Path,
    gate: str | None = None,
    contract_name: str | None = None,
    table_name: str | None = None,
    priority: str | None = None,
    include: list[str] | None = None,
) -> dict[str, Any]:
    """Get unified data quality report or zone-specific gate status.

    When ``gate`` is set, delegates to ``_get_zone_status`` for per-gate
    violations, fix actions, and skipped detectors.  Otherwise assembles
    the overall entropy + contract + actions report.
    """
    # Fall back to cached contract from prior run
    resolved_contract = contract_name or _get_cached_contract(output_dir)

    if gate:
        return _get_zone_status(output_dir, gate=gate, contract_name=resolved_contract)

    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.storage import Table

    sections_to_include = set(include or ["entropy", "contract", "actions"])

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _no_data_error(output_dir)

    try:
        with manager.session_scope() as session:
            source = _get_pipeline_source(session)

            if not source:
                return {"error": "No sources found"}

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

            sections: dict[str, Any] = {}

            # --- Entropy section ---
            if "entropy" in sections_to_include:
                sections["entropy"] = _build_entropy_section(
                    session, source, network_context, column_summaries, table_name
                )

            # --- Contract section ---
            if "contract" in sections_to_include:
                sections["contract"] = _build_contract_section(column_summaries, resolved_contract)

            # --- Actions section ---
            if "actions" in sections_to_include:
                sections["actions"] = _build_actions_section(session, source, priority, table_name)

            result = format_quality_report(sections)

            # When sections are unavailable, explain the zone-by-zone model.
            # The overall report (entropy + contract + actions) requires a
            # complete pipeline run. During zone-by-zone execution, agents
            # should use the gate parameter instead.
            any_unavailable = any(
                isinstance(v, dict) and v.get("status") == "unavailable" for v in sections.values()
            )
            if any_unavailable:
                result["hint"] = (
                    "The overall quality report requires a complete pipeline run. "
                    "The pipeline runs zone-by-zone: Gate 1 (quality_review) checks "
                    "foundation quality, Gate 2 (analysis_review) checks enrichment, "
                    "Gate 3 (computation_review) checks interpretation. "
                    "Use get_quality(gate=...) to see violations and fix actions at "
                    "the current gate. Use get_context to see pipeline status and "
                    "which gate to inspect next."
                )

            return result
    finally:
        manager.close()


def _build_entropy_section(
    session: Any,
    source: Any,
    network_context: Any,
    column_summaries: Any,
    table_name: str | None,
) -> dict[str, Any]:
    """Build entropy section for quality report."""
    from sqlalchemy import select

    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord

    if not network_context or network_context.total_columns == 0:
        return {"status": "unavailable"}

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
        source.name,
        network_context.overall_readiness,
        network_context.avg_entropy_score,
        interpretations,
        table_name,
        dimension_scores,
    )

    from dataraum.entropy.views.network_context import format_network_context

    result["network_analysis"] = format_network_context(network_context)

    return result


def _build_contract_section(
    column_summaries: Any,
    contract_name: str | None,
) -> dict[str, Any]:
    """Build contract section for quality report."""
    from dataraum.entropy.contracts import evaluate_contract, get_contract, list_contracts

    if not column_summaries:
        return {"status": "unavailable"}

    # Auto-detect contract if not specified
    resolved_name = contract_name
    if not resolved_name:
        contracts = list_contracts()
        resolved_name = contracts[0]["name"] if contracts else "aggregation_safe"

    profile = get_contract(resolved_name)
    if profile is None:
        return {"status": "error", "message": f"Contract not found: {resolved_name}"}

    evaluation = evaluate_contract(column_summaries, resolved_name)
    return format_contract_evaluation(evaluation, profile)


def _build_actions_section(
    session: Any,
    source: Any,
    priority: str | None,
    table_name: str | None,
) -> dict[str, Any]:
    """Build actions section for quality report."""
    from dataraum.entropy.actions import load_actions

    actions = load_actions(session, source)

    if not actions:
        return {"total_actions": 0, "actions": []}

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
) -> dict[str, Any]:
    """Execute a natural language query."""
    from dataraum.core.connections import get_manager_for_directory
    from dataraum.query import answer_question

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _no_data_error(output_dir)

    try:
        with manager.session_scope() as session:
            source = _get_pipeline_source(session)

            if not source:
                return {"error": "No sources found"}

            with manager.duckdb_cursor() as cursor:
                result = answer_question(
                    question=question,
                    session=session,
                    duckdb_conn=cursor,
                    source_id=source.source_id,
                    contract=contract_name,
                )

            if not result.success or not result.value:
                return {"error": str(result.error)}

            return format_query_result(result.value)
    finally:
        manager.close()


def _run_sql(
    output_dir: Path,
    steps: list[dict[str, Any]] | None = None,
    sql: str | None = None,
    column_mappings: dict[str, str] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Execute SQL directly against analyzed data.

    Args:
        output_dir: Pipeline output directory.
        steps: Structured SQL steps.
        sql: Raw SQL string.
        column_mappings: Maps output column names to source columns (raw SQL mode).
        limit: Max rows to return.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.mcp.sql_executor import run_sql
    from dataraum.storage import Table

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _no_data_error(output_dir)

    try:
        with manager.session_scope() as session:
            source = _get_pipeline_source(session)
            table_ids: list[str] = []
            if source:
                tables = (
                    session.execute(
                        select(Table).where(
                            Table.source_id == source.source_id,
                            Table.layer == "typed",
                        )
                    )
                    .scalars()
                    .all()
                )
                table_ids = [t.table_id for t in tables]

            with manager.duckdb_cursor() as cursor:
                return run_sql(
                    cursor,
                    session=session,
                    source_id=source.source_id if source else None,
                    table_ids=table_ids,
                    steps=steps,
                    sql=sql,
                    column_mappings=column_mappings,
                    limit=limit,
                )
    finally:
        manager.close()


def _get_cached_contract(output_dir: Path) -> str | None:
    """Get the contract name from the latest pipeline run config.

    Returns None if no runs exist or no contract was specified.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.pipeline.db_models import PipelineRun

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return None

    try:
        with manager.session_scope() as session:
            run = session.execute(
                select(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(1)
            ).scalar_one_or_none()
            if run and run.config:
                return run.config.get("contract")
    finally:
        manager.close()

    return None


def _resolve_source_path(output_dir: Path) -> str | None:
    """Resolve the source_path from the database if available.

    Queries the first registered Source and returns its connection_config path.
    Returns None if no source or no path is found.
    """
    from dataraum.core.connections import get_manager_for_directory

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return None

    try:
        with manager.session_scope() as session:
            source = _get_pipeline_source(session)
            if source and source.connection_config:
                path: str | None = source.connection_config.get("path")
                return path
    finally:
        manager.close()

    return None


def _get_or_create_manager(output_dir: Path) -> Any:
    """Get a ConnectionManager, creating the database if it doesn't exist yet."""
    from dataraum.core.connections import ConnectionConfig, ConnectionManager

    config = ConnectionConfig.for_directory(output_dir)
    config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    manager = ConnectionManager(config)
    manager.initialize()
    return manager


def _discover_sources(output_dir: Path, scan_path: str, recursive: bool) -> dict[str, Any]:
    """Scan workspace for data files and list existing registered sources."""
    from dataraum.sources.discovery import discover_sources

    root = Path(scan_path).resolve()
    if not root.is_dir():
        return {"error": f"Directory not found: {scan_path}"}

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

    # Get registered source paths to mark files as already-added
    registered_paths: set[str] = set()
    try:
        from sqlalchemy import select

        from dataraum.core.connections import get_manager_for_directory
        from dataraum.storage import Source as SourceModel

        mgr = get_manager_for_directory(output_dir)
        try:
            with mgr.session_scope() as sess:
                for src in sess.execute(select(SourceModel)).scalars():
                    if src.connection_config and "path" in src.connection_config:
                        registered_paths.add(str(Path(src.connection_config["path"]).resolve()))
        finally:
            mgr.close()
    except FileNotFoundError:
        pass

    # Format as structured output
    output: dict[str, Any] = {
        "scan_root": result.scan_root,
        "files": [
            {
                "path": f.path,
                "format": f.format,
                "size_bytes": f.size_bytes,
                "columns": f.columns,
                "row_count_estimate": f.row_count_estimate,
                "registered": str(Path(f.path).resolve()) in registered_paths,
            }
            for f in result.files
        ],
        "existing_sources": result.existing_sources,
    }

    if not result.files and not result.existing_sources:
        output["hint"] = (
            f"No data files found in {scan_path}. Try a different directory or add files."
        )
        return output

    # Add guidance
    unregistered = [f for f in output["files"] if not f["registered"]]
    if unregistered:
        output["hint"] = (
            f"{len(unregistered)} file(s) not yet registered. "
            f"Use add_source to register them, then analyze to process all sources together."
        )

    return output


def _add_source(output_dir: Path, arguments: dict[str, Any]) -> dict[str, Any]:
    """Register a new data source."""
    from sqlalchemy import select

    from dataraum.core.credentials import CredentialChain
    from dataraum.sources.manager import SourceManager
    from dataraum.storage import Source

    name = arguments["name"]
    path = arguments.get("path")
    backend = arguments.get("backend")

    if not path and not backend:
        return {"error": "Provide either 'path' (for files) or 'backend' (for databases)."}
    if path and backend:
        return {"error": "Provide 'path' or 'backend', not both."}

    try:
        manager = _get_or_create_manager(output_dir)
    except Exception as e:
        return {"error": f"Initializing database failed: {e}"}

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
                return {"error": str(result.error)}

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

            # Include total source count for multi-source flow
            all_sources = (
                session.execute(select(Source.name).where(Source.archived_at.is_(None)))
                .scalars()
                .all()
            )
            output["registered_sources"] = {
                "count": len(all_sources),
                "names": list(all_sources),
            }
            output["next_steps"] = (
                "Add more sources with add_source, or call analyze "
                "to process all registered sources together."
            )

            return output
    finally:
        manager.close()


# ---------------------------------------------------------------------------
# Zone status
# ---------------------------------------------------------------------------

# Maps gate phase names to (zone_name, gate_label).
_GATE_ZONES: dict[str, tuple[str, str]] = {
    "quality_review": ("foundation", "Gate 1"),
    "analysis_review": ("enrichment", "Gate 2"),
    "computation_review": ("interpretation", "Gate 3"),
}


def _get_zone_status(
    output_dir: Path,
    gate: str,
    contract_name: str | None = None,
) -> dict[str, Any]:
    """Read persisted gate scores and format zone status for an agent.

    Args:
        output_dir: Pipeline output directory.
        gate: Gate phase to inspect (quality_review or analysis_review).
        contract_name: Contract to evaluate against. Auto-detects if omitted.
    """
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.entropy.contracts import get_contract, get_contracts
    from dataraum.entropy.gate import assess_contracts, match_threshold
    from dataraum.pipeline.db_models import PhaseLog

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _no_data_error(output_dir)

    try:
        with manager.session_scope() as session:
            # Find source
            source = _get_pipeline_source(session)
            if not source:
                return {"error": "No sources found."}

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
                return {
                    "error": f"No gate measurement found for {gate_phase}.",
                    "hint": (
                        f"The pipeline may not have reached this gate yet. "
                        f"Run analyze first, then call get_quality(gate='{gate_phase}')."
                    ),
                }

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
                return {"error": "No contract found."}

            thresholds = contract.dimension_thresholds

            # Assess violations (accepted targets excluded via contract overrule)
            accepted_raw = outputs.get("accepted_targets", {})
            accepted = {k: set(v) for k, v in accepted_raw.items()}
            issues = assess_contracts(
                scores,
                thresholds,
                column_details,
                gate_phase,
                accepted_targets=accepted,
            )

            # Build violation entries with full context for agent triage
            violation_dims = {i.dimension_path for i in issues}

            from dataraum.entropy.fix_schemas import (
                get_schemas_for_detector,
                get_triage_guidance,
            )

            # Per-target evidence is stored in gate outputs during gate measurement.
            # Keyed by dimension_path → target → evidence dict.
            gate_evidence = outputs.get("gate_column_evidence", {})

            # Load interpretation records scoped to violated targets only
            from dataraum.entropy.interpretation_db_models import (
                EntropyInterpretationRecord,
            )

            violated_tables = set()
            for issue in issues:
                for t in issue.affected_targets:
                    ref = t.replace("column:", "").replace("table:", "")
                    violated_tables.add(ref.split(".", 1)[0])

            interp_by_col: dict[tuple[str, str | None], Any] = {}
            if violated_tables:
                interp_records = (
                    session.execute(
                        select(EntropyInterpretationRecord).where(
                            EntropyInterpretationRecord.source_id == source.source_id,
                            EntropyInterpretationRecord.table_name.in_(violated_tables),
                        )
                    )
                    .scalars()
                    .all()
                )
                for rec in interp_records:
                    interp_by_col[(rec.table_name, rec.column_name)] = rec

            violations = []
            for issue in issues:
                detector_id = id_map.get(
                    issue.dimension_path, issue.dimension_path.rsplit(".", 1)[-1]
                )

                # Full action details from fix schemas
                schemas = get_schemas_for_detector(detector_id)
                executable_actions = []
                for s in schemas:
                    action_detail: dict[str, Any] = {
                        "action": s.action,
                        "description": s.description,
                        "routing": s.routing,
                        "guidance": s.guidance,
                    }
                    if s.fields:
                        action_detail["fields"] = {
                            fname: {
                                "type": fdef.type,
                                "required": fdef.required,
                                "description": fdef.description,
                                **({"default": fdef.default} if fdef.default else {}),
                                **({"enum_values": fdef.enum_values} if fdef.enum_values else {}),
                            }
                            for fname, fdef in s.fields.items()
                        }
                    executable_actions.append(action_detail)

                # Triage guidance
                triage = get_triage_guidance(detector_id)

                # Interpretation context (from first affected target)
                interpretation = None
                for target in issue.affected_targets:
                    parts = target.replace("column:", "").replace("table:", "").split(".", 1)
                    tbl = parts[0]
                    col = parts[1] if len(parts) > 1 else None
                    interp_rec = interp_by_col.get((tbl, col))
                    if interp_rec:
                        interpretation = {
                            "explanation": interp_rec.explanation,
                            "resolution_actions": interp_rec.resolution_actions_json or [],
                        }
                        break

                # Accepted targets for this dimension
                dim_accepted = list(accepted.get(issue.dimension_path, set()))

                # Per-target evidence from gate measurement (outlier_ratio, null_ratio, etc.)
                dim_evidence = gate_evidence.get(issue.dimension_path, {})
                target_evidence: dict[str, Any] = {}
                for target in issue.affected_targets:
                    ev = dim_evidence.get(target)
                    if ev:
                        target_evidence[target] = ev

                violation: dict[str, Any] = {
                    "dimension_path": issue.dimension_path,
                    "detector_id": detector_id,
                    "score": issue.score,
                    "threshold": issue.threshold,
                    "affected_targets": issue.affected_targets,
                    "target_evidence": target_evidence,
                    "executable_actions": executable_actions,
                    "triage_guidance": triage,
                }
                if interpretation:
                    violation["interpretation"] = interpretation
                if dim_accepted:
                    violation["accepted_targets"] = dim_accepted

                violations.append(violation)

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

            zone_name, gate_label = _GATE_ZONES.get(gate_phase, ("unknown", "Gate ?"))
            return format_zone_status(
                zone_name,
                gate_label,
                gate_phase,
                violations,
                passing,
                contract.name,
            )
    finally:
        manager.close()


def _continue_pipeline(
    output_dir: Path,
    target_gate: str,
    source_path: str | None = None,
    event_callback: EventCallback | None = None,
) -> dict[str, Any]:
    """Resume the pipeline from current position to the next zone boundary.

    Args:
        output_dir: Pipeline output directory (must already exist from prior run).
        target_gate: Where to stop — 'analysis_review' (Gate 2) or 'end' (full pipeline).
        source_path: Path to original source data. Auto-resolved from DB if omitted.
        event_callback: Optional callback for progress updates.
    """
    from dataraum.pipeline.runner import RunConfig, run

    # Map target_gate to target_phase (None = run to end)
    target_phase: str | None = None if target_gate == "end" else target_gate

    # Auto-resolve source_path if not provided
    resolved = source_path or _resolve_source_path(output_dir)
    sp: Path | None = Path(resolved) if resolved else None

    config = RunConfig(
        source_path=sp,
        output_dir=output_dir,
        target_phase=target_phase,
        event_callback=event_callback,
    )

    result = run(config)

    if not result.success or not result.value:
        return {"error": f"Pipeline failed: {result.error}"}

    result_dict = format_pipeline_result(result.value)

    # Append inline zone status when stopping at a gate
    if target_gate != "end":
        result_dict["gate_status"] = _get_zone_status(output_dir, gate=target_gate)

    return result_dict


def _export(
    output_dir: Path,
    question: str | None = None,
    sql: str | None = None,
    export_path: str = "./export.csv",
    fmt: str = "csv",
) -> dict[str, Any]:
    """Export query results or SQL output to a file."""
    from dataraum.core.connections import get_manager_for_directory
    from dataraum.export import ExportFormat, export_query_result, export_sql

    if not question and not sql:
        return {"error": "Provide either 'question' or 'sql'."}
    if question and sql:
        return {"error": "Provide 'question' or 'sql', not both."}

    if fmt not in ("csv", "parquet", "json"):
        return {"error": f"Unknown format '{fmt}'. Use csv, parquet, or json."}

    export_fmt: ExportFormat = fmt  # type: ignore[assignment]
    dest = Path(export_path)

    try:
        manager = get_manager_for_directory(output_dir)
    except FileNotFoundError:
        return _no_data_error(output_dir)

    try:
        if question:
            # Run query agent, then export the result
            from dataraum.query import answer_question

            with manager.session_scope() as session:
                source = _get_pipeline_source(session)
                if not source:
                    return {"error": "No sources found"}

                with manager.duckdb_cursor() as cursor:
                    query_result = answer_question(
                        question=question,
                        session=session,
                        duckdb_conn=cursor,
                        source_id=source.source_id,
                    )

                if not query_result.success or not query_result.value:
                    return {"error": f"Query failed: {query_result.error}"}

                qr = query_result.value
                if not qr.data or not qr.columns:
                    return {"error": f"Query returned no tabular data. Answer: {qr.answer}"}

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
                with open(sidecar) as f:
                    meta = json.load(f)
                return format_export_result(
                    str(exported), fmt, meta.get("row_count", 0), str(sidecar)
                )
    except Exception as e:
        return {"error": f"Export failed: {e}"}
    finally:
        manager.close()


def _parse_target(target: str) -> tuple[str, str | None]:
    """Parse a target string into (table_name, column_name).

    "column:orders.amount" → ("orders", "amount")
    "table:orders"         → ("orders", None)
    "orders.amount"        → ("orders", "amount")

    Raises:
        ValueError: If target is empty or produces an empty table name.
    """
    if not target or not target.strip():
        raise ValueError("Target string is empty")

    # Strip prefix
    if ":" in target:
        prefix, ref = target.split(":", 1)
        if prefix == "table":
            if not ref:
                raise ValueError(f"Empty table name in target: {target!r}")
            return ref, None
        if prefix != "column":
            raise ValueError(f"Unknown target prefix {prefix!r} in: {target!r}")
        # column:table.col
        parts = ref.split(".", 1)
        table = parts[0]
        col = parts[1] if len(parts) > 1 else None
    else:
        parts = target.split(".", 1)
        table = parts[0]
        col = parts[1] if len(parts) > 1 else None

    if not table:
        raise ValueError(f"Empty table name in target: {target!r}")
    if col is not None and not col:
        col = None  # Treat empty column as None

    return table, col


def _apply_fix(
    output_dir: Path,
    fixes: list[dict[str, Any]],
    source_path: str | None = None,
) -> dict[str, Any]:
    """Apply fixes and re-run affected pipeline phases.

    Resolves action -> FixSchema -> FixDocuments via the bridge, then
    applies, logs to fix ledger, and reports before/after score deltas.

    Args:
        output_dir: Pipeline output directory.
        fixes: List of fix dicts with action, target, parameters, reason.
        source_path: Optional path to original source data.

    Returns:
        Dict with fix results and before/after score deltas.
    """
    from dataraum.core.connections import get_manager_for_directory
    from dataraum.documentation.ledger import log_fix
    from dataraum.entropy.fix_schemas import get_fix_schema
    from dataraum.pipeline.fixes import FixInput
    from dataraum.pipeline.fixes.api import apply_fixes
    from dataraum.pipeline.fixes.bridge import build_fix_documents

    all_documents = []
    # Track fix metadata for ledger logging
    fix_meta: list[dict[str, Any]] = []

    for f in fixes:
        action = f["action"]
        target = f["target"]
        parameters = f.get("parameters", {})
        reason = f.get("reason", "")

        try:
            table_name, column_name = _parse_target(target)
        except ValueError as e:
            return {"error": f"Invalid target: {e}"}

        schema = get_fix_schema(action)
        if schema is None:
            return {"error": f"Unknown action '{action}'"}

        dimension = schema.dimension_path

        # Build affected_columns list from target
        if column_name:
            affected = [f"{table_name}.{column_name}"]
        else:
            affected = [f"table:{table_name}"]

        fix_input = FixInput(
            action_name=action,
            affected_columns=affected,
            parameters=parameters,
            interpretation=reason,
        )
        docs = build_fix_documents(schema, fix_input, table_name, column_name, dimension)
        all_documents.extend(docs)
        fix_meta.append(
            {
                "action": action,
                "table_name": table_name,
                "column_name": column_name,
                "reason": reason,
            }
        )

    if not all_documents:
        return {"error": "No fix documents generated. Check action names and parameters."}

    # Determine the latest gate needed for re-measurement
    gate_order = ["quality_review", "analysis_review", "computation_review"]
    target_phase = "quality_review"
    for meta in fix_meta:
        s = get_fix_schema(meta["action"])
        if s and s.gate and s.gate in gate_order:
            if gate_order.index(s.gate) > gate_order.index(target_phase):
                target_phase = s.gate

    resolved = source_path or _resolve_source_path(output_dir)
    result = apply_fixes(
        output_dir=output_dir,
        fix_documents=all_documents,
        source_path=Path(resolved) if resolved else None,
        target_phase=target_phase,
    )

    if not result.success:
        return {"error": f"Fix application failed: {result.error}"}

    # Log to fix ledger for audit trail
    try:
        ledger_mgr = get_manager_for_directory(output_dir)
        try:
            with ledger_mgr.session_scope() as session:
                source = _get_pipeline_source(session)
                if source:
                    for meta in fix_meta:
                        log_fix(
                            session=session,
                            source_id=source.source_id,
                            action_name=meta["action"],
                            table_name=meta["table_name"],
                            column_name=meta["column_name"],
                            user_input=meta["reason"],
                            interpretation=f"MCP apply_fix: {meta['action']}",
                        )
        finally:
            ledger_mgr.close()
    except Exception:
        pass  # Ledger logging is best-effort

    output: dict[str, Any] = {
        "fixes_applied": len(result.applied_fixes),
    }
    if result.phases_rerun:
        output["phases_rerun"] = result.phases_rerun

    # Build score deltas
    all_dims = set(result.gate_before.keys()) | set(result.gate_after.keys())
    deltas: list[dict[str, Any]] = []
    for dim in sorted(all_dims):
        before_targets = result.gate_before.get(dim, {})
        after_targets = result.gate_after.get(dim, {})
        all_targets = set(before_targets.keys()) | set(after_targets.keys())
        for target in sorted(all_targets):
            b = before_targets.get(target, 0.0)
            a = after_targets.get(target, 0.0)
            delta = a - b
            if abs(delta) > 0.001:
                deltas.append(
                    {
                        "dimension": dim,
                        "target": target,
                        "before": round(b, 3),
                        "after": round(a, 3),
                        "delta": round(delta, 3),
                    }
                )
    if deltas:
        output["score_deltas"] = deltas

    # Include post-fix gate status so the agent can decide immediately
    # whether to fix more or advance the pipeline.
    output["gate_status"] = _get_zone_status(output_dir, gate=target_phase)

    return output


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
