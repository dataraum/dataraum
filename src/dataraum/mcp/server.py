"""MCP Server implementation for DataRaum.

Exposes high-level tools that call library functions directly (no HTTP).
Output directory is resolved from DATARAUM_OUTPUT_DIR env var or passed to create_server().
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session as SASession

    from dataraum.core.connections import ConnectionManager
    from dataraum.investigation.db_models import InvestigationSession
    from dataraum.storage import Column as ColumnModel
    from dataraum.storage import Table as TableModel

from mcp.server import Server
from mcp.server.experimental.request_context import Experimental
from mcp.server.experimental.task_context import ServerTaskContext
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, CreateTaskResult, TextContent, Tool, ToolExecution

from dataraum.mcp.formatters import format_query_result
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
                labels = _get_phase_labels()
                label = labels.get(event.phase or "", event.phase or "")
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


def _resolve_root_dir() -> Path:
    """Resolve the DataRaum root directory.

    The root contains workspace/, archive/, and credentials.yaml.
    Reads DATARAUM_HOME env var, falling back to ~/.dataraum/.
    Legacy DATARAUM_OUTPUT_DIR is also accepted (treated as root).
    """
    home = os.environ.get("DATARAUM_HOME") or os.environ.get("DATARAUM_OUTPUT_DIR")
    if home:
        return Path(home).expanduser()
    return Path("~/.dataraum").expanduser()


def create_server(output_dir: Path | None = None) -> Server:
    """Create and configure the MCP server with DataRaum tools.

    Args:
        output_dir: Explicit workspace directory (for tests). If not provided,
            resolves root via DATARAUM_HOME env var (default ~/.dataraum/)
            and uses root/workspace/ as the workspace.
    """
    if output_dir is None:
        root_dir = _resolve_root_dir()
        output_dir = root_dir / "workspace"
    else:
        # Explicit output_dir (tests, CLI) — root is the parent
        root_dir = output_dir.parent

    # Server-level ConnectionManager — lazy-initialized on first tool call.
    # Stays alive for the server lifetime. call_tool opens session/cursor
    # scopes from this manager and passes them to handlers.
    # Not shared across threads: all call_tool invocations run on the asyncio
    # event loop (single-threaded). The pipeline runs in its own thread with
    # its own manager — no cross-thread sharing of connections.
    _manager: ConnectionManager | None = None

    def _get_manager() -> ConnectionManager:
        """Get or create the server-level ConnectionManager."""
        nonlocal _manager
        if _manager is None:
            from dataraum.core.connections import ConnectionConfig
            from dataraum.core.connections import ConnectionManager as CM

            config = ConnectionConfig.for_directory(output_dir)
            config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            _manager = CM(config)
            _manager.initialize()
        return _manager

    def _get_active_session(db_session: SASession) -> InvestigationSession | None:
        """Query DB for the most recent active investigation session.

        Session state is DB-derived, not held in closure variables.
        This survives server restarts and avoids orphan cleanup.
        """
        from sqlalchemy import select

        from dataraum.investigation.db_models import InvestigationSession

        return db_session.execute(
            select(InvestigationSession)
            .where(InvestigationSession.status == "active")
            .order_by(InvestigationSession.started_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    def _archive_and_reset(session_id: str) -> str | None:
        """Archive the workspace and reset manager for a fresh session.

        Moves the workspace to {root}/archive/{session_id}/, then clears
        the ConnectionManager so the next tool call creates a fresh workspace.

        Returns:
            Warning message if archival failed, None on success.
        """
        import shutil

        nonlocal _manager

        # Close DB connections before moving files
        if _manager is not None:
            _manager.close()

        archive_dir = root_dir / "archive" / session_id
        try:
            archive_dir.parent.mkdir(parents=True, exist_ok=True)
            if output_dir.exists():
                shutil.move(str(output_dir), str(archive_dir))
                _log.info("Archived workspace to %s", archive_dir)
            _manager = None
            return None
        except OSError:
            _manager = None  # Still reset — connections are closed
            _log.warning(
                "Failed to archive workspace %s → %s", output_dir, archive_dir, exc_info=True
            )
            return f"Session ended but workspace archival failed. {output_dir} may need manual cleanup."

    server = Server("dataraum")
    server.experimental.enable_tasks()

    @server.list_tools()  # type: ignore[no-untyped-call, untyped-decorator]
    async def list_tools() -> list[Tool]:
        """List available tools."""
        return [
            # --- Orientation tools ---
            Tool(
                name="look",
                description=(
                    "Explore the data schema and profiles. Without target: dataset "
                    "overview (tables, columns, types, semantic annotations, "
                    "relationships). With target='table': column details + stats. "
                    "With target='table.column': full column profile (type "
                    "candidates, outliers, temporal, derived). With sample=N: "
                    "actual rows from the table. No entropy scores — use measure "
                    "for that."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "target": {
                            "type": "string",
                            "description": (
                                "Resolution level. Omit for dataset overview. "
                                "'table_name' for table detail. "
                                "'table_name.column_name' for column profile."
                            ),
                        },
                        "sample": {
                            "type": "integer",
                            "description": "Return N sample rows from the table. Requires target to include a table name.",
                        },
                    },
                },
            ),
            # --- Measurement tools ---
            Tool(
                name="measure",
                description=(
                    "Measure data entropy (uncertainty). Returns measurement "
                    "points per column+dimension, layer scores, and BBN readiness "
                    "per column. Triggers pipeline if no data exists yet. Use "
                    "target to filter to a specific table or column. Poll by "
                    "calling measure again to get partial results during a "
                    "pipeline run."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "target": {
                            "type": "string",
                            "description": (
                                "Filter to a table or column. E.g. 'orders' or 'orders.amount'."
                            ),
                        },
                    },
                },
                execution=ToolExecution(taskSupport="optional"),
            ),
            # --- Session tools ---
            Tool(
                name="begin_session",
                description=(
                    "Start an investigation session. You MUST call this before "
                    "using look, measure, query, or run_sql. First register "
                    "sources with add_source, then begin the session.\n\n"
                    "The contract defines data readiness thresholds for the "
                    "intended use case. Ask the user what they're trying to "
                    "accomplish, then choose the appropriate contract:\n\n"
                    "- exploratory_analysis: Data exploration, hypothesis testing (lenient)\n"
                    "- data_science: Feature engineering, model training (moderate)\n"
                    "- operational_analytics: Ops reports, team dashboards (moderate)\n"
                    "- aggregation_safe: SUM/AVG/COUNT queries — unit + null focus (moderate)\n"
                    "- executive_dashboard: C-level dashboards, KPI tracking (strict)\n"
                    "- regulatory_reporting: Financial statements, audit submissions (very strict)\n\n"
                    "If unsure, start with exploratory_analysis — you can always "
                    "start a new session with a stricter contract later."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["intent"],
                    "properties": {
                        "intent": {
                            "type": "string",
                            "description": "What you're investigating (e.g. 'check data quality for dashboard').",
                        },
                        "contract": {
                            "type": "string",
                            "description": (
                                "Contract name. Defaults to 'exploratory_analysis' if not provided."
                            ),
                        },
                    },
                },
            ),
            Tool(
                name="end_session",
                description=(
                    "End the current investigation session. Call this when the "
                    "investigation is complete, the user wants to start fresh, "
                    "or the request cannot be fulfilled. The workspace will be "
                    "archived. A fresh workspace is created when you next call "
                    "begin_session."
                ),
                inputSchema={
                    "type": "object",
                    "required": ["outcome"],
                    "properties": {
                        "outcome": {
                            "type": "string",
                            "enum": ["delivered", "refused", "escalated", "abandoned"],
                            "description": (
                                "Session outcome. 'delivered': analysis complete. "
                                "'refused': data unsuitable. 'escalated': needs human. "
                                "'abandoned': user stopped."
                            ),
                        },
                        "summary": {
                            "type": "string",
                            "description": "Brief justification for the outcome.",
                        },
                    },
                },
            ),
            # --- Query tools ---
            Tool(
                name="query",
                description=(
                    "Answer an analytical question using AI reasoning. "
                    "The query agent understands the data context — column "
                    "semantics, quality issues, business cycles — and writes "
                    "SQL that accounts for them. It tracks assumptions explicitly "
                    "and evaluates confidence against the active contract. "
                    "Prerequisites: call look first to understand the schema. "
                    "Use measure to understand data quality before asking "
                    "analytical questions. Returns: answer, confidence level, "
                    "assumptions, SQL steps, and result data. "
                    "Optionally export results to CSV, Parquet, or JSON with "
                    "export_format and export_name."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question about the data",
                        },
                        "export_format": {
                            "type": "string",
                            "enum": ["csv", "parquet", "json"],
                            "description": "Export results to a file. Omit to skip export.",
                        },
                        "export_name": {
                            "type": "string",
                            "description": "Filename stem for the export (e.g. 'revenue_by_month'). Auto-generated if omitted.",
                        },
                    },
                    "required": ["question"],
                },
            ),
            Tool(
                name="run_sql",
                description=(
                    "Execute SQL directly against the analyzed data. "
                    "Returns rows with per-column quality metadata when available.\n\n"
                    "Important: call look first to understand the schema, column "
                    "semantics, and quality issues. For analytical questions, "
                    "prefer query — it reasons over context automatically. "
                    "Use run_sql for spot-checks, drill-downs, or when you "
                    "already understand the data shape.\n\n"
                    "Snippets: previous queries are auto-saved as reusable "
                    "snippets in a knowledge base. The response includes a "
                    "snippet_summary showing how many were reused vs. newly saved. "
                    "Each step_id becomes both a temp view name (referenceable by "
                    "later steps) and a snippet key.\n\n"
                    "Column mappings: map output columns to source columns "
                    '(e.g. {"revenue": "orders.amount"}) to get per-column '
                    "quality metadata in the response."
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
                                "Mutually exclusive with 'sql'."
                            ),
                            "items": {
                                "type": "object",
                                "required": ["step_id", "sql"],
                                "properties": {
                                    "step_id": {
                                        "type": "string",
                                        "description": "Business concept name (e.g. 'monthly_revenue'). Used as view name by later steps.",
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
                                        "description": "Maps output columns to source columns for quality metadata lookup",
                                        "additionalProperties": {"type": "string"},
                                    },
                                },
                            },
                        },
                        "sql": {
                            "type": "string",
                            "description": (
                                "Raw SQL for quick one-off queries. Prefer 'steps' for "
                                "multi-stage analysis. CTE queries are auto-decomposed "
                                "into individual snippets. Mutually exclusive with 'steps'."
                            ),
                        },
                        "column_mappings": {
                            "type": "object",
                            "description": "Maps output columns to source columns for quality metadata (raw SQL mode).",
                            "additionalProperties": {"type": "string"},
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max rows to return. Default: 100, max: 10000.",
                            "default": 100,
                        },
                        "export_format": {
                            "type": "string",
                            "enum": ["csv", "parquet", "json"],
                            "description": "Export results to a file. Omit to skip export.",
                        },
                        "export_name": {
                            "type": "string",
                            "description": "Filename stem for the export (e.g. 'monthly_revenue'). Auto-generated if omitted.",
                        },
                    },
                },
            ),
            # --- Source management ---
            Tool(
                name="add_source",
                description=(
                    "Register a data source. Add sources before calling measure — "
                    "the pipeline processes them together. For files, provide a "
                    "path (file or directory). For databases, provide a backend type."
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
        """Execute a tool and return JSON results."""
        started_at = datetime.now(UTC)

        # --- Resolve session state from DB (read scalars inside scope) ---
        mgr = _get_manager()
        with mgr.session_scope() as session:
            active_session = _get_active_session(session)
            active_session_id = active_session.session_id if active_session else None
            active_contract = active_session.contract if active_session else None

        # --- Flow enforcement ---
        if name == "add_source":
            if active_session_id is not None:
                return _json_text_content(
                    {
                        "error": (
                            "Sources are sealed at session start. "
                            "Call end_session first, then add_source, then begin_session."
                        )
                    }
                )
        elif name == "begin_session":
            pass  # begin_session handles its own logic (idempotent resume)
        elif name == "end_session":
            if active_session_id is None:
                return _json_text_content({"error": "No active session to end."})
        else:
            # look, measure, query, run_sql — require active session
            if active_session_id is None:
                return _json_text_content({"error": "No active session. Call begin_session first."})

        # --- Dispatch (each tool gets its own session/cursor scope) ---
        result: dict[str, Any]

        if name == "look":
            with mgr.session_scope() as session, mgr.duckdb_cursor() as cursor:
                result = _look(
                    session,
                    target=arguments.get("target"),
                    sample=arguments.get("sample"),
                    cursor=cursor,
                )
        elif name == "measure":
            measure_target = arguments.get("target")
            with mgr.session_scope() as session:
                result = _measure(session, target=measure_target)
            # Pipeline trigger: when no entropy data exists, start a pipeline run
            if result.get("status") == "no_data":
                ctx = server.request_context
                measure_experimental: Experimental = ctx.experimental
                if measure_experimental and measure_experimental.is_task:
                    loop = asyncio.get_running_loop()
                    # Capture contract as local — session scope is closed
                    _contract = active_contract

                    async def _measure_work(task: ServerTaskContext) -> CallToolResult:
                        callback = _make_task_event_callback(task, loop)
                        await asyncio.to_thread(_run_pipeline, output_dir, callback, _contract)
                        with _get_manager().session_scope() as post_session:
                            measure_result = _measure(post_session, target=measure_target)
                        return CallToolResult(
                            content=[
                                TextContent(
                                    type="text",
                                    text=json.dumps(measure_result, indent=2, default=str),
                                )
                            ]
                        )

                    return await measure_experimental.run_task(
                        _measure_work,
                        model_immediate_response=(
                            "No entropy data yet. Triggering pipeline — "
                            "this typically takes 3-7 minutes. "
                            "Progress updates will follow."
                        ),
                    )
                else:
                    # No task API: fire-and-forget
                    bg = asyncio.create_task(_run_pipeline_background(output_dir, active_contract))
                    _background_tasks.add(bg)
                    bg.add_done_callback(_background_tasks.discard)
                    result = {
                        "status": "pipeline_triggered",
                        "hint": "Pipeline started. Call measure() again to poll for results.",
                    }
        elif name == "begin_session":
            if active_session is not None:
                # Idempotent: resume existing session instead of creating new
                result = _resume_session(mgr, active_session)
            else:
                with mgr.session_scope() as session:
                    result = _begin_session(
                        session,
                        intent=arguments["intent"],
                        contract=arguments.get("contract"),
                    )
                # _session_id is internal bookkeeping — strip from agent response
                result.pop("_session_id", None)
        elif name == "end_session":
            result = _end_session(
                mgr,
                session_id=active_session_id,  # type: ignore[arg-type]  # guarded above
                outcome=arguments.get("outcome", ""),
                summary=arguments.get("summary"),
            )
            # Archive workspace and reset manager for fresh next session
            if "error" not in result:
                assert active_session_id is not None  # guarded by flow enforcement
                archive_warning = _archive_and_reset(active_session_id)
                if archive_warning:
                    result["warning"] = archive_warning
        elif name == "query":
            with mgr.session_scope() as session, mgr.duckdb_cursor() as cursor:
                result = _query(session, cursor, arguments["question"], active_contract)
            # Export if requested — query nests data under result["data"]
            export_fmt = arguments.get("export_format")
            if export_fmt and "error" not in result and "data" in result:
                rows = result["data"].get("rows", [])
                export_input = {
                    "columns": result["data"].get("columns", []),
                    "rows": rows,
                    "steps_executed": result.get("execution_steps", []),
                }
                export_info = _export_tool_result(
                    export_input, root_dir, export_fmt, arguments.get("export_name"), tool="query"
                )
                if "export_path" in export_info:
                    result["export_path"] = export_info["export_path"]
                    # query display is truncated to 50 rows — warn if export hit ceiling
                    if len(rows) >= 50:
                        result["export_warning"] = (
                            "Query export limited to 50 rows (display limit). "
                            "Use run_sql with export_format for full dataset exports."
                        )
        elif name == "run_sql":
            with mgr.session_scope() as session, mgr.duckdb_cursor() as cursor:
                result = _run_sql(
                    session,
                    cursor,
                    steps=arguments.get("steps"),
                    sql=arguments.get("sql"),
                    column_mappings=arguments.get("column_mappings"),
                    limit=arguments.get("limit", 100),
                )
            # Export if requested
            export_fmt = arguments.get("export_format")
            if export_fmt and "error" not in result:
                export_info = _export_tool_result(
                    result, root_dir, export_fmt, arguments.get("export_name"), tool="run_sql"
                )
                if "export_path" in export_info:
                    result["export_path"] = export_info["export_path"]
        elif name == "add_source":
            with mgr.session_scope() as session, mgr.duckdb_cursor() as cursor:
                result = _add_source(session, cursor, arguments)
        else:
            result = {"error": f"Unknown tool: {name}"}

        # Record step in investigation trace (separate session scope for isolation).
        # begin_session/end_session excluded — session records capture their own state.
        if active_session_id is not None and name not in ("begin_session", "end_session"):
            _record_tool_step(
                mgr,
                session_id=active_session_id,
                tool_name=name,
                arguments=arguments,
                result=result,
                started_at=started_at,
            )

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


def _resume_session(
    manager: ConnectionManager,
    active_session: InvestigationSession,
) -> dict[str, Any]:
    """Resume an existing active session.

    Returns orientation info matching the _begin_session response shape,
    with a hint that the session is being resumed.

    Args:
        manager: Server-level ConnectionManager.
        active_session: The active InvestigationSession from DB.

    Returns:
        Dict with sources, contract, pipeline data status, and resume hint.
    """
    from sqlalchemy import select

    from dataraum.entropy.contracts import get_contract
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.storage import Source

    with manager.session_scope() as session:
        all_sources = list(
            session.execute(
                select(Source).where(Source.archived_at.is_(None)).order_by(Source.created_at)
            )
            .scalars()
            .all()
        )

        source = _get_pipeline_source(session)
        source_id = source.source_id if source else active_session.source_id

        has_data = (
            session.execute(
                select(EntropyObjectRecord.object_id)
                .where(EntropyObjectRecord.source_id == source_id)
                .limit(1)
            ).scalar_one_or_none()
            is not None
        )

    contract_profile = get_contract(active_session.contract or "exploratory_analysis")
    contract_info = (
        {
            "name": contract_profile.name,
            "display_name": contract_profile.display_name,
            "description": contract_profile.description,
        }
        if contract_profile
        else {
            "name": active_session.contract or "unknown",
            "display_name": active_session.contract or "unknown",
        }
    )

    return {
        "sources": [s.name for s in all_sources],
        "contract": contract_info,
        "has_pipeline_data": has_data,
        "resumed": True,
        "step_count": active_session.step_count,
        "hint": (
            "Resuming session from earlier. If you'd like to start fresh, call end_session first."
        ),
    }


def _end_session(
    manager: ConnectionManager,
    session_id: str,
    outcome: str,
    summary: str | None = None,
) -> dict[str, Any]:
    """End the active investigation session.

    Closes the session record in the DB. Workspace archival and manager
    reset are handled by the caller (_archive_and_reset in call_tool).

    Args:
        manager: Server-level ConnectionManager.
        session_id: Active session to close.
        outcome: One of delivered, refused, escalated, abandoned.
        summary: Agent's justification for the outcome.

    Returns:
        Dict with session outcome summary.
    """
    from dataraum.investigation.recorder import end_session

    _VALID_OUTCOMES = {"delivered", "refused", "escalated", "abandoned"}
    if outcome not in _VALID_OUTCOMES:
        return {"error": f"Invalid outcome '{outcome}'. Must be one of: {sorted(_VALID_OUTCOMES)}"}

    try:
        with manager.session_scope() as session:
            inv = end_session(session, session_id, outcome, summary=summary)
            return {
                "status": "ended",
                "outcome": inv.status,
                "duration_seconds": inv.duration_seconds,
                "step_count": inv.step_count,
                "summary": inv.outcome_summary,
            }
    except ValueError as e:
        return {"error": str(e)}


def _record_tool_step(
    manager: ConnectionManager,
    session_id: str,
    tool_name: str,
    arguments: dict[str, Any],
    result: dict[str, Any],
    started_at: datetime,
) -> None:
    """Record a tool invocation in the investigation session trace.

    Uses its own session scope from the server-level manager for isolation —
    recording failures must not affect tool execution or its transaction.
    """
    from dataraum.investigation.recorder import record_step

    try:
        with manager.session_scope() as db_session:
            duration = (datetime.now(UTC) - started_at).total_seconds()
            status = "error" if "error" in result else "success"
            record_step(
                db_session,
                session_id,
                tool_name,
                arguments,
                status=status,
                result=result,
                error=result.get("error") if status == "error" else None,
                started_at=started_at,
                duration_seconds=duration,
            )
    except Exception:
        _log.debug("Failed to record step for session %s", session_id, exc_info=True)


def _get_phase_labels() -> dict[str, str]:
    """Load phase descriptions from pipeline.yaml for progress reporting."""
    from dataraum.pipeline.pipeline_config import load_phase_declarations

    declarations = load_phase_declarations()
    return {name: decl.description for name, decl in declarations.items()}


def _run_pipeline(
    output_dir: Path,
    event_callback: EventCallback | None = None,
    contract: str | None = None,
) -> dict[str, Any]:
    """Run the pipeline on registered sources (multi-source mode).

    Always runs in multi-source mode (source_path=None) — sources are
    registered via add_source and read from the database by the import phase.

    Args:
        output_dir: Pipeline output directory.
        event_callback: Optional callback for pipeline events.
        contract: Active contract name from the session.

    Returns:
        Dict with pipeline result.
    """
    from dataraum.pipeline.runner import RunConfig, run

    config = RunConfig(
        source_path=None,
        output_dir=output_dir,
        event_callback=event_callback,
        contract=contract,
    )

    result = run(config)

    if not result.success or not result.value:
        return {"error": f"Pipeline failed: {result.error}"}

    return {"status": "complete", "phases_completed": result.value.phases_completed}


async def _run_pipeline_background(output_dir: Path, contract: str | None = None) -> None:
    """Run _run_pipeline in a background thread, logging errors."""
    try:
        await asyncio.to_thread(_run_pipeline, output_dir, None, contract)
    except Exception:
        _log.exception("Background pipeline failed for %s", output_dir)


def _query(
    session: SASession,
    cursor: Any,
    question: str,
    contract_name: str | None = None,
) -> dict[str, Any]:
    """Execute a natural language query.

    Args:
        session: SQLAlchemy session from server-level manager.
        cursor: DuckDB cursor from server-level manager.
        question: Natural language question.
        contract_name: Active contract name for confidence evaluation.
    """
    from dataraum.query import answer_question

    source = _get_pipeline_source(session)
    if not source:
        return {"error": "No sources found"}

    result = answer_question(
        question=question,
        session=session,
        duckdb_conn=cursor,
        source_id=source.source_id,
        contract=contract_name,
    )

    if not result.success or not result.value:
        return {"error": str(result.error)}

    qr = result.value
    if not qr.success:
        return {"error": qr.error or "Query generation failed"}

    return format_query_result(qr)


def _run_sql(
    session: SASession,
    cursor: Any,
    steps: list[dict[str, Any]] | None = None,
    sql: str | None = None,
    column_mappings: dict[str, str] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Execute SQL directly against analyzed data.

    Args:
        session: SQLAlchemy session from server-level manager.
        cursor: DuckDB cursor from server-level manager.
        steps: Structured SQL steps.
        sql: Raw SQL string.
        column_mappings: Maps output column names to source columns (raw SQL mode).
        limit: Max rows to return.
    """
    from sqlalchemy import select

    from dataraum.mcp.sql_executor import run_sql
    from dataraum.storage import Table

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


def _export_tool_result(
    result: dict[str, Any],
    root_dir: Path,
    fmt: str,
    name: str | None = None,
    *,
    tool: str = "run_sql",
) -> dict[str, Any]:
    """Export MCP tool results to a file via export_data().

    Args:
        result: Tool result dict with 'columns' and 'rows'.
        root_dir: DataRaum root directory (exports go to root/exports/).
        fmt: Export format — csv, parquet, or json.
        name: Filename stem. Auto-generated if omitted.
        tool: Tool name for auto-generated filenames.

    Returns:
        Dict with export_path, format, row_count.
    """
    import re

    from dataraum.export import export_data

    columns = result.get("columns", [])
    rows = result.get("rows", [])
    if not columns or not rows:
        return {"error": "No data to export"}

    # Sanitize filename stem — strip path separators and special chars
    raw_stem = name or f"{tool}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    stem = re.sub(r"[^\w\-]", "_", raw_stem)[:128]
    export_dir = (root_dir / "exports").resolve()
    output_path = export_dir / f"{stem}.{fmt}"

    # Defense-in-depth: verify path stays inside exports dir
    if not str(output_path.resolve()).startswith(str(export_dir)):
        return {"error": "Invalid export name"}

    try:
        exported = export_data(
            columns,
            rows,
            output_path,
            fmt,  # type: ignore[arg-type]
            metadata={"steps_executed": result.get("steps_executed", [])},
        )
        return {"export_path": str(exported), "format": fmt, "row_count": len(rows)}
    except Exception as e:
        return {"error": f"Export failed: {e}"}


def _check_prerequisites() -> str | None:
    """Check hard prerequisites before starting a session.

    Returns an error message if any check fails, None if all pass.
    """
    import os

    errors: list[str] = []

    # API key: load config to get the env var name, then probe
    try:
        from dataraum.llm.config import load_llm_config

        llm_config = load_llm_config()
        provider_config = llm_config.providers.get(llm_config.active_provider)
        if provider_config:
            env_var = provider_config.api_key_env
            if not os.getenv(env_var):
                errors.append(
                    f"Missing {env_var}. The pipeline requires an LLM API key. "
                    f"Set it via: export {env_var}=<your-api-key> "
                    f"or add it to a .env file."
                )
    except Exception:
        pass  # Config load failure is not a prereq error — pipeline will report it

    if not errors:
        return None
    return " | ".join(errors)


def _begin_session(
    session: SASession,
    intent: str,
    contract: str | None = None,
) -> dict[str, Any]:
    """Start an investigation session.

    Creates an InvestigationSession and returns orientation info.
    The ``_session_id`` key is popped by call_tool for server-side state —
    it is never surfaced to the agent.

    Args:
        session: SQLAlchemy session from server-level manager.
        intent: What the agent is investigating.
        contract: Contract name. Defaults to ``exploratory_analysis``.

    Returns:
        Dict with _session_id (internal), sources, contract, has_pipeline_data, hint.
    """
    from sqlalchemy import select

    from dataraum.entropy.contracts import get_contract
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.investigation.recorder import begin_session
    from dataraum.storage import Source

    # --- Prerequisite checks (fail fast with actionable messages) ---
    prereq_errors = _check_prerequisites()
    if prereq_errors:
        return {"error": prereq_errors}

    # Validate and default contract
    contract_name = contract or "exploratory_analysis"
    contract_profile = get_contract(contract_name)
    if contract_profile is None:
        from dataraum.entropy.contracts import list_contracts

        available = [c["name"] for c in list_contracts()]
        return {"error": f"Unknown contract '{contract_name}'. Available: {available}"}

    # Require at least one registered source
    all_sources = list(
        session.execute(
            select(Source).where(Source.archived_at.is_(None)).order_by(Source.created_at)
        )
        .scalars()
        .all()
    )
    if not all_sources:
        return {"error": "No sources registered. Use add_source first."}

    source = _get_pipeline_source(session)
    source_id = source.source_id if source else all_sources[0].source_id

    # Create investigation session
    inv = begin_session(session, source_id, intent, contract=contract_name)

    # Check if pipeline has run (quick existence check, not full measurement)
    has_data = (
        session.execute(
            select(EntropyObjectRecord.object_id)
            .where(EntropyObjectRecord.source_id == source_id)
            .limit(1)
        ).scalar_one_or_none()
        is not None
    )

    return {
        "_session_id": inv.session_id,
        "sources": [s.name for s in all_sources],
        "contract": {
            "name": contract_profile.name,
            "display_name": contract_profile.display_name,
            "description": contract_profile.description,
        },
        "has_pipeline_data": has_data,
        "hint": (
            "Use look to explore the schema, measure to check entropy scores."
            if has_data
            else "No pipeline data yet. Call measure to trigger the pipeline."
        ),
    }


def _resolve_table_name(tables: list[TableModel], name: str) -> TableModel | None:
    """Resolve a table name, supporting short names without source prefix.

    Tries exact match first, then suffix match (e.g. "invoices" → "zone1__invoices").
    Returns None if no match or ambiguous (multiple suffix matches).
    """
    exact = next((t for t in tables if t.table_name == name), None)
    if exact:
        return exact
    suffix = f"__{name}"
    matches = [t for t in tables if t.table_name.endswith(suffix)]
    if len(matches) == 1:
        return matches[0]
    return None


def _aggregate_layer_scores(scores: dict[str, float]) -> dict[str, float]:
    """Aggregate dimension scores by top-level layer (mean per layer)."""
    layer_totals: dict[str, list[float]] = {}
    for dim_path, score in scores.items():
        layer = dim_path.split(".")[0]
        layer_totals.setdefault(layer, []).append(score)
    return {layer: round(sum(vals) / len(vals), 4) for layer, vals in layer_totals.items()}


def _look(
    session: SASession,
    target: str | None = None,
    sample: int | None = None,
    *,
    cursor: Any | None = None,
) -> dict[str, Any]:
    """Explore data schema and profiles at varying resolution.

    Args:
        session: SQLAlchemy session from server-level manager.
        target: None=dataset, "table"=table detail, "table.col"=column profile.
        sample: If set, return N sample rows from the table.
        cursor: DuckDB cursor, required for sample mode.

    Returns:
        Schema/profile data at the requested resolution level.
    """
    from sqlalchemy import select

    from dataraum.storage import Column, Table

    source = _get_pipeline_source(session)
    if not source:
        return {"error": "No sources found. Use add_source first."}

    tables = list(
        session.execute(
            select(Table).where(
                Table.source_id == source.source_id,
                Table.layer == "typed",
            )
        )
        .scalars()
        .all()
    )
    if not tables:
        return {"error": "No tables found. Run the pipeline first."}

    # Parse target
    table_name: str | None = None
    column_name: str | None = None
    if target:
        parts = target.split(".", 1)
        table_name = parts[0]
        if len(parts) == 2:
            column_name = parts[1]

    # Resolve short table names (e.g. "invoices" → "zone1__invoices")
    resolved_table: TableModel | None = None
    if table_name:
        resolved_table = _resolve_table_name(tables, table_name)
        if not resolved_table:
            available = [t.table_name for t in tables]
            return {"error": f"Table '{table_name}' not found. Available: {available}"}
        table_name = resolved_table.table_name

    # Sample mode: return actual rows
    if sample is not None:
        if not table_name:
            return {
                "error": "sample requires a target table (e.g. look(target='orders', sample=10))"
            }
        return _look_sample(cursor, table_name, min(sample, 1000))

    # Column level
    if column_name:
        assert resolved_table is not None
        col = session.execute(
            select(Column).where(
                Column.table_id == resolved_table.table_id,
                Column.column_name == column_name,
            )
        ).scalar_one_or_none()
        if not col:
            return {"error": f"Column '{column_name}' not found in table '{table_name}'."}
        assert table_name is not None  # guaranteed by column_name being set
        return _look_column(session, col, table_name)

    # Table level
    if table_name:
        assert resolved_table is not None
        return _look_table(session, resolved_table)

    # Dataset level
    return _look_dataset(session, tables)


def _look_dataset(session: SASession, tables: list[TableModel]) -> dict[str, Any]:
    """Dataset overview: tables, columns, semantic annotations, relationships."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
    from dataraum.storage import Column

    table_ids = [t.table_id for t in tables]

    # Bulk load: entities, columns, annotations (avoid N+1)
    entities_by_table: dict[str, Any] = {
        e.table_id: e
        for e in session.execute(select(TableEntity).where(TableEntity.table_id.in_(table_ids)))
        .scalars()
        .all()
    }

    all_columns = list(
        session.execute(
            select(Column)
            .where(Column.table_id.in_(table_ids))
            .order_by(Column.table_id, Column.column_position)
        )
        .scalars()
        .all()
    )
    col_ids = [c.column_id for c in all_columns]

    annotations_by_col: dict[str, Any] = {}
    if col_ids:
        annotations_by_col = {
            a.column_id: a
            for a in session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(col_ids))
            )
            .scalars()
            .all()
        }

    # Group columns by table
    columns_by_table: dict[str, list[Any]] = {}
    for col in all_columns:
        columns_by_table.setdefault(col.table_id, []).append(col)

    result_tables = []
    for tbl in tables:
        entity = entities_by_table.get(tbl.table_id)
        columns = columns_by_table.get(tbl.table_id, [])

        col_list = []
        for col in columns:
            ann = annotations_by_col.get(col.column_id)
            col_info: dict[str, Any] = {
                "name": col.column_name,
                "type": col.resolved_type or col.raw_type,
            }
            if ann:
                if ann.semantic_role:
                    col_info["semantic_role"] = ann.semantic_role
                if ann.business_name:
                    col_info["business_name"] = ann.business_name
                if ann.business_concept:
                    col_info["business_concept"] = ann.business_concept
                if ann.entity_type:
                    col_info["entity_type"] = ann.entity_type
                if ann.temporal_behavior:
                    col_info["temporal_behavior"] = ann.temporal_behavior
                if ann.unit_source_column:
                    col_info["unit_source_column"] = ann.unit_source_column
            col_list.append(col_info)

        table_info: dict[str, Any] = {
            "name": tbl.table_name,
            "row_count": tbl.row_count,
            "columns": col_list,
        }
        if entity:
            table_info["entity_type"] = entity.detected_entity_type
            table_info["is_fact_table"] = entity.is_fact_table
            if entity.description:
                table_info["description"] = entity.description
            if entity.time_column:
                table_info["time_column"] = entity.time_column
            grain_cols = entity.grain_columns
            if grain_cols:
                table_info["grain"] = grain_cols

        result_tables.append(table_info)

    # Relationships — LLM-confirmed only, eager-load columns+tables to avoid lazy N+1
    rels = list(
        session.execute(
            select(Relationship)
            .where(
                Relationship.from_table_id.in_(table_ids),
                Relationship.detection_method == "llm",
            )
            .options(
                selectinload(Relationship.from_column).selectinload(Column.table),
                selectinload(Relationship.to_column).selectinload(Column.table),
            )
        )
        .scalars()
        .all()
    )

    relationships = [
        {
            "from": f"{rel.from_column.table.table_name}.{rel.from_column.column_name}",
            "to": f"{rel.to_column.table.table_name}.{rel.to_column.column_name}",
            "type": rel.relationship_type,
            "cardinality": rel.cardinality,
            "confidence": round(rel.confidence, 2),
        }
        for rel in rels
    ]

    return {"tables": result_tables, "relationships": relationships}


def _look_table(session: SASession, tbl: TableModel) -> dict[str, Any]:
    """Table detail: columns with stats from StatisticalProfile."""
    from sqlalchemy import select

    from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
    from dataraum.analysis.statistics.db_models import StatisticalProfile
    from dataraum.storage import Column

    entity = session.execute(
        select(TableEntity).where(TableEntity.table_id == tbl.table_id)
    ).scalar_one_or_none()

    columns = list(
        session.execute(
            select(Column).where(Column.table_id == tbl.table_id).order_by(Column.column_position)
        )
        .scalars()
        .all()
    )

    # Bulk load annotations and profiles (avoid N+1)
    col_ids = [c.column_id for c in columns]
    annotations_by_col: dict[str, Any] = {}
    profiles_by_col: dict[str, Any] = {}
    if col_ids:
        annotations_by_col = {
            a.column_id: a
            for a in session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(col_ids))
            )
            .scalars()
            .all()
        }
        # Latest typed-layer profile per column
        all_profiles = list(
            session.execute(
                select(StatisticalProfile)
                .where(
                    StatisticalProfile.column_id.in_(col_ids),
                    StatisticalProfile.layer == "typed",
                )
                .order_by(StatisticalProfile.profiled_at.desc())
            )
            .scalars()
            .all()
        )
        for p in all_profiles:
            if p.column_id not in profiles_by_col:
                profiles_by_col[p.column_id] = p

    col_list = []
    for col in columns:
        ann = annotations_by_col.get(col.column_id)
        profile = profiles_by_col.get(col.column_id)

        col_info: dict[str, Any] = {
            "name": col.column_name,
            "type": col.resolved_type or col.raw_type,
        }
        if ann:
            if ann.semantic_role:
                col_info["semantic_role"] = ann.semantic_role
            if ann.business_name:
                col_info["business_name"] = ann.business_name
            if ann.business_concept:
                col_info["business_concept"] = ann.business_concept
            if ann.unit_source_column:
                col_info["unit_source_column"] = ann.unit_source_column

        if profile:
            col_info["nullable"] = profile.null_count > 0
            stats: dict[str, Any] = {
                "total_count": profile.total_count,
                "null_count": profile.null_count,
                "distinct_count": profile.distinct_count,
                "cardinality_ratio": profile.cardinality_ratio,
            }
            pd = profile.profile_data or {}
            if "numeric_stats" in pd:
                stats["numeric"] = pd["numeric_stats"]
            if "top_values" in pd:
                stats["top_values"] = pd["top_values"][:10]
            col_info["stats"] = stats

        col_list.append(col_info)

    result: dict[str, Any] = {
        "name": tbl.table_name,
        "row_count": tbl.row_count,
        "columns": col_list,
    }
    if entity:
        result["entity_type"] = entity.detected_entity_type
        result["is_fact_table"] = entity.is_fact_table
        if entity.description:
            result["description"] = entity.description
        if entity.time_column:
            result["time_column"] = entity.time_column
        if entity.grain_columns:
            result["grain"] = entity.grain_columns

    return result


def _look_column(session: SASession, col: ColumnModel, table_name: str) -> dict[str, Any]:
    """Full column profile: types, stats, outliers, temporal, derived."""
    from sqlalchemy import select

    from dataraum.analysis.correlation.db_models import DerivedColumn
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.statistics.db_models import StatisticalProfile
    from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics
    from dataraum.analysis.temporal.db_models import TemporalColumnProfile
    from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision

    result: dict[str, Any] = {
        "name": col.column_name,
        "table": table_name,
        "type": col.resolved_type or col.raw_type,
    }

    # Semantic annotation
    ann = session.execute(
        select(SemanticAnnotation).where(SemanticAnnotation.column_id == col.column_id)
    ).scalar_one_or_none()
    if ann:
        semantic: dict[str, Any] = {}
        if ann.semantic_role:
            semantic["role"] = ann.semantic_role
        if ann.business_name:
            semantic["business_name"] = ann.business_name
        if ann.business_concept:
            semantic["business_concept"] = ann.business_concept
        if ann.entity_type:
            semantic["entity_type"] = ann.entity_type
        if ann.temporal_behavior:
            semantic["temporal_behavior"] = ann.temporal_behavior
        if semantic:
            result["semantic"] = semantic

    # Statistical profile
    profile = session.execute(
        select(StatisticalProfile)
        .where(
            StatisticalProfile.column_id == col.column_id,
            StatisticalProfile.layer == "typed",
        )
        .order_by(StatisticalProfile.profiled_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if profile:
        result["stats"] = profile.profile_data

    # Type candidates
    candidates = list(
        session.execute(
            select(TypeCandidate)
            .where(TypeCandidate.column_id == col.column_id)
            .order_by(TypeCandidate.confidence.desc())
        )
        .scalars()
        .all()
    )
    if candidates:
        result["type_candidates"] = [
            {
                "type": tc.data_type,
                "confidence": round(tc.confidence, 3),
                "parse_success_rate": round(tc.parse_success_rate, 3)
                if tc.parse_success_rate
                else None,
                "detected_pattern": tc.detected_pattern,
                "detected_unit": tc.detected_unit,
            }
            for tc in candidates
        ]

    # Type decision
    decision = session.execute(
        select(TypeDecision).where(TypeDecision.column_id == col.column_id)
    ).scalar_one_or_none()
    if decision:
        result["type_decision"] = {
            "type": decision.decided_type,
            "source": decision.decision_source,
            "reason": decision.decision_reason,
        }

    # Outlier / quality metrics
    quality = session.execute(
        select(StatisticalQualityMetrics)
        .where(StatisticalQualityMetrics.column_id == col.column_id)
        .order_by(StatisticalQualityMetrics.computed_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if quality:
        outlier_info: dict[str, Any] = {
            "has_outliers": quality.has_outliers,
            "iqr_outlier_ratio": quality.iqr_outlier_ratio,
            "zscore_outlier_ratio": quality.zscore_outlier_ratio,
        }
        if quality.benford_compliant is not None:
            outlier_info["benford_compliant"] = quality.benford_compliant
        if quality.quality_data:
            qd = quality.quality_data
            if "benford" in qd:
                outlier_info["benford"] = qd["benford"]
            if "outlier_details" in qd:
                outlier_info["outlier_details"] = qd["outlier_details"]
        result["quality"] = outlier_info

    # Temporal profile
    temporal = session.execute(
        select(TemporalColumnProfile)
        .where(TemporalColumnProfile.column_id == col.column_id)
        .order_by(TemporalColumnProfile.profiled_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if temporal:
        result["temporal"] = {
            "min_timestamp": str(temporal.min_timestamp) if temporal.min_timestamp else None,
            "max_timestamp": str(temporal.max_timestamp) if temporal.max_timestamp else None,
            "granularity": temporal.detected_granularity,
            "completeness": temporal.completeness_ratio,
            "has_seasonality": temporal.has_seasonality,
            "has_trend": temporal.has_trend,
            "is_stale": temporal.is_stale,
        }

    # Derived column relationships
    derived = list(
        session.execute(
            select(DerivedColumn).where(DerivedColumn.derived_column_id == col.column_id)
        )
        .scalars()
        .all()
    )
    if derived:
        result["derived"] = [
            {
                "type": d.derivation_type,
                "formula": d.formula,
                "match_rate": round(d.match_rate, 3) if d.match_rate else None,
            }
            for d in derived
        ]

    return result


def _look_sample(cursor: Any, table_name: str, n: int) -> dict[str, Any]:
    """Return sample rows from a typed table."""
    try:
        result = cursor.execute(f"SELECT * FROM typed_{table_name} LIMIT {int(n)}")
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return {
            "table": table_name,
            "columns": columns,
            "rows": [list(row) for row in rows],
            "row_count": len(rows),
        }
    except Exception as e:
        return {"error": f"Failed to sample table '{table_name}': {e}"}


def _measure(
    session: SASession,
    target: str | None = None,
) -> dict[str, Any]:
    """Measure entropy across detectors, returning points + readiness.

    Args:
        session: SQLAlchemy session from server-level manager.
        target: Optional filter — "table" or "table.column".

    Returns:
        Dict with status, points, scores (by layer), readiness (per column).
    """
    from sqlalchemy import select

    from dataraum.entropy.detectors.base import get_default_registry
    from dataraum.entropy.measurement import measure_entropy
    from dataraum.pipeline.db_models import PhaseLog, PipelineRun
    from dataraum.storage import Column, Table

    source = _get_pipeline_source(session)
    if not source:
        return {"error": "No sources found. Use add_source first."}

    tables = list(
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

    # Get entropy measurements + pipeline status (single query for latest_run)
    registry = get_default_registry()
    detector_ids = [d.detector_id for d in registry.get_all_detectors()]
    measurement = measure_entropy(session, source.source_id, detector_ids)

    latest_run = session.execute(
        select(PipelineRun)
        .where(PipelineRun.source_id == source.source_id)
        .order_by(PipelineRun.started_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if not measurement.scores:
        if latest_run and latest_run.status == "running":
            completed_names = [
                row[0]
                for row in session.execute(
                    select(PhaseLog.phase_name).where(
                        PhaseLog.run_id == latest_run.run_id,
                        PhaseLog.status == "completed",
                    )
                ).all()
            ]
            return {
                "status": "running",
                "phases_completed": completed_names,
                "hint": "Pipeline is running. Call measure() again to poll.",
            }

        return {
            "status": "no_data",
            "hint": "No entropy data. Pipeline will be triggered.",
        }

    # Build points from column_details + table_details
    points: list[dict[str, Any]] = []
    for dim_path, targets in measurement.column_details.items():
        for tgt, score in targets.items():
            points.append({"target": tgt, "dimension": dim_path, "score": round(score, 4)})
    for dim_path, targets in measurement.table_details.items():
        for tgt, score in targets.items():
            points.append({"target": tgt, "dimension": dim_path, "score": round(score, 4)})

    scores = _aggregate_layer_scores(measurement.scores)

    # BBN readiness per column
    readiness: dict[str, str] = {}
    if table_ids:
        try:
            from dataraum.entropy.views.network_context import build_for_network

            network = build_for_network(session, table_ids)
            if network and network.columns:
                readiness = {
                    col_key: col_result.readiness for col_key, col_result in network.columns.items()
                }
                # Aggregate table-level readiness (worst-of columns per table)
                table_cols: dict[str, list[str]] = {}
                for col_key, col_result in network.columns.items():
                    # col_key is "column:table.col" — extract table
                    bare = col_key.removeprefix("column:")
                    tbl = bare.split(".")[0]
                    table_cols.setdefault(tbl, []).append(col_result.readiness)
                _rank = {"blocked": 2, "investigate": 1, "ready": 0}
                for tbl, col_readiness_list in table_cols.items():
                    readiness[f"table:{tbl}"] = max(
                        col_readiness_list, key=lambda r: _rank.get(r, 0)
                    )
                # Dataset-level readiness from BBN
                readiness["dataset"] = network.overall_readiness
        except Exception:
            _log.debug("BBN readiness unavailable", exc_info=True)

    status = "complete"
    phases_completed: list[str] = []
    if latest_run:
        if latest_run.status == "running":
            status = "running"
        phases_completed = [
            row[0]
            for row in session.execute(
                select(PhaseLog.phase_name).where(
                    PhaseLog.run_id == latest_run.run_id,
                    PhaseLog.status == "completed",
                )
            ).all()
        ]

    result: dict[str, Any] = {
        "status": status,
        "phases_completed": phases_completed,
        "points": points,
        "scores": scores,
        "readiness": readiness,
    }

    # Filter by target if provided
    if target:
        parts = target.split(".", 1)
        raw_table = parts[0]
        col_name = parts[1] if len(parts) == 2 else None

        resolved = _resolve_table_name(tables, raw_table)
        if not resolved:
            available = [t.table_name for t in tables]
            return {"error": f"Table '{raw_table}' not found. Available: {available}"}

        full_table = resolved.table_name

        if col_name:
            col_exists = session.execute(
                select(Column).where(
                    Column.table_id == resolved.table_id,
                    Column.column_name == col_name,
                )
            ).scalar_one_or_none()
            if not col_exists:
                return {"error": f"Column '{col_name}' not found in table '{full_table}'."}
            col_target = f"column:{full_table}.{col_name}"
            result["points"] = [p for p in points if p["target"] == col_target]
            result["readiness"] = {k: v for k, v in readiness.items() if k == col_target}
        else:
            table_target = f"table:{full_table}"
            col_prefix = f"column:{full_table}."
            result["points"] = [
                p
                for p in points
                if p["target"] == table_target or p["target"].startswith(col_prefix)
            ]
            result["readiness"] = {
                k: v for k, v in readiness.items() if k == table_target or k.startswith(col_prefix)
            }

        # Recompute scores from filtered points
        if result["points"]:
            layer_totals: dict[str, list[float]] = {}
            for p in result["points"]:
                layer = p["dimension"].split(".")[0]
                layer_totals.setdefault(layer, []).append(p["score"])
            result["scores"] = {
                layer: round(sum(vals) / len(vals), 4) for layer, vals in layer_totals.items()
            }
        else:
            result["scores"] = {}

    return result


def _add_source(
    session: SASession,
    cursor: Any,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Register a new data source.

    Args:
        session: SQLAlchemy session from server-level manager.
        cursor: DuckDB cursor (used for database backend sources).
        arguments: Tool arguments (name, path or backend, etc.).
    """
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

    credential_chain = CredentialChain()

    if backend:
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
        session.execute(select(Source.name).where(Source.archived_at.is_(None))).scalars().all()
    )
    output["registered_sources"] = {
        "count": len(all_sources),
        "names": list(all_sources),
    }
    output["next_steps"] = (
        "Add more sources with add_source, or call measure to trigger the pipeline."
    )

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
