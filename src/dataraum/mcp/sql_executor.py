"""Direct SQL execution with quality metadata and snippet integration.

Core logic for the run_sql MCP tool. Converts caller-provided SQL (raw or
structured steps) into execute_sql_steps() calls, enriches results with
per-column quality metadata, and integrates with the snippet library.
"""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dataraum.query.execution import ExecutionResult, SQLStep, execute_sql_steps

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

_log = logging.getLogger(__name__)

# Hard ceiling on row limit to prevent enormous responses.
MAX_ROW_LIMIT = 10_000
DEFAULT_ROW_LIMIT = 100


def run_sql(
    cursor: duckdb.DuckDBPyConnection,
    *,
    session: Session | None = None,
    source_id: str | None = None,
    table_ids: list[str] | None = None,
    steps: list[dict[str, Any]] | None = None,
    sql: str | None = None,
    column_mappings: dict[str, str] | None = None,
    limit: int = DEFAULT_ROW_LIMIT,
) -> dict[str, Any]:
    """Execute SQL and return results as a structured dict.

    Args:
        cursor: DuckDB connection.
        session: SQLAlchemy session (needed for quality metadata and snippets).
        source_id: Pipeline source ID (needed for snippet integration).
        table_ids: Typed table IDs (needed for quality metadata).
        steps: Structured SQL steps (list of dicts with step_id, sql, description,
            optional column_mappings).
        sql: Raw SQL convenience mode. Mutually exclusive with steps.
        column_mappings: Maps output column names to source column names (raw SQL mode).
        limit: Max rows to return. Capped at MAX_ROW_LIMIT.

    Returns:
        Dict with columns, rows, row_count, truncated, steps_executed.
        On error, returns dict with "error" key.
    """
    # --- Validate input ---
    if steps is not None and sql is not None:
        return {"error": "Provide either 'steps' or 'sql', not both."}
    if steps is None and sql is None:
        return {"error": "Provide either 'steps' or 'sql'."}
    if steps is not None and len(steps) == 0:
        return {"error": "steps list cannot be empty."}

    # Clamp limit
    effective_limit = min(max(1, limit), MAX_ROW_LIMIT)

    # --- Build SQLStep list + final_sql ---
    sql_steps: list[SQLStep]
    final_sql: str
    raw_steps = steps  # preserve original dicts for column_mappings

    if sql is not None:
        # Try CTE decomposition first — each CTE becomes its own reusable snippet.
        from dataraum.mcp.cte_parser import decompose_ctes

        decomposition = decompose_ctes(sql, column_mappings)
        if decomposition is not None:
            sql_steps = [
                SQLStep(
                    step_id=s["step_id"],
                    sql=s["sql"],
                    description=s.get("description", ""),
                )
                for s in decomposition.steps
            ]
            final_sql = decomposition.final_sql
            raw_steps = decomposition.steps
        else:
            # Fallback: wrap raw SQL as single step.
            # step_id is always "query" (used as the temp view name).
            # snippet_key uses a content hash so different SQL strings don't collide.
            sql_steps = [SQLStep(step_id="query", sql=sql, description="Raw SQL query")]
            final_sql = "SELECT * FROM query"
            snippet_key = f"query_{hashlib.sha256(sql.encode()).hexdigest()[:12]}"
            raw_steps = [
                {
                    "step_id": "query",
                    "sql": sql,
                    "description": "Raw SQL query",
                    "_snippet_key": snippet_key,
                },
            ]
    else:
        assert steps is not None
        sql_steps = [
            SQLStep(
                step_id=s["step_id"],
                sql=s["sql"],
                description=s.get("description", ""),
            )
            for s in steps
        ]
        # Final SQL selects from the last step
        last_step_id = sql_steps[-1].step_id
        final_sql = f"SELECT * FROM {last_step_id}"

    # --- Snippet lookup (before execution) ---
    session_source: str | None = None
    snippet_matches: dict[str, tuple[str, str]] = {}  # step_id → (status, snippet_id)
    if session is not None and source_id:
        session_source = f"mcp:session_{uuid4().hex[:8]}"
        snippet_matches = _lookup_snippets(session, source_id, sql_steps, raw_steps or [])

    # --- Execute ---
    result = execute_sql_steps(
        steps=sql_steps,
        final_sql=final_sql,
        duckdb_conn=cursor,
        repair_fn=None,
        return_table=True,
    )

    is_error = not result.success or not result.value

    # --- Snippet save/failure (after execution) ---
    snippet_summary: dict[str, Any] | None = None
    if session is not None and source_id:
        if is_error:
            # Record failures for matched snippets
            failed_ids = [sid for _, (_, sid) in snippet_matches.items() if sid]
            if failed_ids:
                try:
                    from dataraum.query.snippet_library import SnippetLibrary

                    library = SnippetLibrary(session)
                    library.record_failure(failed_ids)
                except Exception:
                    _log.debug("Snippet failure recording failed", exc_info=True)
        else:
            # Save novel steps as snippets
            assert session_source is not None  # set inside the same guard
            snippet_summary = _save_snippets(
                session,
                source_id,
                sql_steps,
                raw_steps or [],
                snippet_matches,
                session_source,
            )

    if is_error:
        _log.warning("run_sql execution failed: %s", result.error)
        return {"error": str(result.error)}

    assert result.value is not None  # guarded by is_error check above
    exec_result: ExecutionResult = result.value
    columns = exec_result.columns or []
    all_rows = exec_result.rows or []
    total_rows = len(all_rows)
    sliced_rows = all_rows[:effective_limit]

    # Convert to list-of-dicts
    rows_as_dicts = [dict(zip(columns, row, strict=False)) for row in sliced_rows]

    # --- Quality metadata (best-effort) ---
    column_quality: dict[str, Any] | None = None
    quality_caveat: str | None = None
    if session is not None and table_ids:
        # Merge column_mappings: top-level (raw SQL mode) + per-step mappings
        merged_mappings: dict[str, str] = {}
        if column_mappings:
            merged_mappings.update(column_mappings)
        if raw_steps is not None:
            for s in raw_steps:
                mappings = s.get("column_mappings")
                if mappings:
                    merged_mappings.update(mappings)

        try:
            column_quality, quality_caveat = _build_column_quality(
                session, table_ids, columns, merged_mappings
            )
        except Exception:
            _log.debug("Quality metadata lookup failed", exc_info=True)

    # --- Build step execution info with snippet status ---
    step_info: list[dict[str, Any]] = []
    for sr in exec_result.step_results:
        entry: dict[str, Any] = {"step_id": sr.step_id, "sql": sr.sql_executed}
        match = snippet_matches.get(sr.step_id)
        if match:
            entry["snippet_status"] = match[0]
            entry["snippet_id"] = match[1]
        step_info.append(entry)

    from dataraum.mcp.formatters import format_run_sql_result

    return format_run_sql_result(
        columns=columns,
        rows=rows_as_dicts,
        limit=effective_limit,
        total_rows=total_rows,
        step_info=step_info,
        column_quality=column_quality,
        quality_caveat=quality_caveat,
        snippet_summary=snippet_summary,
    )


def _snippet_key_for_step(step: SQLStep, raw_steps: list[dict[str, Any]]) -> str:
    """Get the snippet standard_field key for a step.

    For structured steps, uses step_id directly.
    For raw SQL mode, uses a content-derived key to avoid collisions.
    """
    for rs in raw_steps:
        if rs.get("step_id") == step.step_id and "_snippet_key" in rs:
            return str(rs["_snippet_key"])
    return step.step_id


def _lookup_snippets(
    session: Session,
    source_id: str,
    sql_steps: list[SQLStep],
    raw_steps: list[dict[str, Any]],
) -> dict[str, tuple[str, str]]:
    """Look up snippets for each step before execution.

    Returns:
        Dict mapping step_id → (match_status, snippet_id).
        match_status is "exact_reuse" or "adapted".
    """
    from dataraum.query.snippet_library import SnippetLibrary

    library = SnippetLibrary(session)
    matches: dict[str, tuple[str, str]] = {}

    for step in sql_steps:
        try:
            key = _snippet_key_for_step(step, raw_steps)
            match = library.find_by_key(
                snippet_type="query",
                schema_mapping_id=source_id,
                standard_field=key,
            )
            if match:
                status = "exact_reuse" if match.snippet.sql == step.sql else "adapted"
                matches[step.step_id] = (status, match.snippet.snippet_id)
        except Exception:
            _log.debug("Snippet lookup failed for step %s", step.step_id, exc_info=True)

    return matches


def _save_snippets(
    session: Session,
    source_id: str,
    sql_steps: list[SQLStep],
    raw_steps: list[dict[str, Any]],
    snippet_matches: dict[str, tuple[str, str]],
    session_source: str,
) -> dict[str, Any]:
    """Save novel steps as snippets and return summary.

    Returns:
        Dict with reused, saved counts and session_source.
    """
    from dataraum.query.snippet_library import SnippetLibrary

    library = SnippetLibrary(session)
    reused = len(snippet_matches)
    saved = 0

    # Build column_mappings lookup from raw steps
    mappings_by_step: dict[str, dict[str, str]] = {}
    for s in raw_steps:
        cm = s.get("column_mappings")
        if cm:
            mappings_by_step[s["step_id"]] = cm

    for step in sql_steps:
        if step.step_id in snippet_matches:
            continue  # Already matched — don't re-save
        try:
            key = _snippet_key_for_step(step, raw_steps)
            library.save_snippet(
                snippet_type="query",
                sql=step.sql,
                description=step.description or f"MCP run_sql step: {step.step_id}",
                schema_mapping_id=source_id,
                source=session_source,
                standard_field=key,
                column_mappings=mappings_by_step.get(step.step_id),
            )
            saved += 1
        except Exception:
            _log.debug("Snippet save failed for step %s", step.step_id, exc_info=True)

    summary: dict[str, Any] = {
        "reused": reused,
        "saved": saved,
        "session_source": session_source,
    }
    return summary


def _build_column_quality(
    session: Session,
    table_ids: list[str],
    output_columns: list[str],
    column_mappings: dict[str, str],
) -> tuple[dict[str, Any | None], str | None]:
    """Look up quality metadata for output columns.

    Args:
        session: SQLAlchemy session.
        table_ids: Typed table IDs for this pipeline run.
        output_columns: Column names in the SQL result.
        column_mappings: Maps output col name → source col name (e.g. revenue → Betrag).

    Returns:
        (column_quality dict, caveat string or None)
    """
    from sqlalchemy import select

    from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.storage import Column as ColumnModel
    from dataraum.storage import Table

    # Build lookup: (table_name, column_name) → quality info
    # First load all quality reports for these tables
    tables = session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars().all()
    table_names = {t.table_id: t.table_name for t in tables}

    # Get all column IDs for these tables
    all_columns = (
        session.execute(select(ColumnModel).where(ColumnModel.table_id.in_(table_ids)))
        .scalars()
        .all()
    )

    # Map (table_name, column_name) → column_id (typed layer)
    col_id_map: dict[tuple[str, str], str] = {}
    for col in all_columns:
        tname = table_names.get(col.table_id, "")
        col_id_map[(tname, col.column_name)] = col.column_id

    # ColumnQualityReport.source_column_id points to slicing_view columns,
    # not typed columns. Resolve slicing_view tables and use their column IDs
    # for the quality report lookup (same pattern as ColumnQualityDetector).
    sv_col_ids: list[str] = []
    # Maps slicing_view column_id → (typed_table_name, column_name)
    sv_col_to_typed: dict[str, tuple[str, str]] = {}
    for tname in table_names.values():
        sv_table = session.execute(
            select(Table).where(
                Table.table_name == f"slicing_{tname}",
                Table.layer == "slicing_view",
            )
        ).scalar_one_or_none()
        if sv_table:
            sv_cols = (
                session.execute(
                    select(ColumnModel).where(ColumnModel.table_id == sv_table.table_id)
                )
                .scalars()
                .all()
            )
            for c in sv_cols:
                sv_col_ids.append(c.column_id)
                sv_col_to_typed[c.column_id] = (tname, c.column_name)

    # Load quality reports — prefer slicing_view column IDs, fall back to typed
    quality_by_col: dict[tuple[str, str], ColumnQualityReport] = {}
    lookup_ids = sv_col_ids if sv_col_ids else list(col_id_map.values())
    if lookup_ids:
        reports = (
            session.execute(
                select(ColumnQualityReport).where(
                    ColumnQualityReport.source_column_id.in_(lookup_ids)
                )
            )
            .scalars()
            .all()
        )
        for r in reports:
            if sv_col_ids:
                sv_key = sv_col_to_typed.get(r.source_column_id)
                if sv_key:
                    quality_by_col[sv_key] = r
            else:
                # Fallback: resolve typed column_id to (table_name, col_name)
                for (tn, cn), cid in col_id_map.items():
                    if cid == r.source_column_id:
                        quality_by_col[(tn, cn)] = r
                        break

    # Build readiness from entropy network context
    readiness_by_col: dict[str, str] = {}
    try:
        from dataraum.entropy.views.network_context import build_for_network
        from dataraum.entropy.views.query_context import network_to_column_summaries

        network_ctx = build_for_network(session, table_ids)
        if network_ctx and network_ctx.total_columns > 0:
            col_summaries = network_to_column_summaries(network_ctx)
            for key, summary in col_summaries.items():
                readiness_by_col[key] = summary.readiness
    except Exception:
        _log.debug("Entropy readiness lookup failed", exc_info=True)

    # Check if entropy data exists for these tables
    caveat: str | None = None
    try:
        has_entropy = session.execute(
            select(EntropyObjectRecord.object_id)
            .where(EntropyObjectRecord.table_id.in_(table_ids))
            .limit(1)
        ).scalar_one_or_none()
        if not has_entropy:
            caveat = "Quality metadata incomplete: entropy phase has not run yet"
    except Exception:
        pass

    # For each output column, resolve to source and look up quality
    column_quality: dict[str, Any | None] = {}
    for out_col in output_columns:
        # Resolve source column via mappings or direct match
        mapping = column_mappings.get(out_col, out_col)

        # Support qualified "table.column" in column_mappings
        if "." in mapping:
            qual_table, qual_col = mapping.split(".", 1)
            matches = [(qual_table, qual_col)] if col_id_map.get((qual_table, qual_col)) else []
        else:
            # Unqualified: collect all tables that have this column
            matches = [
                (tname, mapping)
                for tname in table_names.values()
                if col_id_map.get((tname, mapping)) is not None
            ]

        if len(matches) == 1:
            tname, source_col = matches[0]
            entry: dict[str, Any] = {"source_column": f"{tname}.{source_col}"}

            report = quality_by_col.get((tname, source_col))
            if report:
                entry["quality_grade"] = report.quality_grade
                entry["quality_score"] = round(report.overall_quality_score, 2)

            readiness_key = f"{tname}.{source_col}"
            if readiness_key in readiness_by_col:
                entry["readiness"] = readiness_by_col[readiness_key]

            column_quality[out_col] = entry
        elif len(matches) > 1:
            # Ambiguous — column exists in multiple tables, skip to avoid wrong metadata
            column_quality[out_col] = {
                "ambiguous": True,
                "candidates": [f"{t}.{c}" for t, c in matches],
            }
        else:
            column_quality[out_col] = None

    return column_quality, caveat
