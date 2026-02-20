"""MCP Server implementation for DataRaum.

Exposes high-level tools that call library functions directly (no HTTP).
Output directory is resolved from DATARAUM_OUTPUT_DIR env var or passed to create_server().
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from dataraum.mcp.formatters import (
    format_actions_report,
    format_context_for_llm,
    format_contract_evaluation,
    format_entropy_summary,
    format_query_result,
)


def create_server(output_dir: Path | None = None) -> Server:
    """Create and configure the MCP server with DataRaum tools.

    Args:
        output_dir: Pipeline output directory. If not provided, reads from
            DATARAUM_OUTPUT_DIR env var, defaulting to ./pipeline_output.
    """
    if output_dir is None:
        output_dir = Path(os.environ.get("DATARAUM_OUTPUT_DIR", "./pipeline_output"))

    server = Server("dataraum")

    @server.list_tools()  # type: ignore[no-untyped-call, untyped-decorator]
    async def list_tools() -> list[Tool]:
        """List available tools."""
        return [
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
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        """Execute a tool and return results."""
        if name == "get_context":
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


def _get_context(output_dir: Path) -> str:
    """Get formatted context document."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.graphs.context import build_execution_context, format_context_for_prompt
    from dataraum.storage import Source, Table

    manager = get_manager_for_directory(output_dir)

    try:
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

    manager = get_manager_for_directory(output_dir)

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

            interp_query = interp_query.order_by(EntropyInterpretationRecord.composite_score.desc())
            interp_result = session.execute(interp_query)
            interpretations = interp_result.scalars().all()

            return format_entropy_summary(source.name, snapshot, interpretations, table_name)
    finally:
        manager.close()


def _evaluate_contract(output_dir: Path, contract_name: str) -> str:
    """Evaluate a contract."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.entropy.analysis.aggregator import ColumnSummary, EntropyAggregator
    from dataraum.entropy.contracts import evaluate_contract, get_contract
    from dataraum.entropy.core.storage import EntropyRepository
    from dataraum.storage import Source, Table

    manager = get_manager_for_directory(output_dir)

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

            # Build column summaries
            repo = EntropyRepository(session)
            aggregator = EntropyAggregator()

            typed_table_ids = repo.get_typed_table_ids(table_ids)
            column_summaries: dict[str, ColumnSummary] = {}
            compound_risks: list[Any] = []

            if typed_table_ids:
                table_map, column_map = repo.get_table_column_mapping(typed_table_ids)
                entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

                if entropy_objects:
                    column_summaries, _ = aggregator.summarize_columns_by_table(
                        entropy_objects=entropy_objects,
                        table_map=table_map,
                        column_map=column_map,
                    )
                    for summary in column_summaries.values():
                        compound_risks.extend(summary.compound_risks)

            profile = get_contract(contract_name)
            if profile is None:
                return f"Error: Contract not found: {contract_name}"

            evaluation = evaluate_contract(column_summaries, contract_name, compound_risks)
            return format_contract_evaluation(evaluation, profile)
    finally:
        manager.close()


def _query(output_dir: Path, question: str, contract_name: str | None = None) -> str:
    """Execute a natural language query."""
    from sqlalchemy import select

    from dataraum.core.connections import get_manager_for_directory
    from dataraum.query import answer_question
    from dataraum.storage import Source

    manager = get_manager_for_directory(output_dir)

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
    from dataraum.entropy.analysis.aggregator import EntropyAggregator
    from dataraum.entropy.contracts import evaluate_all_contracts
    from dataraum.entropy.core.storage import EntropyRepository
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
    from dataraum.storage import Column, Source, Table

    manager = get_manager_for_directory(output_dir)

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

            # Get column summaries from entropy aggregator
            repo = EntropyRepository(session)
            aggregator = EntropyAggregator()

            typed_table_ids = repo.get_typed_table_ids(table_ids)
            column_summaries: dict[str, Any] = {}
            compound_risks: list[Any] = []

            if typed_table_ids:
                table_map, column_map = repo.get_table_column_mapping(typed_table_ids)
                entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

                if entropy_objects:
                    column_summaries, _ = aggregator.summarize_columns_by_table(
                        entropy_objects=entropy_objects,
                        table_map=table_map,
                        column_map=column_map,
                    )
                    for summary in column_summaries.values():
                        compound_risks.extend(summary.compound_risks)

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
            evaluations = evaluate_all_contracts(column_summaries, compound_risks)
            violation_dims: dict[str, list[str]] = {}
            for eval_result in evaluations.values():
                for v in eval_result.violations:
                    if v.dimension:
                        violation_dims.setdefault(v.dimension, []).extend(v.affected_columns)

            # Merge actions from all sources
            actions = merge_actions(
                column_summaries=column_summaries,
                interp_by_col=interp_by_col,
                entropy_objects_by_col=entropy_objects_by_col,
                violation_dims=violation_dims,
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
