"""Schema resolver for validation checks.

Provides table schema with semantic annotations for LLM context.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dataraum_context.analysis.semantic.db_models import SemanticAnnotation
from dataraum_context.storage import Column, Table

logger = logging.getLogger(__name__)


async def get_table_schema_for_llm(
    session: AsyncSession,
    table_id: str,
) -> dict[str, Any]:
    """Get table schema with semantic annotations for LLM context.

    The LLM uses this schema to understand the table structure and
    identify relevant columns for validation checks.

    Args:
        session: Database session
        table_id: Table ID

    Returns:
        Dict with table info and column details including semantic annotations
    """
    table_query = (
        select(Table)
        .where(Table.table_id == table_id)
        .options(selectinload(Table.columns).selectinload(Column.semantic_annotation))
    )
    table_result = await session.execute(table_query)
    table = table_result.scalar_one_or_none()

    if not table:
        return {"error": f"Table {table_id} not found"}

    if not table.duckdb_path:
        return {"error": f"Table {table_id} has no DuckDB path"}

    columns = []
    for col in table.columns:
        col_info: dict[str, Any] = {
            "column_name": col.column_name,
            "data_type": col.resolved_type or col.raw_type,
        }

        if col.semantic_annotation:
            ann = col.semantic_annotation
            col_info["semantic"] = {
                "role": ann.semantic_role,
                "entity_type": ann.entity_type,
                "business_name": ann.business_name,
                "domain": ann.business_domain,
            }

        columns.append(col_info)

    return {
        "table_name": table.table_name,
        "table_id": table.table_id,
        "duckdb_path": table.duckdb_path,
        "columns": columns,
    }


def format_schema_for_prompt(schema: dict[str, Any]) -> str:
    """Format schema dict as text for LLM prompt.

    Args:
        schema: Schema dict from get_table_schema_for_llm

    Returns:
        Formatted string for prompt context
    """
    if "error" in schema:
        return f"Error: {schema['error']}"

    lines = [
        f"Table: {schema['table_name']}",
        f"DuckDB Path: {schema['duckdb_path']}",
        "",
        "Columns:",
    ]

    for col in schema.get("columns", []):
        col_line = f"  - {col['column_name']} ({col.get('data_type', 'unknown')})"

        if "semantic" in col:
            sem = col["semantic"]
            annotations = []
            if sem.get("entity_type"):
                annotations.append(f"entity: {sem['entity_type']}")
            if sem.get("role"):
                annotations.append(f"role: {sem['role']}")
            if sem.get("business_name"):
                annotations.append(f"business_name: {sem['business_name']}")
            if sem.get("domain"):
                annotations.append(f"domain: {sem['domain']}")
            if annotations:
                col_line += f" [{', '.join(annotations)}]"

        lines.append(col_line)

    return "\n".join(lines)


# Keep SemanticAnnotation import visible for type checking
__all__ = [
    "get_table_schema_for_llm",
    "format_schema_for_prompt",
    "SemanticAnnotation",
]
