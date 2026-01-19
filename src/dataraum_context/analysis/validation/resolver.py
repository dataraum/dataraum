"""Schema resolver for validation checks.

Provides table schemas with semantic annotations and relationships for LLM context.
Supports multi-table validation by fetching all related tables at once.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum_context.storage import Column, Table

logger = logging.getLogger(__name__)


def get_multi_table_schema_for_llm(
    session: Session,
    table_ids: list[str],
) -> dict[str, Any]:
    """Get schemas for multiple tables with semantic annotations and relationships.

    This is the primary function for multi-table validation. It fetches all
    table schemas along with detected relationships between them.

    Args:
        session: Database session
        table_ids: List of table IDs to include

    Returns:
        Dict with:
        - tables: List of table schemas
        - relationships: List of detected relationships between tables
    """
    from dataraum_context.analysis.relationships.db_models import Relationship

    if not table_ids:
        return {"error": "No tables found"}

    # Fetch all tables with their columns and annotations
    table_query = (
        select(Table)
        .where(Table.table_id.in_(table_ids))
        .options(selectinload(Table.columns).selectinload(Column.semantic_annotation))
    )
    table_result = session.execute(table_query)
    tables = table_result.scalars().all()

    if not tables:
        return {"error": "No tables found"}

    # Build table schemas
    table_schemas = []
    table_id_to_name = {}
    column_id_to_info = {}

    for table in tables:
        if not table.duckdb_path:
            continue

        schema = _format_table_schema(table)
        table_schemas.append(schema)
        table_id_to_name[table.table_id] = table.table_name

        # Build column lookup for relationship formatting
        for col in table.columns:
            column_id_to_info[col.column_id] = {
                "table_name": table.table_name,
                "column_name": col.column_name,
            }

    if not table_schemas:
        return {"error": "No tables with DuckDB paths found"}

    # Fetch relationships between these tables
    rel_query = select(Relationship).where(
        Relationship.from_table_id.in_(table_ids),
        Relationship.to_table_id.in_(table_ids),
    )
    rel_result = session.execute(rel_query)
    relationships = rel_result.scalars().all()

    # Format relationships
    formatted_rels = []
    for rel in relationships:
        from_info = column_id_to_info.get(rel.from_column_id, {})
        to_info = column_id_to_info.get(rel.to_column_id, {})

        if from_info and to_info:
            formatted_rels.append(
                {
                    "from_table": from_info.get("table_name"),
                    "from_column": from_info.get("column_name"),
                    "to_table": to_info.get("table_name"),
                    "to_column": to_info.get("column_name"),
                    "relationship_type": rel.relationship_type,
                    "cardinality": rel.cardinality,
                    "confidence": rel.confidence,
                }
            )

    return {
        "tables": table_schemas,
        "relationships": formatted_rels,
    }


def _format_table_schema(table: Table) -> dict[str, Any]:
    """Format a single table's schema.

    Args:
        table: Table ORM object with columns loaded

    Returns:
        Dict with table info and columns
    """
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
            }

        columns.append(col_info)

    return {
        "table_name": table.table_name,
        "table_id": table.table_id,
        "duckdb_path": table.duckdb_path,
        "columns": columns,
    }


def format_multi_table_schema_for_prompt(schema: dict[str, Any]) -> str:
    """Format multi-table schema dict as text for LLM prompt.

    Args:
        schema: Schema dict from get_multi_table_schema_for_llm

    Returns:
        Formatted string for prompt context
    """
    if "error" in schema:
        return f"Error: {schema['error']}"

    lines = ["## Available Tables\n"]

    for table in schema.get("tables", []):
        lines.append(f"### {table['table_name']}")
        lines.append(f"DuckDB Path: {table['duckdb_path']}")
        lines.append("Columns:")

        for col in table.get("columns", []):
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
                if annotations:
                    col_line += f" [{', '.join(annotations)}]"

            lines.append(col_line)

        lines.append("")

    # Add relationships section
    relationships = schema.get("relationships", [])
    if relationships:
        lines.append("## Detected Relationships\n")
        for rel in relationships:
            lines.append(
                f"- {rel['from_table']}.{rel['from_column']} → "
                f"{rel['to_table']}.{rel['to_column']} "
                f"({rel['relationship_type']}, {rel['cardinality']}, "
                f"confidence: {rel['confidence']:.0%})"
            )
        lines.append("")

    return "\n".join(lines)


__all__ = [
    "get_multi_table_schema_for_llm",
    "format_multi_table_schema_for_prompt",
]
