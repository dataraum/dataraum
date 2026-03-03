"""Schema resolver for validation checks.

Provides table schemas with semantic annotations and relationships for LLM context.
Supports multi-table validation by fetching all related tables at once.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    import duckdb

logger = get_logger(__name__)


def get_multi_table_schema_for_llm(
    session: Session,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, Any]:
    """Get schemas for multiple tables with semantic annotations and relationships.

    This is the primary function for multi-table validation. It fetches all
    table schemas along with detected relationships between them.

    Args:
        session: Database session
        table_ids: List of table IDs to include
        duckdb_conn: Optional DuckDB connection for row counts

    Returns:
        Dict with:
        - tables: List of table schemas (with row counts if duckdb_conn provided)
        - relationships: List of LLM-confirmed relationships between tables
        - enriched_views: List of available pre-joined views
    """
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

        row_count = None
        if duckdb_conn:
            try:
                result = duckdb_conn.execute(
                    f'SELECT COUNT(*) FROM "{table.duckdb_path}"'
                ).fetchone()
                row_count = result[0] if result else None
            except Exception:
                logger.warning(
                    "row_count_failed",
                    table=table.table_name,
                    duckdb_path=table.duckdb_path,
                )

        schema = _format_table_schema(table, row_count=row_count)
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

    # Fetch LLM-confirmed relationships between these tables
    rel_query = select(Relationship).where(
        Relationship.from_table_id.in_(table_ids),
        Relationship.to_table_id.in_(table_ids),
        Relationship.detection_method == "llm",
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

    # Fetch slice definitions (categorical value distributions) for these tables
    slice_stmt = select(SliceDefinition).where(
        SliceDefinition.table_id.in_(table_ids),
    )
    slices = session.execute(slice_stmt).scalars().all()

    # Build column_id → distinct_values lookup
    column_slices: dict[str, list[str]] = {}
    for sl in slices:
        if sl.distinct_values:
            column_slices[sl.column_id] = sl.distinct_values

    # Attach slice values to table schemas
    for table in tables:
        table_schema = next((s for s in table_schemas if s["table_id"] == table.table_id), None)
        if not table_schema:
            continue
        for col in table.columns:
            if col.column_id in column_slices:
                col_schema = next(
                    (c for c in table_schema["columns"] if c["column_name"] == col.column_name),
                    None,
                )
                if col_schema:
                    col_schema["distinct_values"] = column_slices[col.column_id]

    # Fetch enriched views for these tables
    enriched_stmt = select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
    enriched_views = session.execute(enriched_stmt).scalars().all()

    formatted_views = []
    for ev in enriched_views:
        fact_name = table_id_to_name.get(ev.fact_table_id, "unknown")
        dim_names = [
            table_id_to_name[tid]
            for tid in (ev.dimension_table_ids or [])
            if tid in table_id_to_name
        ]
        formatted_views.append(
            {
                "view_name": ev.view_name,
                "duckdb_path": ev.view_name,
                "fact_table": fact_name,
                "dimension_tables": dim_names,
                "dimension_columns": ev.dimension_columns or [],
            }
        )

    return {
        "tables": table_schemas,
        "relationships": formatted_rels,
        "enriched_views": formatted_views,
    }


def _format_table_schema(table: Table, *, row_count: int | None = None) -> dict[str, Any]:
    """Format a single table's schema.

    Args:
        table: Table ORM object with columns loaded
        row_count: Optional row count from DuckDB

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
                "business_concept": ann.business_concept,
                "temporal_behavior": ann.temporal_behavior,
                "business_description": ann.business_description,
            }

        columns.append(col_info)

    result: dict[str, Any] = {
        "table_name": table.table_name,
        "table_id": table.table_id,
        "duckdb_path": table.duckdb_path,
        "columns": columns,
    }
    if row_count is not None:
        result["row_count"] = row_count
    return result


def format_multi_table_schema_for_prompt(schema: dict[str, Any]) -> str:
    """Format multi-table schema dict as text for LLM prompt.

    Uses XML format and emphasizes exact column names with quoting examples.

    Args:
        schema: Schema dict from get_multi_table_schema_for_llm

    Returns:
        Formatted string for prompt context
    """
    if "error" in schema:
        return f"<error>{schema['error']}</error>"

    lines = ["<tables>"]

    for table in schema.get("tables", []):
        row_count_attr = f' row_count="{table["row_count"]}"' if table.get("row_count") else ""
        lines.append(
            f'<table name="{table["table_name"]}" duckdb_path="{table["duckdb_path"]}"{row_count_attr}>'
        )
        lines.append("<columns>")

        for col in table.get("columns", []):
            col_name = col["column_name"]
            data_type = col.get("data_type", "unknown")

            # Show how to reference this column in SQL
            sql_ref = f'"{col_name}"'

            col_line = f'  <column name="{col_name}" type="{data_type}" sql_reference={sql_ref}'

            if "semantic" in col:
                sem = col["semantic"]
                if sem.get("role"):
                    col_line += f' role="{sem["role"]}"'
                if sem.get("entity_type"):
                    col_line += f' entity="{sem["entity_type"]}"'
                if sem.get("business_name"):
                    col_line += f' business_name="{sem["business_name"]}"'
                if sem.get("business_concept"):
                    col_line += f' business_concept="{sem["business_concept"]}"'
                if sem.get("temporal_behavior"):
                    col_line += f' temporal_behavior="{sem["temporal_behavior"]}"'
                if sem.get("business_description"):
                    desc = sem["business_description"][:120]
                    col_line += f' description="{desc}"'

            # Distinct values from slicing phase (categorical columns)
            if col.get("distinct_values"):
                vals = ", ".join(col["distinct_values"])
                col_line += f' distinct_values="{vals}"'

            col_line += " />"
            lines.append(col_line)

        lines.append("</columns>")
        lines.append("</table>")
        lines.append("")

    lines.append("</tables>")

    # Add relationships section
    relationships = schema.get("relationships", [])
    if relationships:
        lines.append("")
        lines.append("<relationships>")
        for rel in relationships:
            lines.append(
                f'<relationship from_table="{rel["from_table"]}" from_column="{rel["from_column"]}" '
                f'to_table="{rel["to_table"]}" to_column="{rel["to_column"]}" '
                f'type="{rel["relationship_type"]}" cardinality="{rel["cardinality"]}" '
                f'confidence="{rel["confidence"]:.0%}" />'
            )
        lines.append("</relationships>")

    # Add enriched views section
    enriched_views = schema.get("enriched_views", [])
    if enriched_views:
        lines.append("")
        lines.append("<enriched_views>")
        lines.append("<!-- Pre-joined views available as alternative to manual JOINs -->")
        for ev in enriched_views:
            dims = ", ".join(ev["dimension_tables"]) if ev.get("dimension_tables") else ""
            lines.append(
                f'<view name="{ev["view_name"]}" duckdb_path="{ev["duckdb_path"]}" '
                f'fact_table="{ev["fact_table"]}" dimension_tables="{dims}" />'
            )
        lines.append("</enriched_views>")

    # Add usage note
    lines.append("")
    lines.append("<sql_usage_note>")
    lines.append("IMPORTANT: Use the sql_reference attribute when writing SQL.")
    lines.append('Column names with spaces MUST be quoted: "Transaction date" not transaction_date')
    lines.append("Use the duckdb_path for table references in FROM/JOIN clauses.")
    lines.append("</sql_usage_note>")

    return "\n".join(lines)


__all__ = [
    "get_multi_table_schema_for_llm",
    "format_multi_table_schema_for_prompt",
]
