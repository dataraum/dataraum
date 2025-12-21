"""Context builder for business cycle detection.

Prepares the upfront context given to the LLM agent,
extracted from existing metadata in the database.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.cycles.config import format_cycle_vocabulary_for_context

if TYPE_CHECKING:
    import duckdb


async def build_cycle_detection_context(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    *,
    domain: str | None = None,
) -> dict[str, Any]:
    """Build context for the business cycle detection agent.

    Extracts relevant metadata from the database and formats it
    for the LLM agent prompt.

    Args:
        session: SQLAlchemy async session
        duckdb_conn: DuckDB connection for data queries
        table_ids: Tables to analyze
        domain: Optional domain name for domain-specific vocabulary
                (e.g., "financial", "retail", "manufacturing")

    Returns:
        Context dictionary with:
        - dataset_overview: Tables, row counts, relationships
        - semantic_annotations: Column-level semantic metadata
        - entity_classifications: Fact/dimension table types
        - status_columns: Detected status/state columns
        - relationship_graph: Column-to-column relationships
        - domain_vocabulary: Cycle type definitions and hints (if available)
    """
    from dataraum_context.analysis.relationships.db_models import Relationship
    from dataraum_context.analysis.semantic.db_models import (
        SemanticAnnotation,
        TableEntity,
    )
    from dataraum_context.storage import Column, Table

    context: dict[str, Any] = {}

    # 1. Dataset Overview
    tables_stmt = select(Table).where(Table.table_id.in_(table_ids))
    tables = (await session.execute(tables_stmt)).scalars().all()

    table_info = []
    for t in tables:
        # Get row count from DuckDB
        try:
            duckdb_table_name = f"typed_{t.table_name}"
            result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{duckdb_table_name}"').fetchone()
            row_count = result[0] if result else None
        except Exception:
            row_count = None

        # Get columns
        cols_stmt = select(Column).where(Column.table_id == t.table_id)
        columns = (await session.execute(cols_stmt)).scalars().all()

        table_info.append(
            {
                "table_id": t.table_id,
                "table_name": t.table_name,
                "row_count": row_count,
                "column_count": len(columns),
                "columns": [{"name": c.column_name, "type": c.resolved_type} for c in columns],
            }
        )

    context["dataset_overview"] = {
        "table_count": len(tables),
        "tables": table_info,
    }

    # 2. Semantic Annotations (the rich metadata from Phase 5)
    annotations_stmt = (
        select(SemanticAnnotation, Column.column_name, Table.table_name)
        .join(Column, SemanticAnnotation.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
    )
    annotations = (await session.execute(annotations_stmt)).all()

    semantic_by_table: dict[str, list[dict[str, Any]]] = {}
    status_columns: list[dict[str, Any]] = []

    for ann, col_name, table_name in annotations:
        if table_name not in semantic_by_table:
            semantic_by_table[table_name] = []

        col_semantic = {
            "column_name": col_name,
            "semantic_role": ann.semantic_role,
            "entity_type": ann.entity_type,
            "business_name": ann.business_name,
            "business_description": ann.business_description,
        }
        semantic_by_table[table_name].append(col_semantic)

        # Detect status/state columns by entity_type
        if ann.entity_type and any(
            indicator in (ann.entity_type or "").lower()
            for indicator in ["status", "state", "paid", "cleared", "flag"]
        ):
            # Get distinct values for status columns
            try:
                duckdb_table_name = f"typed_{table_name}"
                values = duckdb_conn.execute(
                    f'SELECT DISTINCT "{col_name}", COUNT(*) as cnt '
                    f'FROM "{duckdb_table_name}" '
                    f'WHERE "{col_name}" IS NOT NULL '
                    f'GROUP BY "{col_name}" '
                    f"ORDER BY cnt DESC LIMIT 10"
                ).fetchall()
                distinct_values = [{"value": v[0], "count": v[1]} for v in values]
            except Exception:
                distinct_values = []

            status_columns.append(
                {
                    "table_name": table_name,
                    "column_name": col_name,
                    "entity_type": ann.entity_type,
                    "business_description": ann.business_description,
                    "distinct_values": distinct_values,
                }
            )

    context["semantic_annotations"] = semantic_by_table
    context["status_columns"] = status_columns

    # 3. Entity Classifications (fact vs dimension)
    entities_stmt = (
        select(TableEntity, Table.table_name)
        .join(Table, TableEntity.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
    )
    entities = (await session.execute(entities_stmt)).all()

    context["entity_classifications"] = [
        {
            "table_name": table_name,
            "entity_type": ent.detected_entity_type,
            "description": ent.description,
            "is_fact_table": ent.is_fact_table,
            "is_dimension_table": ent.is_dimension_table,
            "grain_columns": ent.grain_columns,
        }
        for ent, table_name in entities
    ]

    # 4. Relationship Graph
    relationships_stmt = (
        select(
            Relationship,
            Table.table_name.label("from_table"),
            Column.column_name.label("from_column"),
        )
        .join(Table, Relationship.from_table_id == Table.table_id)
        .join(Column, Relationship.from_column_id == Column.column_id)
        .where(Relationship.from_table_id.in_(table_ids))
    )
    relationships = (await session.execute(relationships_stmt)).all()

    # Get to-side info
    rel_list = []
    for rel, from_table, from_col in relationships:
        # Get to-table and to-column names
        to_table_stmt = select(Table.table_name).where(Table.table_id == rel.to_table_id)
        to_col_stmt = select(Column.column_name).where(Column.column_id == rel.to_column_id)
        to_table = (await session.execute(to_table_stmt)).scalar()
        to_col = (await session.execute(to_col_stmt)).scalar()

        if rel.detection_method == "llm" or rel.confidence > 0.7:
            rel_list.append(
                {
                    "from_table": from_table,
                    "from_column": from_col,
                    "to_table": to_table,
                    "to_column": to_col,
                    "relationship_type": rel.relationship_type,
                    "cardinality": rel.cardinality,
                    "confidence": rel.confidence,
                    "detection_method": rel.detection_method,
                }
            )

    context["relationships"] = rel_list

    # 5. Summary statistics
    context["summary"] = {
        "total_tables": len(tables),
        "total_columns": sum(len(cols) for cols in semantic_by_table.values()),
        "total_relationships": len(rel_list),
        "status_columns_found": len(status_columns),
        "fact_tables": sum(1 for e in context["entity_classifications"] if e["is_fact_table"]),
        "dimension_tables": sum(
            1 for e in context["entity_classifications"] if e["is_dimension_table"]
        ),
    }

    # 6. Domain vocabulary (cycle types, completion indicators, hints)
    vocabulary = format_cycle_vocabulary_for_context(domain)
    context["domain_vocabulary"] = vocabulary
    context["domain"] = domain

    return context


def format_context_for_prompt(context: dict[str, Any]) -> str:
    """Format the context dictionary as a readable string for the LLM prompt.

    Args:
        context: Context dictionary from build_cycle_detection_context

    Returns:
        Formatted string suitable for LLM prompt
    """
    lines = []

    # Domain vocabulary first (provides reference framework)
    vocabulary = context.get("domain_vocabulary", "")
    if vocabulary:
        lines.append("# DOMAIN KNOWLEDGE")
        lines.append("")
        lines.append(vocabulary)
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("# DATASET CONTEXT")
        lines.append("")

    # Summary
    summary = context.get("summary", {})
    lines.append("## DATASET SUMMARY")
    lines.append(f"- Tables: {summary.get('total_tables', 0)}")
    lines.append(f"- Columns: {summary.get('total_columns', 0)}")
    lines.append(f"- Relationships: {summary.get('total_relationships', 0)}")
    lines.append(f"- Fact tables: {summary.get('fact_tables', 0)}")
    lines.append(f"- Dimension tables: {summary.get('dimension_tables', 0)}")
    lines.append(f"- Status/state columns detected: {summary.get('status_columns_found', 0)}")
    lines.append("")

    # Entity classifications
    lines.append("## TABLE CLASSIFICATIONS")
    for ent in context.get("entity_classifications", []):
        table_type = (
            "FACT"
            if ent["is_fact_table"]
            else "DIMENSION"
            if ent["is_dimension_table"]
            else "OTHER"
        )
        lines.append(f"- {ent['table_name']} ({table_type}): {ent['entity_type']}")
        if ent.get("description"):
            lines.append(f"  Description: {ent['description'][:200]}")
    lines.append("")

    # Relationships
    lines.append("## RELATIONSHIPS")
    for rel in context.get("relationships", []):
        lines.append(
            f"- {rel['from_table']}.{rel['from_column']} → "
            f"{rel['to_table']}.{rel['to_column']} "
            f"({rel['relationship_type']}, {rel['cardinality']}, conf={rel['confidence']:.2f})"
        )
    lines.append("")

    # Status columns (key for cycle detection!)
    lines.append("## STATUS/STATE COLUMNS (Cycle Indicators)")
    for sc in context.get("status_columns", []):
        lines.append(f"- {sc['table_name']}.{sc['column_name']}")
        lines.append(f"  Entity type: {sc['entity_type']}")
        if sc.get("business_description"):
            lines.append(f"  Description: {sc['business_description']}")
        if sc.get("distinct_values"):
            values_str = ", ".join(
                f"{v['value']} ({v['count']:,})" for v in sc["distinct_values"][:5]
            )
            lines.append(f"  Values: {values_str}")
    lines.append("")

    # Semantic annotations by table
    lines.append("## COLUMN SEMANTICS BY TABLE")
    for table_name, columns in context.get("semantic_annotations", {}).items():
        lines.append(f"\n### {table_name}")
        for col in columns:
            role = col.get("semantic_role", "?")
            entity = col.get("entity_type", "?")
            lines.append(f"  - {col['column_name']}: role={role}, entity={entity}")
            if col.get("business_description"):
                desc = col["business_description"][:100]
                lines.append(f"    {desc}")

    return "\n".join(lines)
