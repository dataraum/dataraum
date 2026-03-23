"""Context builder for business cycle detection.

Assembles rich context from all available pipeline metadata:
slice definitions, statistical profiles, temporal profiles,
quality reports, enriched views, semantic annotations,
entity classifications, and confirmed relationships.

The LLM receives pre-computed signals and synthesizes them
into business cycle analysis — no exploration tools needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum.analysis.cycles.config import format_cycle_vocabulary_for_context
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.graph_topology import (
    analyze_graph_topology,
    format_graph_structure_for_context,
)
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.temporal.db_models import TemporalColumnProfile
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table

logger = get_logger(__name__)

if TYPE_CHECKING:
    import duckdb


def build_cycle_detection_context(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    *,
    vertical: str,
) -> dict[str, Any]:
    """Build context for the business cycle detection agent.

    Loads all available pipeline metadata and formats it for the LLM.
    The context is rich enough for single-call cycle detection without
    exploration tools.

    Args:
        session: SQLAlchemy session
        duckdb_conn: DuckDB connection for row counts
        table_ids: Tables to analyze
        vertical: Vertical name (e.g. 'finance')

    Returns:
        Context dictionary with all pipeline metadata for cycle detection.
    """
    context: dict[str, Any] = {}

    # 1. Tables + columns (with eager-loaded semantic annotations)
    tables_stmt = (
        select(Table)
        .where(Table.table_id.in_(table_ids))
        .options(selectinload(Table.columns).selectinload(Column.semantic_annotation))
    )
    tables = session.execute(tables_stmt).scalars().all()

    # Build lookup maps
    table_by_id = {t.table_id: t for t in tables}
    column_by_id: dict[str, Column] = {}
    for t in tables:
        for c in t.columns:
            column_by_id[c.column_id] = c

    # Row counts from DuckDB
    row_counts: dict[str, int | None] = {}
    for t in tables:
        try:
            result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{t.duckdb_path}"').fetchone()
            row_counts[t.table_name] = result[0] if result else None
        except Exception:
            logger.warning("row_count_failed", table=t.table_name, duckdb_path=t.duckdb_path)
            row_counts[t.table_name] = None

    # Build table info with columns and semantic annotations
    table_info = []
    for t in tables:
        columns = []
        for c in t.columns:
            col_info: dict[str, Any] = {
                "name": c.column_name,
                "type": c.resolved_type or c.raw_type,
            }
            if c.semantic_annotation:
                ann = c.semantic_annotation
                col_info["semantic_role"] = ann.semantic_role
                col_info["entity_type"] = ann.entity_type
                col_info["business_concept"] = ann.business_concept
                col_info["temporal_behavior"] = ann.temporal_behavior
                col_info["business_name"] = ann.business_name
                col_info["business_description"] = ann.business_description
            columns.append(col_info)

        table_info.append(
            {
                "table_id": t.table_id,
                "table_name": t.table_name,
                "row_count": row_counts.get(t.table_name),
                "columns": columns,
            }
        )

    context["tables"] = table_info

    # 2. Entity classifications (fact vs dimension)
    entities_stmt = (
        select(TableEntity, Table.table_name)
        .join(Table, TableEntity.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
    )
    entities = session.execute(entities_stmt).all()

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

    # 3. Relationships (LLM-confirmed only)
    rel_stmt = select(Relationship).where(
        Relationship.from_table_id.in_(table_ids),
        Relationship.to_table_id.in_(table_ids),
        Relationship.detection_method == "llm",
    )
    relationships = session.execute(rel_stmt).scalars().all()

    rel_list = []
    for rel in relationships:
        from_col = column_by_id.get(rel.from_column_id)
        to_col = column_by_id.get(rel.to_column_id)
        from_table = table_by_id.get(rel.from_table_id)
        to_table = table_by_id.get(rel.to_table_id)

        if from_col and to_col and from_table and to_table:
            rel_list.append(
                {
                    "from_table_id": rel.from_table_id,
                    "from_table": from_table.table_name,
                    "from_column": from_col.column_name,
                    "to_table_id": rel.to_table_id,
                    "to_table": to_table.table_name,
                    "to_column": to_col.column_name,
                    "relationship_type": rel.relationship_type,
                    "cardinality": rel.cardinality,
                    "confidence": rel.confidence,
                }
            )

    context["relationships"] = rel_list

    # 4. Graph topology
    table_name_map = {t.table_id: t.table_name for t in tables}
    graph_structure = analyze_graph_topology(
        table_ids=table_ids,
        relationships=rel_list,
        table_names=table_name_map,
    )
    context["graph_topology"] = graph_structure

    # 5. Slice definitions (pre-identified categorical dimensions = status columns)
    slice_stmt = (
        select(SliceDefinition)
        .where(SliceDefinition.table_id.in_(table_ids))
        .options(selectinload(SliceDefinition.table), selectinload(SliceDefinition.column))
        .order_by(SliceDefinition.slice_priority)
    )
    slices = session.execute(slice_stmt).scalars().all()

    slice_list = []
    for sd in slices:
        # Get value counts from statistical profile if available
        value_counts = _get_value_counts_for_column(session, sd.column_id)

        slice_list.append(
            {
                "table_name": sd.table.table_name,
                "column_name": sd.column.column_name,
                "slice_type": sd.slice_type,
                "values": sd.distinct_values or [],
                "value_counts": value_counts,
                "confidence": sd.confidence,
                "business_context": sd.business_context,
                "priority": sd.slice_priority,
            }
        )

    context["slice_definitions"] = slice_list

    # 6. Temporal profiles
    temporal_stmt = (
        select(TemporalColumnProfile, Column.column_name, Table.table_name)
        .join(Column, TemporalColumnProfile.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
    )
    temporal_results = session.execute(temporal_stmt).all()

    context["temporal_profiles"] = [
        {
            "table_name": table_name,
            "column_name": col_name,
            "granularity": tp.detected_granularity,
            "date_range_start": str(tp.min_timestamp) if tp.min_timestamp else None,
            "date_range_end": str(tp.max_timestamp) if tp.max_timestamp else None,
            "completeness": tp.completeness_ratio,
            "is_stale": tp.is_stale,
        }
        for tp, col_name, table_name in temporal_results
    ]

    # 7. Quality signals — retired with quality_summary phase (v0.2).
    # BBN readiness signals will replace this in future.
    quality_signals: list[dict[str, object]] = []

    context["quality_signals"] = quality_signals

    # 8. Enriched views (pre-joined table schemas)
    enriched_stmt = select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
    enriched_views = session.execute(enriched_stmt).scalars().all()

    enriched_list = []
    for ev in enriched_views:
        fact_table = table_by_id.get(ev.fact_table_id)
        dim_tables = [
            table_by_id[tid].table_name
            for tid in (ev.dimension_table_ids or [])
            if tid in table_by_id
        ]
        enriched_list.append(
            {
                "view_name": ev.view_name,
                "fact_table": fact_table.table_name if fact_table else "unknown",
                "dimension_tables": dim_tables,
                "dimension_columns": ev.dimension_columns or [],
            }
        )

    context["enriched_views"] = enriched_list

    # 9. Summary statistics
    context["summary"] = {
        "total_tables": len(tables),
        "total_columns": sum(len(t.columns) for t in tables),
        "total_relationships": len(rel_list),
        "slice_dimensions_found": len(slice_list),
        "temporal_columns": len(context["temporal_profiles"]),
        "quality_issues": len(quality_signals),
        "enriched_views": len(enriched_list),
        "fact_tables": sum(1 for e in context["entity_classifications"] if e["is_fact_table"]),
        "dimension_tables": sum(
            1 for e in context["entity_classifications"] if e["is_dimension_table"]
        ),
        "graph_pattern": graph_structure.pattern,
    }

    # 10. Domain vocabulary
    vocabulary = format_cycle_vocabulary_for_context(vertical=vertical)
    context["domain_vocabulary"] = vocabulary

    return context


def _get_value_counts_for_column(
    session: Session,
    column_id: str,
) -> list[dict[str, Any]]:
    """Get value counts from statistical profile for a column.

    Args:
        session: SQLAlchemy session
        column_id: Column to look up

    Returns:
        List of {value, count, percentage} dicts, or empty list.
    """
    profile_stmt = select(StatisticalProfile).where(
        StatisticalProfile.column_id == column_id,
        StatisticalProfile.layer == "typed",
    )
    profile = session.execute(profile_stmt).scalars().first()

    if not profile or not profile.profile_data:
        return []

    top_values = profile.profile_data.get("top_values", [])
    return [
        {
            "value": tv.get("value", ""),
            "count": tv.get("count", 0),
            "percentage": round(tv.get("percentage", 0), 1),
        }
        for tv in top_values[:10]
    ]


def format_context_for_prompt(context: dict[str, Any]) -> str:
    """Format the context dictionary as a readable string for the LLM prompt.

    Organizes metadata into sections that support cycle detection:
    1. Domain vocabulary (reference framework)
    2. Dataset summary
    3. Pre-identified categorical dimensions (= cycle indicators)
    4. Enriched views (pre-joined tables)
    5. Relationships
    6. Temporal patterns
    7. Quality signals
    8. Column semantics by table

    Args:
        context: Context dictionary from build_cycle_detection_context

    Returns:
        Formatted string suitable for LLM prompt
    """
    lines: list[str] = []

    # Domain vocabulary first (provides reference framework)
    vocabulary = context.get("domain_vocabulary", "")
    if vocabulary:
        lines.append("# DOMAIN KNOWLEDGE")
        lines.append("")
        lines.append(vocabulary)
        lines.append("")
        lines.append("---")
        lines.append("")

    # Dataset summary
    lines.append("# DATASET CONTEXT")
    lines.append("")
    summary = context.get("summary", {})
    lines.append("## SUMMARY")
    lines.append(f"- Tables: {summary.get('total_tables', 0)}")
    lines.append(f"- Columns: {summary.get('total_columns', 0)}")
    lines.append(f"- Confirmed relationships: {summary.get('total_relationships', 0)}")
    lines.append(f"- Fact tables: {summary.get('fact_tables', 0)}")
    lines.append(f"- Dimension tables: {summary.get('dimension_tables', 0)}")
    lines.append(
        f"- Categorical dimensions (status/type columns): {summary.get('slice_dimensions_found', 0)}"
    )
    lines.append(f"- Temporal columns: {summary.get('temporal_columns', 0)}")
    lines.append(f"- Graph pattern: {summary.get('graph_pattern', 'unknown')}")
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
        table_info = context["tables"]
        row_count = next(
            (t["row_count"] for t in table_info if t["table_name"] == ent["table_name"]),
            None,
        )
        row_str = f", {row_count:,} rows" if row_count else ""
        grain = f", grain: {', '.join(ent['grain_columns'])}" if ent.get("grain_columns") else ""
        lines.append(f"- {ent['table_name']} ({table_type}{row_str}{grain}): {ent['entity_type']}")
        if ent.get("description"):
            lines.append(f"  {ent['description'][:500]}")
    lines.append("")

    # Pre-identified categorical dimensions (= cycle indicators)
    slice_defs = context.get("slice_definitions", [])
    if slice_defs:
        lines.append("## CATEGORICAL DIMENSIONS (Pre-Identified Cycle Indicators)")
        lines.append("")
        lines.append("These columns were identified by the semantic agent as key categorical")
        lines.append("dimensions. Status columns are strong cycle completion indicators.")
        lines.append("")
        for sd in slice_defs:
            lines.append(
                f"### {sd['table_name']}.{sd['column_name']} (confidence: {sd['confidence']:.0%})"
            )
            if sd.get("business_context"):
                lines.append(f"  Context: {sd['business_context'][:500]}")

            # Show values with counts if available
            value_counts = sd.get("value_counts", [])
            if value_counts:
                total = sum(vc["count"] for vc in value_counts)
                values_str = ", ".join(
                    f"{vc['value']} ({vc['count']:,}, {vc['percentage']}%)" for vc in value_counts
                )
                lines.append(f"  Values ({total:,} total): {values_str}")
            elif sd.get("values"):
                lines.append(f"  Values: {', '.join(sd['values'])}")
            lines.append("")

    # Enriched views
    enriched = context.get("enriched_views", [])
    if enriched:
        lines.append("## ENRICHED VIEWS (Pre-Joined Tables)")
        lines.append("")
        lines.append("These DuckDB views join fact tables with their dimension tables.")
        lines.append("They represent confirmed business relationships.")
        lines.append("")
        for ev in enriched:
            dims = ", ".join(ev["dimension_tables"]) if ev["dimension_tables"] else "none"
            lines.append(f"- {ev['view_name']}: {ev['fact_table']} + [{dims}]")
            if ev.get("dimension_columns"):
                cols = ", ".join(ev["dimension_columns"][:8])
                extra = (
                    f" (+{len(ev['dimension_columns']) - 8} more)"
                    if len(ev["dimension_columns"]) > 8
                    else ""
                )
                lines.append(f"  Added columns: {cols}{extra}")
        lines.append("")

    # Relationships
    lines.append("## CONFIRMED RELATIONSHIPS")
    for rel in context.get("relationships", []):
        lines.append(
            f"- {rel['from_table']}.{rel['from_column']} → "
            f"{rel['to_table']}.{rel['to_column']} "
            f"({rel['relationship_type']}, {rel['cardinality']}, conf={rel['confidence']:.0%})"
        )
    lines.append("")

    # Graph topology
    graph_topology = context.get("graph_topology")
    if graph_topology:
        lines.append(format_graph_structure_for_context(graph_topology))
        lines.append("")

    # Temporal patterns
    temporal = context.get("temporal_profiles", [])
    if temporal:
        lines.append("## TEMPORAL PATTERNS")
        for tp in temporal:
            stale_str = " [STALE]" if tp.get("is_stale") else ""
            lines.append(
                f"- {tp['table_name']}.{tp['column_name']}: "
                f"{tp['granularity']}, "
                f"{tp['date_range_start']} to {tp['date_range_end']}, "
                f"completeness={tp['completeness']:.0%}{stale_str}"
            )
        lines.append("")

    # Quality signals (only issues)
    quality = context.get("quality_signals", [])
    if quality:
        lines.append("## DATA QUALITY SIGNALS")
        for qs in quality:
            lines.append(
                f"- {qs['table_name']}.{qs['column_name']}: grade {qs['quality_grade']} ({qs['quality_score']:.2f})"
            )
            lines.append(f"  {qs['summary'][:500]}")
            for finding in qs.get("key_findings", [])[:2]:
                lines.append(f"  - {finding[:500]}")
        lines.append("")

    # Column semantics by table
    lines.append("## COLUMN SEMANTICS BY TABLE")
    for table in context.get("tables", []):
        lines.append(f"\n### {table['table_name']}")
        for col in table["columns"]:
            parts = [col["name"]]
            if col.get("semantic_role"):
                parts.append(f"role={col['semantic_role']}")
            if col.get("business_concept"):
                parts.append(f"concept={col['business_concept']}")
            if col.get("entity_type"):
                parts.append(f"entity={col['entity_type']}")
            lines.append(f"  - {', '.join(parts)}")
            if col.get("business_description"):
                lines.append(f"    {col['business_description'][:500]}")

    return "\n".join(lines)
