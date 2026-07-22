"""Schema resolver for validation checks.

Provides table schemas with semantic annotations and relationships for LLM context.
Supports multi-table validation by fetching all related tables at once.

The resolver is an **in-run reader** (ADR-0008): it never resolves snapshot
heads itself — every run-versioned read (defined relationships, per-column
semantic annotations) is scoped by the :class:`~dataraum.lifecycle.BaseRunMap`
pinned once at run start and passed in. An absent pin reads EMPTY, never
cross-run (fail-closed, DAT-429).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.analysis.semantic.utils import load_column_concepts
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.models import CURATED_SLICE_BUDGET
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.lifecycle import BaseRunMap
from dataraum.storage import Table

if TYPE_CHECKING:
    import duckdb

logger = get_logger(__name__)


def get_multi_table_schema_for_llm(
    session: Session,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    *,
    base_runs: BaseRunMap,
) -> dict[str, Any]:
    """Get schemas for multiple tables with semantic annotations and relationships.

    This is the primary function for multi-table validation. It fetches all
    table schemas along with detected relationships between them, every
    run-versioned read scoped by the pinned ``base_runs`` (ADR-0008 in-run
    mode — the resolver never resolves heads itself).

    Args:
        session: Database session
        table_ids: List of table IDs to include
        duckdb_conn: Optional DuckDB connection for row counts
        base_runs: The run's pinned upstream heads. An absent pin
            (``relationship_run_id is None`` / table missing from
            ``semantic_runs``) reads EMPTY — fail-closed, never cross-run.

    Returns:
        Dict with:
        - tables: List of table schemas (with row counts if duckdb_conn provided)
        - relationships: List of LLM-confirmed relationships between tables
        - enriched_views: List of available pre-joined views
    """
    if not table_ids:
        return {"error": "No tables found"}

    # Fetch all tables with their columns. Annotations are NOT loaded through
    # the ORM relationship: ``Column.semantic_annotation`` is one-to-one over a
    # run-versioned table — under multi-run coexistence (two add_source runs of
    # the same table) it silently picks one of N coexisting rows. The explicit
    # run-scoped query below replaces it.
    table_query = (
        select(Table).where(Table.table_id.in_(table_ids)).options(selectinload(Table.columns))
    )
    table_result = session.execute(table_query)
    tables = table_result.scalars().all()

    if not tables:
        return {"error": "No tables found"}

    # A requested table missing from metadata silently shrinks the LLM's
    # schema — surface it loudly (DAT-439 sweep): the session-scope wiring
    # asked for a table that does not exist.
    if len(tables) < len(table_ids):
        missing = sorted(set(table_ids) - {t.table_id for t in tables})
        logger.warning("validation_schema_tables_missing", missing_table_ids=missing)

    annotations = _load_pinned_annotations(session, tables, base_runs.semantic_runs)
    # Catalogue-grain concepts (DAT-637), pinned to the same begin_session run.
    concepts = (
        load_column_concepts(session, table_ids, base_runs.relationship_run_id)
        if base_runs.relationship_run_id
        else {}
    )

    # Build table schemas
    table_schemas = []
    table_id_to_name = {}
    column_id_to_info = {}

    for table in tables:
        if not table.duckdb_path:
            # Excluded from the schema the LLM sees — loud, never a silent
            # drop (DAT-439 sweep). A typed table without a lake path is a
            # pipeline wiring bug.
            logger.warning(
                "validation_schema_table_without_duckdb_path",
                table=table.table_name,
                table_id=table.table_id,
            )
            continue

        row_count = None
        if duckdb_conn:
            try:
                result = duckdb_conn.execute(
                    f'SELECT COUNT(*) FROM "{table.duckdb_path}"'
                ).fetchone()
                row_count = result[0] if result else None
            except Exception:
                # DAT-439 decision (item 4): KEEP this swallow. Row counts are
                # LLM-context garnish — a failed COUNT(*) must not block
                # schema assembly. The real failure it could mask (table
                # missing from the lake) surfaces loudly downstream: bind
                # proves every generated SQL with EXPLAIN, which fails →
                # bind ERROR → the artifact stays ``declared`` with the
                # reason. Pinned by test_bind_missing_lake_table_fails_explain
                # and test_missing_lake_table_keeps_schema_without_row_count.
                logger.warning(
                    "row_count_failed",
                    table=table.table_name,
                    duckdb_path=table.duckdb_path,
                )

        schema = _format_table_schema(table, annotations, concepts, row_count=row_count)
        table_schemas.append(schema)
        table_id_to_name[table.table_id] = table.table_name

        # Build column lookup for relationship formatting
        for col in table.columns:
            column_id_to_info[col.column_id] = {
                "table_name": table.table_name,
                "duckdb_path": table.duckdb_path,
                "column_name": col.column_name,
            }

    if not table_schemas:
        return {"error": "No tables with DuckDB paths found"}

    # The defined relationships (not candidate) between these tables, scoped to
    # the PINNED begin_session run (ADR-0008 in-run mode). **Fail-closed
    # (DAT-429, session isolation):** with no pinned run we MUST NOT fall back
    # to a cross-run read (``run_id=None`` reads ALL runs), which would surface
    # OTHER sessions' relationships into this schema. Leave relationships empty
    # instead; the table schemas above are keyed by table_id and are unaffected.
    run_id = base_runs.relationship_run_id
    relationships = (
        load_defined_relationships(session, table_ids, run_id=run_id) if run_id is not None else []
    )

    # Format relationships. relationship_type is served VERBATIM and is now the
    # trustworthy edge-kind owner (DAT-850, resolved+enforced at the write
    # site): a 'conformed_dimension' entry is two facts meeting at a shared
    # axis, and the reference-integrity sql_hints (orphan_transactions) gate
    # their legs on type='foreign_key' — served typed and loud, never silently
    # dropped from the schema context.
    formatted_rels = []
    for rel in relationships:
        from_info = column_id_to_info.get(rel.from_column_id, {})
        to_info = column_id_to_info.get(rel.to_column_id, {})

        if from_info and to_info:
            formatted_rels.append(
                {
                    "from_table": from_info.get("duckdb_path") or from_info.get("table_name"),
                    "from_column": from_info.get("column_name"),
                    "to_table": to_info.get("duckdb_path") or to_info.get("table_name"),
                    "to_column": to_info.get("column_name"),
                    "relationship_type": rel.relationship_type,
                    "cardinality": rel.cardinality,
                    "confidence": rel.confidence,
                }
            )

    # Fetch slice definitions (categorical value distributions) for these tables.
    # Run-versioned (DAT-448), sealed at begin_session's session grain — scoped by
    # the SAME pin as the relationships above; unpinned reads EMPTY, never
    # cross-run. CURATED read (DAT-725): the catalog is the full deterministic
    # inventory, so only the top-priority budget decorates the schemas with
    # value distributions (1 = most interesting; column_name tiebreak keeps the
    # cut deterministic across floor-priority structural rows).
    slices = (
        list(
            session.execute(
                select(SliceDefinition)
                .where(
                    SliceDefinition.table_id.in_(table_ids),
                    SliceDefinition.run_id == run_id,
                )
                .order_by(SliceDefinition.slice_priority, SliceDefinition.column_name)
                .limit(CURATED_SLICE_BUDGET)
            )
            .scalars()
            .all()
        )
        if run_id is not None
        else []
    )

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


def _load_pinned_annotations(
    session: Session,
    tables: list[Table] | Any,
    semantic_runs: dict[str, str],
) -> dict[str, SemanticAnnotation]:
    """Load each table's semantic annotations at its PINNED run, keyed by column_id.

    Replaces the ``Column.semantic_annotation`` one-to-one ORM navigation,
    which is broken under multi-run coexistence (the table is run-versioned
    with a ``(column_id, run_id)`` UNIQUE — N runs leave N rows per column and
    the one-to-one silently picks one). A table with no pinned semantic run
    contributes nothing — fail-closed, never an arbitrary run's annotations.
    """
    annotations: dict[str, SemanticAnnotation] = {}
    for table in tables:
        run_id = semantic_runs.get(table.table_id)
        if run_id is None:
            continue
        column_ids = [col.column_id for col in table.columns]
        if not column_ids:
            continue
        rows = (
            session.execute(
                select(SemanticAnnotation).where(
                    SemanticAnnotation.column_id.in_(column_ids),
                    SemanticAnnotation.run_id == run_id,
                )
            )
            .scalars()
            .all()
        )
        annotations.update({ann.column_id: ann for ann in rows})
    return annotations


def _format_table_schema(
    table: Table,
    annotations: dict[str, SemanticAnnotation],
    concepts: dict[str, ColumnConcept],
    *,
    row_count: int | None = None,
) -> dict[str, Any]:
    """Format a single table's schema.

    Args:
        table: Table ORM object with columns loaded
        annotations: run-pinned object-grain semantic annotations keyed by column_id
        concepts: run-pinned catalogue-grain column concepts keyed by column_id
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

        ann = annotations.get(col.column_id)
        concept = concepts.get(col.column_id)
        if ann is not None or concept is not None:
            col_info["semantic"] = {
                "role": ann.semantic_role if ann else None,
                "entity_type": ann.entity_type if ann else None,
                "business_name": ann.business_name if ann else None,
                "business_description": ann.business_description if ann else None,
                "meaning": concept.meaning if concept else None,
                "temporal_behavior": concept.temporal_behavior if concept else None,
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
                if sem.get("meaning"):
                    col_line += f' meaning="{sem["meaning"]}"'
                if sem.get("temporal_behavior"):
                    col_line += f' temporal_behavior="{sem["temporal_behavior"]}"'
                if sem.get("business_description"):
                    desc = sem["business_description"][:500]
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
