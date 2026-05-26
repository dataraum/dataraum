"""Shared utility functions for semantic analysis."""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.storage import Column, Table


def load_table_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[str, str]:
    """Load mapping of table_name -> table_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping table_name to table_id
    """
    stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    result = session.execute(stmt)
    return dict(result.tuples().all())


def load_column_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[tuple[str, str], str]:
    """Load mapping of (table_name, column_name) -> column_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping (table_name, column_name) tuples to column_id
    """
    stmt = (
        select(Table.table_name, Column.column_name, Column.column_id)
        .join(Column)
        .where(Table.table_id.in_(table_ids))
    )
    result = session.execute(stmt)
    return {(table_name, col_name): col_id for table_name, col_name, col_id in result.all()}


def load_persisted_annotations(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load persisted per-column semantic annotations for the given tables.

    The per-table synthesis phase (DAT-362) reads these as read-only context —
    they are the post-teach column annotations produced + persisted by the
    per-column phase. Returns one dict per annotated column with the fields the
    per-table prompt needs to reason about table classification + relationships.

    Args:
        session: Database session.
        table_ids: Table IDs whose columns' annotations to load.

    Returns:
        List of ``{table_name, column_name, semantic_role, business_concept,
        entity_type, confidence}`` dicts, ordered by table then column.
    """
    stmt = (
        select(
            Table.table_name,
            Column.column_name,
            SemanticAnnotation.semantic_role,
            SemanticAnnotation.business_concept,
            SemanticAnnotation.entity_type,
            SemanticAnnotation.confidence,
        )
        .join(Column, SemanticAnnotation.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
        .order_by(Table.table_name, Column.column_position)
    )
    rows = session.execute(stmt).all()
    return [
        {
            "table_name": table_name,
            "column_name": column_name,
            "semantic_role": semantic_role,
            "business_concept": business_concept,
            "entity_type": entity_type,
            "confidence": confidence,
        }
        for table_name, column_name, semantic_role, business_concept, entity_type, confidence in rows
    ]
