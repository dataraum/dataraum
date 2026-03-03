"""Shared utility functions for semantic analysis."""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

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


def load_derived_columns_for_semantic(
    session: Session,
    table_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Load derived column data for semantic analysis context.

    Args:
        session: Database session
        table_ids: List of table IDs to load derived columns for

    Returns:
        Dictionary mapping table_name to list of derived column dicts
    """
    from dataraum.analysis.correlation.db_models import (
        DerivedColumn,
    )

    # Load table name mapping
    table_map = load_table_mappings(session, table_ids)
    table_id_to_name = {v: k for k, v in table_map.items()}

    # Load column name mapping
    col_stmt = (
        select(Column.column_id, Column.column_name, Table.table_name)
        .join(Table)
        .where(Table.table_id.in_(table_ids))
    )
    col_result = session.execute(col_stmt)
    col_id_to_name = {col_id: col_name for col_id, col_name, _ in col_result.all()}

    result: dict[str, list[dict[str, Any]]] = {name: [] for name in table_map}

    # Load derived columns
    derived_stmt = select(DerivedColumn).where(DerivedColumn.table_id.in_(table_ids))
    derived_result = session.execute(derived_stmt)

    for derived in derived_result.scalars().all():
        table_name_opt = table_id_to_name.get(derived.table_id)
        if not table_name_opt:
            continue
        table_name = table_name_opt

        derived_col_name = col_id_to_name.get(derived.derived_column_id, derived.derived_column_id)
        source_names = []
        for col_id in derived.source_column_ids:
            col_name = col_id_to_name.get(col_id, col_id)
            source_names.append(col_name)

        result[table_name].append(
            {
                "derived_column": derived_col_name,
                "source_columns": source_names,
                "formula": derived.formula,
                "match_rate": derived.match_rate,
            }
        )

    return result
