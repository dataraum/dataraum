"""Shared utility functions for enrichment operations."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.storage.models_v2 import Column, Table


async def load_table_mappings(
    session: AsyncSession,
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
    result = await session.execute(stmt)
    return dict(result.tuples().all())


async def load_column_mappings(
    session: AsyncSession,
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
    result = await session.execute(stmt)
    return {(table_name, col_name): col_id for table_name, col_name, col_id in result.all()}
