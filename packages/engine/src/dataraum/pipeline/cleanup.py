"""Small DB helpers used by phases' ``cleanup()`` methods."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.sql.dml import Delete

from dataraum.storage.models import Table


def exec_delete(session: Session, stmt: Delete) -> int:  # type: ignore[type-arg]
    """Execute a DELETE statement and return the row count."""
    result = session.execute(stmt)
    rc: int = result.rowcount  # type: ignore[attr-defined]
    return rc


def get_slice_table_names(source_id: str, session: Session) -> list[str]:
    """Get table_name values for slice-layer tables."""
    stmt = select(Table.table_name).where(
        Table.source_id == source_id,
        Table.layer == "slice",
    )
    return list(session.execute(stmt).scalars().all())
