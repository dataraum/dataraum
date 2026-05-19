"""Staging layer models.

Defines data structures for the VARCHAR-first staging process."""

from __future__ import annotations

from pydantic import BaseModel


class StagedTable(BaseModel):
    """A staged table.

    This is a lightweight DTO for returning staging results.
    Full metadata is stored in the Table/Column SQLAlchemy models.
    """

    table_id: str
    table_name: str
    raw_table_name: str
    row_count: int
    column_count: int


class StagingResult(BaseModel):
    """Result of staging operation."""

    source_id: str
    tables: list[StagedTable]
    total_rows: int
    duration_seconds: float
