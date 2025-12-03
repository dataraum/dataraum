"""Staging layer models.

Defines data structures for the VARCHAR-first staging process."""

from __future__ import annotations

from pydantic import BaseModel, Field


class StagedColumn(BaseModel):
    """A column in a staged table."""

    column_id: str
    name: str
    position: int
    sample_values: list[str] = Field(default_factory=list)


class StagedTable(BaseModel):
    """A staged table."""

    table_id: str
    table_name: str
    raw_table_name: str
    row_count: int
    columns: list[StagedColumn]


class StagingResult(BaseModel):
    """Result of staging operation."""

    source_id: str
    tables: list[StagedTable]
    total_rows: int
    duration_seconds: float
