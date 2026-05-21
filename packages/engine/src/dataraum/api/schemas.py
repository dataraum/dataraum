"""Pydantic response models for the engine REST surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class Workspace(BaseModel):
    """The active workspace metadata.

    Slice 1 has exactly one workspace per server. Table/source counts
    land in DAT-344 once those models gain a ``workspace_id`` FK.
    """

    workspace_id: str = Field(..., description="UUID for the workspace.")
    name: str = Field(..., description="Display name. Defaults to 'default'.")
    config_dir: str = Field(
        ...,
        description="Absolute path to the writable config overlay on the mounted volume.",
    )
    created_at: datetime = Field(..., description="When the workspace row was created.")


class Source(BaseModel):
    """A data source registered in the workspace.

    Mirrors the field set the MCP ``list_sources`` tool returned — same
    public shape, just an HTTP envelope. Sensitive fields (credentials,
    connection strings) are never surfaced.
    """

    name: str = Field(..., description="Source name (unique within the workspace).")
    type: str = Field(
        ...,
        description="Source type discriminator: 'csv', 'parquet', 'json', 'jsonl', 'directory', 'db_recipe'.",
    )
    status: str = Field(..., description="Lifecycle status (e.g. 'configured').")
    path: str | None = Field(
        None,
        description="Filesystem path for file/directory sources; recipe path for db_recipe sources.",
    )
    backend: str | None = Field(
        None,
        description="Backend identifier for db_recipe sources (e.g. 'mssql'). Absent for file sources.",
    )
    recipe_tables: list[str] | None = Field(
        None,
        description="Named SELECT queries declared in a db_recipe yaml. Absent for file sources.",
    )
