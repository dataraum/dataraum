"""Storage layer for metadata persistence.

This module provides:
- Base: SQLAlchemy declarative base for all models
- Source, Table, Column: Core entity models
- init_database, reset_database: Schema management

Note: Engine and session management is handled by core.connections.ConnectionManager.
"""

from dataraum.storage.base import (
    Base,
    init_database,
    metadata_obj,
    reset_database,
)
from dataraum.storage.models import Column, Source, Table
from dataraum.storage.overlay_models import ConfigOverlay
from dataraum.storage.snapshot_head import (
    GENERATION_STAGE,
    MetadataSnapshotHead,
    catalog_head_target,
    head_run_id,
)

__all__ = [
    # Base and metadata
    "Base",
    "metadata_obj",
    # Core entities
    "Source",
    "Table",
    "Column",
    # Teach overlay (DAT-343)
    "ConfigOverlay",
    # Snapshot version axis (DAT-413)
    "MetadataSnapshotHead",
    "head_run_id",
    "catalog_head_target",
    "GENERATION_STAGE",
    # Database management
    "init_database",
    "reset_database",
]
