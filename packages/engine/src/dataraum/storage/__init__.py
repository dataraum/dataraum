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

__all__ = [
    # Base and metadata
    "Base",
    "metadata_obj",
    # Core entities
    "Source",
    "Table",
    "Column",
    # Database management
    "init_database",
    "reset_database",
]
