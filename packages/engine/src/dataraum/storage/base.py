"""SQLAlchemy base configuration and schema initialization.

Engine and session management is handled by core.connections.ConnectionManager.
This module provides:
- Base: SQLAlchemy declarative base for all models
- init_database: Schema creation
- reset_database: Schema reset (drop and recreate)
"""

from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase

# Naming convention for constraints
# This ensures consistent constraint names across PostgreSQL and SQLite
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata_obj = MetaData(naming_convention=convention)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    metadata = metadata_obj


def load_all_models() -> None:
    """Import every module that defines SQLAlchemy models.

    Populates ``Base.metadata`` with the complete engine schema without
    touching a database. Used by schema creation (``init_database``) and
    the offline DDL dump (``dump_ddl``).
    """
    # Core models not owned by any phase
    from dataraum.entropy import db_models as _entropy  # noqa: F401
    from dataraum.investigation import db_models as _investigation  # noqa: F401
    from dataraum.lifecycle import db_models as _lifecycle  # noqa: F401

    # Phase-owned models: auto-discovered from registry
    from dataraum.pipeline.registry import import_all_phase_models
    from dataraum.query import snippet_models as _snippets  # noqa: F401
    from dataraum.storage import models as _storage  # noqa: F401
    from dataraum.storage import overlay_models as _overlay  # noqa: F401
    from dataraum.storage import snapshot_head as _snapshot_head  # noqa: F401

    import_all_phase_models()


def init_database(engine: Engine) -> None:
    """
    Initialize database schema.

    Creates all tables defined in SQLAlchemy models.
    Safe to call multiple times - only creates missing tables.

    Args:
        engine: SQLAlchemy engine
    """
    load_all_models()

    with engine.begin() as conn:
        Base.metadata.create_all(conn)


def reset_database(engine: Engine) -> None:
    """
    Drop and recreate all tables.

    WARNING: This destroys all data. Use only in development/testing.

    Args:
        engine: SQLAlchemy engine
    """
    load_all_models()

    with engine.begin() as conn:
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)
