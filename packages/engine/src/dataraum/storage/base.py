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


def init_database(engine: Engine) -> None:
    """
    Initialize database schema.

    Creates all tables defined in SQLAlchemy models.
    Safe to call multiple times - only creates missing tables.

    Args:
        engine: SQLAlchemy engine
    """
    # Core models not owned by any phase
    from dataraum.documentation import db_models as _fixes  # noqa: F401
    from dataraum.investigation import db_models as _investigation  # noqa: F401
    from dataraum.pipeline import db_models as _pipeline  # noqa: F401

    # Phase-owned models: auto-discovered from registry
    from dataraum.pipeline.registry import import_all_phase_models
    from dataraum.query import db_models as _query  # noqa: F401
    from dataraum.query import snippet_models as _snippets  # noqa: F401
    from dataraum.storage import models as _storage  # noqa: F401

    import_all_phase_models()

    with engine.begin() as conn:
        Base.metadata.create_all(conn)


def reset_database(engine: Engine) -> None:
    """
    Drop and recreate all tables.

    WARNING: This destroys all data. Use only in development/testing.

    Args:
        engine: SQLAlchemy engine
    """
    # Core models not owned by any phase
    from dataraum.documentation import db_models as _fixes  # noqa: F401
    from dataraum.investigation import db_models as _investigation  # noqa: F401
    from dataraum.pipeline import db_models as _pipeline  # noqa: F401

    # Phase-owned models: auto-discovered from registry
    from dataraum.pipeline.registry import import_all_phase_models
    from dataraum.query import db_models as _query  # noqa: F401
    from dataraum.query import snippet_models as _snippets  # noqa: F401
    from dataraum.storage import models as _storage  # noqa: F401

    import_all_phase_models()

    with engine.begin() as conn:
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)
