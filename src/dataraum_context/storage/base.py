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
    # Import all model modules to register them with SQLAlchemy Base metadata
    # These imports ensure tables are created when init_database() is called
    from dataraum_context.analysis.correlation import db_models as _correlation_models  # noqa: F401
    from dataraum_context.analysis.cycles import db_models as _cycles_models  # noqa: F401
    from dataraum_context.analysis.quality_summary import (
        db_models as _quality_summary_models,  # noqa: F401
    )
    from dataraum_context.analysis.relationships import (
        db_models as _relationships_models,  # noqa: F401
    )
    from dataraum_context.analysis.semantic import db_models as _semantic_models  # noqa: F401
    from dataraum_context.analysis.slicing import db_models as _slicing_models  # noqa: F401
    from dataraum_context.analysis.statistics import db_models as _statistics_models  # noqa: F401
    from dataraum_context.analysis.temporal import db_models as _temporal_models  # noqa: F401
    from dataraum_context.analysis.temporal_slicing import (
        db_models as _temporal_slicing_models,  # noqa: F401
    )
    from dataraum_context.analysis.topology import db_models as _topology_models  # noqa: F401
    from dataraum_context.analysis.typing import db_models as _typing_models  # noqa: F401
    from dataraum_context.analysis.validation import db_models as _validation_models  # noqa: F401
    from dataraum_context.graphs import db_models as _graphs_models  # noqa: F401
    from dataraum_context.llm import db_models as _llm_models  # noqa: F401
    from dataraum_context.pipeline import db_models as _pipeline_models  # noqa: F401
    from dataraum_context.storage import models as _storage_models  # noqa: F401

    with engine.begin() as conn:
        Base.metadata.create_all(conn)


def reset_database(engine: Engine) -> None:
    """
    Drop and recreate all tables.

    WARNING: This destroys all data. Use only in development/testing.

    Args:
        engine: SQLAlchemy engine
    """
    # Import all model modules to register them with SQLAlchemy Base metadata
    from dataraum_context.analysis.correlation import db_models as _correlation_models  # noqa: F401
    from dataraum_context.analysis.cycles import db_models as _cycles_models  # noqa: F401
    from dataraum_context.analysis.quality_summary import (
        db_models as _quality_summary_models,  # noqa: F401
    )
    from dataraum_context.analysis.relationships import (
        db_models as _relationships_models,  # noqa: F401
    )
    from dataraum_context.analysis.semantic import db_models as _semantic_models  # noqa: F401
    from dataraum_context.analysis.slicing import db_models as _slicing_models  # noqa: F401
    from dataraum_context.analysis.statistics import db_models as _statistics_models  # noqa: F401
    from dataraum_context.analysis.temporal import db_models as _temporal_models  # noqa: F401
    from dataraum_context.analysis.temporal_slicing import (
        db_models as _temporal_slicing_models,  # noqa: F401
    )
    from dataraum_context.analysis.topology import db_models as _topology_models  # noqa: F401
    from dataraum_context.analysis.typing import db_models as _typing_models  # noqa: F401
    from dataraum_context.analysis.validation import db_models as _validation_models  # noqa: F401
    from dataraum_context.graphs import db_models as _graphs_models  # noqa: F401
    from dataraum_context.llm import db_models as _llm_models  # noqa: F401
    from dataraum_context.pipeline import db_models as _pipeline_models  # noqa: F401
    from dataraum_context.storage import models as _storage_models  # noqa: F401

    with engine.begin() as conn:
        Base.metadata.drop_all(conn)
        Base.metadata.create_all(conn)
