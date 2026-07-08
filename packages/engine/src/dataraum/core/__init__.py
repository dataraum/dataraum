"""Core module - configuration, connections, and shared models."""

from dataraum.core.connections import (
    ConnectionConfig,
    ConnectionManager,
    close_default_manager,
    get_connection_manager,
)
from dataraum.core.logging import (
    configure_logging,
    get_logger,
)
from dataraum.core.models.base import (
    Cardinality,
    ColumnRef,
    DataType,
    DecisionSource,
    QualitySeverity,
    RelationshipType,
    Result,
    SemanticRole,
    SourceConfig,
    TableRef,
)

__all__ = [
    # Connections
    "ConnectionConfig",
    "ConnectionManager",
    "close_default_manager",
    "get_connection_manager",
    # Logging
    "configure_logging",
    "get_logger",
    # Models - enums
    "Cardinality",
    "DataType",
    "DecisionSource",
    "QualitySeverity",
    "RelationshipType",
    "SemanticRole",
    # Models - base data structures
    "ColumnRef",
    "Result",
    "SourceConfig",
    "TableRef",
]
