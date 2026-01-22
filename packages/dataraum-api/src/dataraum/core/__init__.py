"""Core module - configuration, connections, and shared models."""

from dataraum.core.config import Settings, get_settings
from dataraum.core.connections import (
    ConnectionConfig,
    ConnectionManager,
    close_default_manager,
    get_connection_manager,
)
from dataraum.core.logging import (
    LogConfig,
    PhaseMetrics,
    PipelineMetrics,
    configure_logging,
    end_phase_metrics,
    end_pipeline_metrics,
    get_logger,
    get_phase_metrics,
    get_pipeline_metrics,
    increment_db_query,
    increment_db_write,
    increment_llm_call,
    log_context,
    record_operation_timing,
    record_rows_processed,
    record_tables_processed,
    start_phase_metrics,
    start_pipeline_metrics,
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
    # Config
    "Settings",
    "get_settings",
    # Connections
    "ConnectionConfig",
    "ConnectionManager",
    "close_default_manager",
    "get_connection_manager",
    # Logging
    "LogConfig",
    "PhaseMetrics",
    "PipelineMetrics",
    "configure_logging",
    "end_phase_metrics",
    "end_pipeline_metrics",
    "get_logger",
    "get_phase_metrics",
    "get_pipeline_metrics",
    "increment_db_query",
    "increment_db_write",
    "increment_llm_call",
    "log_context",
    "record_operation_timing",
    "record_rows_processed",
    "record_tables_processed",
    "start_phase_metrics",
    "start_pipeline_metrics",
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
