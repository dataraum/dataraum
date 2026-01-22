"""Structured logging infrastructure for cloud-ready instrumentation.

This module provides extensible logging that works for:
- Local CLI development (rich console output)
- Cloud deployments (JSON structured logs)
- Future OpenTelemetry integration

Usage:
    from dataraum.core.logging import get_logger, configure_logging

    # Configure at startup
    configure_logging(log_level="INFO", log_format="console")

    # Get logger in any module
    logger = get_logger(__name__)

    # Log with structured context
    logger.info("phase_started", phase="import", source_id="abc123")

    # Use context managers for automatic context propagation
    with logger.context(run_id="run-123", phase="typing"):
        logger.info("processing_table", table="customers", rows=1000)
"""

from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import structlog
from structlog.typing import FilteringBoundLogger

# Context variables for correlation
_run_context: ContextVar[dict[str, Any] | None] = ContextVar("run_context", default=None)


@dataclass
class LogConfig:
    """Logging configuration."""

    level: str = "INFO"
    format: str = "console"  # "console" or "json"
    show_timestamps: bool = True
    show_caller: bool = False  # Show file:line in console mode
    color: bool = True


@dataclass
class PhaseMetrics:
    """Metrics collected during phase execution."""

    phase_name: str
    start_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    end_time: datetime | None = None

    # Counters
    tables_processed: int = 0
    columns_processed: int = 0
    rows_processed: int = 0
    llm_calls: int = 0
    llm_input_tokens: int = 0
    llm_output_tokens: int = 0
    db_queries: int = 0
    db_writes: int = 0

    # Sub-operation timings (seconds)
    timings: dict[str, float] = field(default_factory=dict)

    # Errors and warnings
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now(UTC) - self.start_time).total_seconds()

    def record_timing(self, operation: str, seconds: float) -> None:
        """Record timing for a sub-operation."""
        self.timings[operation] = self.timings.get(operation, 0.0) + seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/serialization."""
        return {
            "phase_name": self.phase_name,
            "duration_seconds": self.duration_seconds,
            "tables_processed": self.tables_processed,
            "columns_processed": self.columns_processed,
            "rows_processed": self.rows_processed,
            "llm_calls": self.llm_calls,
            "llm_input_tokens": self.llm_input_tokens,
            "llm_output_tokens": self.llm_output_tokens,
            "db_queries": self.db_queries,
            "db_writes": self.db_writes,
            "timings": self.timings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
        }


@dataclass
class PipelineMetrics:
    """Aggregate metrics for entire pipeline run."""

    run_id: str
    source_id: str
    start_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    end_time: datetime | None = None
    phases: list[PhaseMetrics] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        """Get total duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now(UTC) - self.start_time).total_seconds()

    def add_phase(self, metrics: PhaseMetrics) -> None:
        """Add phase metrics."""
        self.phases.append(metrics)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/serialization."""
        return {
            "run_id": self.run_id,
            "source_id": self.source_id,
            "duration_seconds": self.duration_seconds,
            "phase_count": len(self.phases),
            "total_llm_calls": sum(p.llm_calls for p in self.phases),
            "total_llm_tokens": sum(p.llm_input_tokens + p.llm_output_tokens for p in self.phases),
            "total_tables_processed": sum(p.tables_processed for p in self.phases),
            "total_rows_processed": sum(p.rows_processed for p in self.phases),
            "phases": [p.to_dict() for p in self.phases],
        }

    def get_slowest_phases(self, n: int = 5) -> list[tuple[str, float]]:
        """Get the N slowest phases."""
        sorted_phases = sorted(self.phases, key=lambda p: p.duration_seconds, reverse=True)
        return [(p.phase_name, p.duration_seconds) for p in sorted_phases[:n]]

    def get_bottleneck_operations(self, n: int = 5) -> list[tuple[str, str, float]]:
        """Get the N slowest operations across all phases."""
        all_ops: list[tuple[str, str, float]] = []
        for phase in self.phases:
            for op_name, duration in phase.timings.items():
                all_ops.append((phase.phase_name, op_name, duration))
        sorted_ops = sorted(all_ops, key=lambda x: x[2], reverse=True)
        return sorted_ops[:n]


# Metrics storage (per-run)
_current_metrics: ContextVar[PipelineMetrics | None] = ContextVar("current_metrics", default=None)
_current_phase_metrics: ContextVar[PhaseMetrics | None] = ContextVar(
    "current_phase_metrics", default=None
)


def start_pipeline_metrics(run_id: str, source_id: str) -> PipelineMetrics:
    """Start collecting metrics for a pipeline run."""
    metrics = PipelineMetrics(run_id=run_id, source_id=source_id)
    _current_metrics.set(metrics)
    return metrics


def get_pipeline_metrics() -> PipelineMetrics | None:
    """Get current pipeline metrics."""
    return _current_metrics.get()


def start_phase_metrics(phase_name: str) -> PhaseMetrics:
    """Start collecting metrics for a phase."""
    metrics = PhaseMetrics(phase_name=phase_name)
    _current_phase_metrics.set(metrics)
    return metrics


def get_phase_metrics() -> PhaseMetrics | None:
    """Get current phase metrics."""
    return _current_phase_metrics.get()


def end_phase_metrics() -> PhaseMetrics | None:
    """End current phase metrics and add to pipeline metrics."""
    phase_metrics = _current_phase_metrics.get()
    if phase_metrics:
        phase_metrics.end_time = datetime.now(UTC)
        pipeline_metrics = _current_metrics.get()
        if pipeline_metrics:
            pipeline_metrics.add_phase(phase_metrics)
        _current_phase_metrics.set(None)
    return phase_metrics


def end_pipeline_metrics() -> PipelineMetrics | None:
    """End pipeline metrics collection."""
    metrics = _current_metrics.get()
    if metrics:
        metrics.end_time = datetime.now(UTC)
        _current_metrics.set(None)
    return metrics


def _add_run_context(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Processor to add run context to log events."""
    context = _run_context.get()
    if context:
        event_dict.update(context)
    return event_dict


def _add_metrics_context(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Processor to add current metrics context."""
    phase_metrics = _current_phase_metrics.get()
    if phase_metrics:
        event_dict["_phase"] = phase_metrics.phase_name
    pipeline_metrics = _current_metrics.get()
    if pipeline_metrics:
        event_dict["_run_id"] = pipeline_metrics.run_id
    return event_dict


def configure_logging(
    log_level: str = "INFO",
    log_format: str = "console",
    show_timestamps: bool = True,
    color: bool = True,
) -> None:
    """Configure structured logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_format: Output format ("console" for development, "json" for production/cloud)
        show_timestamps: Whether to show timestamps in console mode
        color: Whether to use colors in console mode
    """
    # Shared processors for all formats
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        _add_run_context,
        _add_metrics_context,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if show_timestamps:
        shared_processors.insert(0, structlog.processors.TimeStamper(fmt="iso", utc=True))

    if log_format == "json":
        # JSON format for cloud/production
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Console format for development
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(
                colors=color,
                exception_formatter=structlog.dev.plain_traceback,
            ),
        ]

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level.upper())),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging for libraries
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level.upper()),
        handlers=[logging.StreamHandler(sys.stderr)],
        force=True,
    )


def get_logger(name: str | None = None) -> FilteringBoundLogger:
    """Get a structured logger.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Bound structlog logger
    """
    return cast(FilteringBoundLogger, structlog.get_logger(name))


class LogContext:
    """Context manager for adding context to logs within a scope."""

    def __init__(self, **context: Any):
        """Initialize with context key-value pairs."""
        self.context = context
        self.token: Any = None

    def __enter__(self) -> LogContext:
        """Enter context, adding values to log context."""
        current = _run_context.get() or {}
        new_context = {**current, **self.context}
        self.token = _run_context.set(new_context)
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context, restoring previous values."""
        if self.token:
            _run_context.reset(self.token)


def log_context(**context: Any) -> LogContext:
    """Create a context manager for scoped logging context.

    Usage:
        with log_context(run_id="abc", phase="import"):
            logger.info("processing")  # Will include run_id and phase
    """
    return LogContext(**context)


# Convenience functions for metrics tracking
def increment_llm_call(input_tokens: int = 0, output_tokens: int = 0) -> None:
    """Increment LLM call counter in current phase metrics."""
    metrics = _current_phase_metrics.get()
    if metrics:
        metrics.llm_calls += 1
        metrics.llm_input_tokens += input_tokens
        metrics.llm_output_tokens += output_tokens


def increment_db_query() -> None:
    """Increment database query counter in current phase metrics."""
    metrics = _current_phase_metrics.get()
    if metrics:
        metrics.db_queries += 1


def increment_db_write() -> None:
    """Increment database write counter in current phase metrics."""
    metrics = _current_phase_metrics.get()
    if metrics:
        metrics.db_writes += 1


def record_tables_processed(count: int) -> None:
    """Record tables processed in current phase metrics."""
    metrics = _current_phase_metrics.get()
    if metrics:
        metrics.tables_processed += count


def record_rows_processed(count: int) -> None:
    """Record rows processed in current phase metrics."""
    metrics = _current_phase_metrics.get()
    if metrics:
        metrics.rows_processed += count


def record_operation_timing(operation: str, seconds: float) -> None:
    """Record timing for a sub-operation in current phase metrics."""
    metrics = _current_phase_metrics.get()
    if metrics:
        metrics.record_timing(operation, seconds)


# Initialize with default configuration
configure_logging()
