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
from dataclasses import dataclass
from typing import Any, cast

import structlog
from rich.console import Console as _RichConsole
from rich.text import Text as _RichText
from structlog.typing import EventDict, FilteringBoundLogger

# Context variables for correlation
_run_context: ContextVar[dict[str, Any] | None] = ContextVar("run_context", default=None)


_active_console: _RichConsole | None = None
_active_log_buffer: LogBuffer | None = None  # forward ref, defined below

_LEVEL_STYLES: dict[str, str] = {
    "debug": "dim",
    "info": "green",
    "warning": "yellow",
    "error": "red bold",
    "critical": "red bold reverse",
}

_LEVEL_ICONS: dict[str, str] = {
    "debug": "\U0001f916",
    "info": "\U0001f4a1",
    "warning": "\U0001f4e2",
    "error": "\U0001f6a8",
    "critical": "\U0001f6a8",
}


@dataclass
class LogBuffer:
    """Prints log lines permanently above a Rich Live widget.

    When a Live display is active, Rich's ``console.print()`` automatically
    renders above it. This class simply holds a console reference so the
    ``_ProxyLogger`` can route there instead of bypassing Live.
    """

    console: _RichConsole

    def append(self, text: _RichText) -> None:
        """Print a log line above the Live widget."""
        self.console.print(text, highlight=False)


def _fmt_value(v: Any) -> str:
    """Format a log value for display — round floats, pass through rest."""
    if isinstance(v, float):
        return f"{v:.2f}" if v != int(v) else str(int(v))
    return str(v)


class _ProxyLogger:
    """Logger that routes structured log events to Rich or stderr.

    When a Rich console is active (via ``activate_console``), builds a
    ``rich.text.Text`` object directly from the structured event dict —
    no ANSI round-trip.  When inactive, formats a plain string for stderr.

    structlog dispatches ``dict`` return values from the final processor
    as ``logger.msg(**event_dict)``, so ``msg`` receives keyword arguments.
    """

    def msg(self, **kv: Any) -> None:
        kv.pop("timestamp", None)
        level = kv.pop("level", "")
        event = kv.pop("event", "")
        phase = kv.pop("phase", "")

        buf = _active_log_buffer
        if buf is not None:
            buf.append(self._build_text(level, event, phase, kv))
        elif _active_console is not None:
            _active_console.print(self._build_text(level, event, phase, kv), highlight=False)
        else:
            self._stderr_print(level, event, phase, kv)

    @staticmethod
    def _build_text(level: str, event: str, phase: str, kv: dict[str, Any]) -> _RichText:
        icon = _LEVEL_ICONS.get(level, "")
        text = _RichText(f"  {icon}" if icon else "  ")
        level_style = _LEVEL_STYLES.get(level, "")

        if phase:
            text.append(f"{phase:<16} ", style="bold " + level_style)
        text.append(str(event), style=level_style)

        if kv:
            pairs = ", ".join(f"{k}: {_fmt_value(v)}" for k, v in kv.items())
            text.append(f"  ({pairs})", style="dim")

        return text

    @staticmethod
    def _stderr_print(level: str, event: str, phase: str, kv: dict[str, Any]) -> None:
        parts: list[str] = []
        if level and level not in ("info", "debug"):
            parts.append(f"[{level}]")
        if phase:
            parts.append(phase)
        parts.append(str(event))
        if kv:
            pairs = ", ".join(f"{k}: {_fmt_value(v)}" for k, v in kv.items())
            parts.append(f"({pairs})")
        print("  ".join(parts), file=sys.stderr, flush=True)

    log = debug = info = warn = warning = msg
    err = error = exception = msg


class _ProxyLoggerFactory:
    def __call__(self, *args: Any) -> _ProxyLogger:
        return _ProxyLogger()


def _passthrough_renderer(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Final processor that passes the structured dict to the logger as-is."""
    return event_dict


def activate_console(console: _RichConsole, log_buffer: LogBuffer | None = None) -> None:
    """Route structlog output through a Rich console or log buffer.

    Args:
        console: Rich console for direct printing (used when no buffer).
        log_buffer: If provided, log lines are appended here instead of
            printed directly — intended for rendering inside a Live widget.
    """
    global _active_console, _active_log_buffer
    _active_console = console
    _active_log_buffer = log_buffer


def deactivate_console() -> None:
    """Restore structlog output to stderr."""
    global _active_console, _active_log_buffer
    _active_console = None
    _active_log_buffer = None


@dataclass
class LogConfig:
    """Logging configuration."""

    level: str = "INFO"
    format: str = "console"  # "console" or "json"
    show_timestamps: bool = True
    show_caller: bool = False  # Show file:line in console mode
    color: bool = True


def _add_run_context(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Processor to add run context to log events."""
    context = _run_context.get()
    if context:
        event_dict.update(context)
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
        # Console format — pass structured dict to _ProxyLogger for rendering
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            _passthrough_renderer,
        ]

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level.upper())),
        context_class=dict,
        logger_factory=_ProxyLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging for libraries
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level.upper()),
        handlers=[logging.StreamHandler(sys.stderr)],
        force=True,
    )

    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("anthropic").setLevel(logging.WARNING)


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


# Initialize with default configuration
configure_logging()
