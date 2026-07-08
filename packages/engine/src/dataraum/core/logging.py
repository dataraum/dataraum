"""Structured logging for the engine.

structlog renders human-readable lines to stderr (the container's docker-logs
stream). When telemetry is on (``OTEL_EXPORTER_OTLP_ENDPOINT`` set — the worker
bootstrap calls :func:`enable_otel_logging`), every event additionally ships
over OTLP to Loki carrying the active span's trace context, and events emitted
inside a span carry ``trace_id``/``span_id`` on the rendered stderr line too.

Logs are the narrative; analysis lives in traces and metrics (Tempo/Prometheus
per ADR-0019). The former ``grep llm_call | jq`` aggregation path is retired
(DAT-707) — token usage and latency are span attributes and histograms now.

Usage:
    from dataraum.core.logging import get_logger, configure_logging

    # Configured at import time with defaults; call again to change the level
    configure_logging(log_level="DEBUG")

    logger = get_logger(__name__)
    logger.info("phase_started", phase="import", source_id="abc123")
"""

from __future__ import annotations

import logging
import sys
from typing import Any, cast

import structlog
from opentelemetry import trace
from opentelemetry._logs import Logger, LoggerProvider, SeverityNumber
from structlog.typing import EventDict, FilteringBoundLogger

# The OTel logs-bridge logger; None means OTLP shipping is off and logging
# behaves exactly as before telemetry existed (no collector needed for tests
# or host dev). Set via enable_otel_logging() at worker bootstrap.
_otel_logger: Logger | None = None

_SEVERITY: dict[str, SeverityNumber] = {
    "debug": SeverityNumber.DEBUG,
    "info": SeverityNumber.INFO,
    "warning": SeverityNumber.WARN,
    "error": SeverityNumber.ERROR,
    "critical": SeverityNumber.FATAL,
}


def enable_otel_logging(provider: LoggerProvider) -> None:
    """Ship every structlog event over OTLP in addition to stderr (DAT-707).

    Only structlog events ship — stdlib logging (library output) stays
    stderr-only, which also keeps the OTel SDK's own error logging out of the
    export path (no feedback loop on exporter failures).

    Once armed, every kv pair at every call site leaves the container. The
    same boundary ADR-0019 sets for spans applies to log attributes: metadata
    only (counts, ids, labels, table/column names) — never row values, prompt
    or completion text, or other business data, while PII handling (DAT-554)
    is open.

    Args:
        provider: The logs provider whose exporter pipeline receives events.
    """
    global _otel_logger
    _otel_logger = provider.get_logger("dataraum")


def _fmt_value(v: Any) -> str:
    """Format a log value for display — round floats, pass through rest."""
    if isinstance(v, float):
        return f"{v:.2f}" if v != int(v) else str(int(v))
    return str(v)


class _ProxyLogger:
    """Logger that renders structured log events to stderr.

    structlog dispatches ``dict`` return values from the final processor as
    ``logger.msg(**event_dict)``, so ``msg`` receives keyword arguments.
    """

    def msg(self, **kv: Any) -> None:
        level = kv.pop("level", "")
        event = kv.pop("event", "")
        phase = kv.pop("phase", "")

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
    err = error = exception = critical = fatal = msg


class _ProxyLoggerFactory:
    def __call__(self, *_args: Any) -> _ProxyLogger:
        return _ProxyLogger()


def _passthrough_renderer(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Final processor that passes the structured dict to the logger as-is."""
    return event_dict


def _add_trace_context(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Stamp the active span's ids so rendered lines correlate with Tempo.

    Outside a recording span (telemetry off, or code not under a span) the
    context is invalid and the event passes through untouched.
    """
    ctx = trace.get_current_span().get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _resolve_exception(exc_info: Any) -> BaseException | None:
    """Normalize structlog's ``exc_info`` forms to the exception instance."""
    if exc_info is True:
        return sys.exc_info()[1]
    if isinstance(exc_info, BaseException):
        return exc_info
    if isinstance(exc_info, tuple) and len(exc_info) == 3:
        return cast("BaseException | None", exc_info[1])
    return None


def _otel_emit(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Mirror the event to the OTel logs bridge when shipping is enabled.

    Body is the event name; the remaining kv pairs ride as attributes (Loki
    structured metadata). ``trace_id``/``span_id`` are skipped — the record's
    own trace context carries them — and ``exc_info`` becomes a real exception
    on the record (``exception.*`` attributes) rather than a string attribute.
    Runs before ``format_exc_info`` so the raw exception is still available.
    """
    if _otel_logger is None:
        return event_dict
    attributes: dict[str, Any] = {}
    for key, value in event_dict.items():
        if key in ("event", "level", "trace_id", "span_id", "exc_info"):
            continue
        if isinstance(value, str | bool | int | float):
            attributes[key] = value
        else:
            # Coerced objects get a defensive cap: an accidentally-logged
            # DataFrame/dict must not become a multi-MB OTLP attribute.
            # Explicit strings pass whole (e.g. base.py's phase_failed
            # traceback is a deliberate, bounded payload).
            text = str(value)
            attributes[key] = text[:2048] + "…[truncated]" if len(text) > 2048 else text
    level = str(event_dict.get("level", "info"))
    _otel_logger.emit(
        severity_number=_SEVERITY.get(level, SeverityNumber.INFO),
        severity_text=level.upper(),
        body=str(event_dict.get("event", "")),
        attributes=attributes,
        exception=_resolve_exception(event_dict.get("exc_info")),
    )
    return event_dict


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for the process.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
    """
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.UnicodeDecoder(),
            _add_trace_context,
            _otel_emit,
            structlog.processors.format_exc_info,
            _passthrough_renderer,
        ],
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


# Initialize with default configuration
configure_logging()
