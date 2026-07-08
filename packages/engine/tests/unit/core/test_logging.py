"""Tests for structlog proxy logger routing and the OTel logs bridge (DAT-707)."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from io import StringIO

import pytest
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.trace import TracerProvider

import dataraum.core.logging as logging_module
from dataraum.core.logging import (
    _add_trace_context,
    _fmt_value,
    _passthrough_renderer,
    _ProxyLogger,
    _ProxyLoggerFactory,
    enable_otel_logging,
    get_logger,
)


class TestProxyLogger:
    def test_msg_writes_to_stderr(self, monkeypatch: object) -> None:
        """Output goes to stderr."""
        buf = StringIO()
        monkeypatch.setattr(sys, "stderr", buf)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="phase.done", level="info", phase="typing", status="completed")

        output = buf.getvalue()
        assert "phase.done" in output
        assert "typing" in output
        assert "status: completed" in output

    def test_stderr_hides_info_level(self, monkeypatch: object) -> None:
        """Info level is not shown in stderr output (only warning/error)."""
        buf = StringIO()
        monkeypatch.setattr(sys, "stderr", buf)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="phase.done", level="info", phase="typing")

        assert "[info]" not in buf.getvalue()

    def test_stderr_shows_warning_level(self, monkeypatch: object) -> None:
        """Warning level IS shown in stderr output."""
        buf = StringIO()
        monkeypatch.setattr(sys, "stderr", buf)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="slow_query", level="warning", phase="typing")

        assert "[warning]" in buf.getvalue()

    def test_aliases_match_msg(self) -> None:
        """All level aliases point to the same method as msg."""
        proxy = _ProxyLogger()
        aliases = [
            "log",
            "debug",
            "info",
            "warn",
            "warning",
            "err",
            "error",
            "exception",
        ]
        for alias in aliases:
            assert getattr(proxy, alias) == proxy.msg, f"{alias} does not match msg"

    def test_factory_returns_proxy(self) -> None:
        """Factory returns a _ProxyLogger instance."""
        factory = _ProxyLoggerFactory()
        logger = factory()
        assert isinstance(logger, _ProxyLogger)


class TestFmtValue:
    def test_float_rounds_to_2dp(self) -> None:
        assert _fmt_value(0.487443167) == "0.49"

    def test_whole_float_shows_as_int(self) -> None:
        assert _fmt_value(3.0) == "3"

    def test_int_passthrough(self) -> None:
        assert _fmt_value(42) == "42"

    def test_string_passthrough(self) -> None:
        assert _fmt_value("completed") == "completed"


class TestPassthroughRenderer:
    def test_returns_event_dict_unchanged(self) -> None:
        """Passthrough renderer returns the dict as-is."""
        event_dict = {"event": "test", "level": "info", "key": "value"}
        result = _passthrough_renderer(None, "info", event_dict)
        assert result is event_dict


class TestAddTraceContext:
    def test_stamps_ids_inside_span(self) -> None:
        """Inside a recording span, trace_id/span_id land as hex strings."""
        tracer = TracerProvider().get_tracer("test")
        with tracer.start_as_current_span("s") as span:
            event_dict = _add_trace_context(None, "info", {"event": "e"})
        ctx = span.get_span_context()
        assert event_dict["trace_id"] == format(ctx.trace_id, "032x")
        assert event_dict["span_id"] == format(ctx.span_id, "016x")

    def test_untouched_outside_span(self) -> None:
        """No active span (telemetry off / host dev) → dict passes through."""
        event_dict = _add_trace_context(None, "info", {"event": "e"})
        assert "trace_id" not in event_dict
        assert "span_id" not in event_dict


class TestOtelEmit:
    @pytest.fixture
    def exporter(self) -> Iterator[InMemoryLogRecordExporter]:
        """OTLP shipping enabled against an in-memory exporter; off after."""
        exporter = InMemoryLogRecordExporter()
        provider = LoggerProvider()
        provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
        enable_otel_logging(provider)
        try:
            yield exporter
        finally:
            logging_module._otel_logger = None

    def test_ships_body_severity_attributes(self, exporter: InMemoryLogRecordExporter) -> None:
        """Event name → body, level → severity, kv → coerced attributes."""
        get_logger("test").warning("slow_thing", elapsed=1.5, table="x", extra={"a": 1})
        (record,) = [f.log_record for f in exporter.get_finished_logs()]
        assert record.body == "slow_thing"
        assert record.severity_text == "WARNING"
        # Primitives pass through; non-primitives are str()-coerced.
        assert dict(record.attributes or {}) == {
            "elapsed": 1.5,
            "table": "x",
            "extra": "{'a': 1}",
        }

    def test_record_carries_active_span_context(self, exporter: InMemoryLogRecordExporter) -> None:
        """Records emitted inside a span correlate with it (Loki→Tempo hop)."""
        tracer = TracerProvider().get_tracer("test")
        with tracer.start_as_current_span("s") as span:
            get_logger("test").info("in_span")
        (record,) = [f.log_record for f in exporter.get_finished_logs()]
        assert record.trace_id == span.get_span_context().trace_id
        assert record.span_id == span.get_span_context().span_id
        # trace ids ride the record's own context, not the attribute bag
        assert "trace_id" not in dict(record.attributes or {})

    def test_exception_becomes_exception_attributes(
        self, exporter: InMemoryLogRecordExporter
    ) -> None:
        """logger.exception() ships the real exception, not a string blob."""
        try:
            raise ValueError("boom")
        except ValueError:
            get_logger("test").exception("it_failed", phase="typing")
        (record,) = [f.log_record for f in exporter.get_finished_logs()]
        attrs = dict(record.attributes or {})
        assert attrs["exception.type"] == "ValueError"
        assert attrs["exception.message"] == "boom"
        assert "raise ValueError" in str(attrs["exception.stacktrace"])
        assert record.severity_text == "ERROR"

    def test_off_by_default_no_ship_no_error(self) -> None:
        """Without enable_otel_logging, logging works and nothing ships."""
        assert logging_module._otel_logger is None
        get_logger("test").info("plain_line", key="value")
