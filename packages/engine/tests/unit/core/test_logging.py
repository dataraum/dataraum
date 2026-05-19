"""Tests for structlog proxy logger routing."""

from __future__ import annotations

import sys
from io import StringIO

from dataraum.core.logging import (
    _fmt_value,
    _passthrough_renderer,
    _ProxyLogger,
    _ProxyLoggerFactory,
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
