"""Tests for structlog proxy logger routing."""

from __future__ import annotations

import sys
from io import StringIO
from unittest.mock import MagicMock

from rich.console import Console
from rich.text import Text

from dataraum.core.logging import (
    _fmt_value,
    _passthrough_renderer,
    _ProxyLogger,
    _ProxyLoggerFactory,
    deactivate_console,
)


class TestProxyLogger:
    def test_msg_to_stderr_when_no_console(self, monkeypatch: object) -> None:
        """Output goes to stderr when no console is active."""
        import dataraum.core.logging as log_mod

        buf = StringIO()
        monkeypatch.setattr(sys, "stderr", buf)  # type: ignore[attr-defined]
        monkeypatch.setattr(log_mod, "_active_console", None)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="phase.done", level="info", phase="typing", status="completed")

        output = buf.getvalue()
        assert "phase.done" in output
        assert "typing" in output
        assert "status: completed" in output

    def test_msg_to_console_when_active(self, monkeypatch: object) -> None:
        """console.print() called with Rich Text and highlight=False."""
        import dataraum.core.logging as log_mod

        mock_console = MagicMock(spec=Console)
        monkeypatch.setattr(log_mod, "_active_console", mock_console)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="phase.done", level="info", phase="typing")

        mock_console.print.assert_called_once()
        args, kwargs = mock_console.print.call_args
        assert isinstance(args[0], Text)
        assert kwargs.get("highlight") is False

    def test_phase_first_then_event(self, monkeypatch: object) -> None:
        """Phase appears before event in Rich output."""
        import dataraum.core.logging as log_mod

        mock_console = MagicMock(spec=Console)
        monkeypatch.setattr(log_mod, "_active_console", mock_console)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="phase.done", level="info", phase="typing")

        text_obj: Text = mock_console.print.call_args[0][0]
        plain = text_obj.plain
        assert plain.index("typing") < plain.index("phase.done")

    def test_kv_pairs_in_parentheses(self, monkeypatch: object) -> None:
        """Key-value pairs appear in (k: v, k: v) format."""
        import dataraum.core.logging as log_mod

        mock_console = MagicMock(spec=Console)
        monkeypatch.setattr(log_mod, "_active_console", mock_console)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="phase.done", level="info", phase="typing", rows=1000)

        text_obj: Text = mock_console.print.call_args[0][0]
        plain = text_obj.plain
        assert "(rows: 1000)" in plain

    def test_no_timestamp_in_output(self, monkeypatch: object) -> None:
        """Timestamp is dropped from output."""
        import dataraum.core.logging as log_mod

        mock_console = MagicMock(spec=Console)
        monkeypatch.setattr(log_mod, "_active_console", mock_console)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(
            event="phase.done",
            level="info",
            timestamp="2026-03-04T20:10:29Z",
            phase="typing",
        )

        text_obj: Text = mock_console.print.call_args[0][0]
        assert "2026" not in text_obj.plain

    def test_deactivate_restores_stderr(self, monkeypatch: object) -> None:
        """After deactivation, output goes back to stderr."""
        import dataraum.core.logging as log_mod

        mock_console = MagicMock(spec=Console)
        monkeypatch.setattr(log_mod, "_active_console", mock_console)  # type: ignore[attr-defined]

        deactivate_console()

        buf = StringIO()
        monkeypatch.setattr(sys, "stderr", buf)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="back_to_stderr", level="info")

        assert "back_to_stderr" in buf.getvalue()
        mock_console.print.assert_not_called()

    def test_stderr_hides_info_level(self, monkeypatch: object) -> None:
        """Info level is not shown in stderr output (only warning/error)."""
        import dataraum.core.logging as log_mod

        buf = StringIO()
        monkeypatch.setattr(sys, "stderr", buf)  # type: ignore[attr-defined]
        monkeypatch.setattr(log_mod, "_active_console", None)  # type: ignore[attr-defined]

        proxy = _ProxyLogger()
        proxy.msg(event="phase.done", level="info", phase="typing")

        assert "[info]" not in buf.getvalue()

    def test_stderr_shows_warning_level(self, monkeypatch: object) -> None:
        """Warning level IS shown in stderr output."""
        import dataraum.core.logging as log_mod

        buf = StringIO()
        monkeypatch.setattr(sys, "stderr", buf)  # type: ignore[attr-defined]
        monkeypatch.setattr(log_mod, "_active_console", None)  # type: ignore[attr-defined]

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
            "fatal",
            "failure",
            "err",
            "error",
            "critical",
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
