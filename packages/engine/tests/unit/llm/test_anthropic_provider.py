"""Tests for AnthropicProvider error classification (DAT-503 typed channel).

Hard-fail policy: ``converse`` RAISES a typed provider exception on an API
failure — :class:`TransientProviderError` (retry may help) vs
:class:`PermanentProviderError` (user must fix) — so retryability rides the
exception *type* to the worker's durable boundary instead of a substring of a
Result.error every intermediate layer would have to forward faithfully.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import anthropic
import httpx
import pytest

from dataraum.llm.providers.anthropic import (
    AnthropicConfig,
    AnthropicProvider,
    _classify_anthropic_error,
)
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    PermanentProviderError,
    ToolDefinition,
    TransientProviderError,
)


def _config() -> AnthropicConfig:
    return AnthropicConfig(
        default_model="claude-x",
        models={"fast": "claude-x", "balanced": "claude-x"},
    )


def _provider() -> AnthropicProvider:
    # Key is injected by the factory in production; tests pass it directly so
    # the provider needs no env / settings.
    return AnthropicProvider(_config(), "sk-ant-test")


def _request() -> ConversationRequest:
    return ConversationRequest(messages=[Message(role="user", content="hi")])


def _status_error(status_code: int) -> anthropic.APIStatusError:
    """Build an APIStatusError instance with a given status code."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.headers = {}
    return anthropic.APIStatusError(message=f"http {status_code}", response=response, body=None)


class TestErrorClassification:
    """_classify_anthropic_error sorts SDK exceptions into typed provider errors."""

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 413, 422])
    def test_permanent_status_codes(self, status: int) -> None:
        assert isinstance(_classify_anthropic_error(_status_error(status)), PermanentProviderError)

    @pytest.mark.parametrize("status", [408, 409, 429, 500, 502, 503, 529])
    def test_transient_status_codes(self, status: int) -> None:
        assert isinstance(_classify_anthropic_error(_status_error(status)), TransientProviderError)

    def test_timeout_is_transient(self) -> None:
        exc = anthropic.APITimeoutError(request=MagicMock(spec=httpx.Request))
        assert isinstance(_classify_anthropic_error(exc), TransientProviderError)

    def test_connection_error_is_transient(self) -> None:
        exc = anthropic.APIConnectionError(request=MagicMock(spec=httpx.Request))
        assert isinstance(_classify_anthropic_error(exc), TransientProviderError)

    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_errors_hint_at_api_key(self, status: int) -> None:
        """401/403 errors must mention ANTHROPIC_API_KEY so the practitioner
        knows what to fix."""
        err = _classify_anthropic_error(_status_error(status))
        assert isinstance(err, PermanentProviderError)
        assert "ANTHROPIC_API_KEY" in str(err)


class TestConverseRaisesTypedError:
    """converse raises the typed exception so callers don't inspect the SDK
    exception — and so a transient failure stays retryable end-to-end."""

    def test_permanent_error_raises_permanent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()

        def boom(**_: object) -> object:
            raise _status_error(401)

        monkeypatch.setattr(provider.client.messages, "create", boom)

        with pytest.raises(PermanentProviderError) as ei:
            provider.converse(_request())
        assert "ANTHROPIC_API_KEY" in str(ei.value)

    def test_transient_error_raises_transient(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()

        def boom(**_: object) -> object:
            raise _status_error(529)

        monkeypatch.setattr(provider.client.messages, "create", boom)

        with pytest.raises(TransientProviderError):
            provider.converse(_request())

    def test_unexpected_error_is_permanent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A non-API failure (a bug in request shaping) is non-retryable — retrying
        # the identical call cannot clear it.
        provider = _provider()

        def boom(**_: object) -> object:
            raise ValueError("malformed kwargs")

        monkeypatch.setattr(provider.client.messages, "create", boom)

        with pytest.raises(PermanentProviderError) as ei:
            provider.converse(_request())
        assert "malformed kwargs" in str(ei.value)

    def test_transient_chains_the_sdk_cause(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The original SDK exception is preserved as __cause__ so the workflow's
        # _failure_message __cause__ walk can surface the innermost message.
        provider = _provider()
        original = _status_error(529)

        def boom(**_: object) -> object:
            raise original

        monkeypatch.setattr(provider.client.messages, "create", boom)

        with pytest.raises(TransientProviderError) as ei:
            provider.converse(_request())
        assert ei.value.__cause__ is original


def _ok_response(
    *,
    input_tokens: int = 100,
    output_tokens: int = 20,
    cache_read: int | None = 0,
    cache_creation: int | None = 0,
) -> SimpleNamespace:
    """A minimal stand-in for the SDK Message a successful create() returns.

    ``cache_read``/``cache_creation`` default to 0 but accept ``None`` to mirror
    the SDK, which leaves those Usage fields unset when no ``cache_control`` is
    in play (the engine today, until DAT-601).
    """
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text="hello")],
        stop_reason="end_turn",
        model="claude-x",
        usage=SimpleNamespace(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_input_tokens=cache_read,
            cache_creation_input_tokens=cache_creation,
        ),
    )


class TestConverseTelemetry:
    """converse emits per-call latency + token telemetry (DAT-600) and surfaces
    the cache-usage fields on the response."""

    def test_logs_label_and_all_token_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()
        monkeypatch.setattr(
            provider.client.messages,
            "create",
            lambda **_: _ok_response(
                input_tokens=512, output_tokens=64, cache_read=480, cache_creation=32
            ),
        )
        log = MagicMock()
        monkeypatch.setattr("dataraum.llm.providers.anthropic.logger", log)

        request = ConversationRequest(
            messages=[Message(role="user", content="hi")], label="graph_sql_generation"
        )
        provider.converse(request).unwrap()

        log.info.assert_called_once()
        event, kwargs = log.info.call_args.args[0], log.info.call_args.kwargs
        assert event == "llm_call"
        assert kwargs["label"] == "graph_sql_generation"
        assert kwargs["model"] == "claude-x"
        assert isinstance(kwargs["elapsed_ms"], int) and kwargs["elapsed_ms"] >= 0
        assert kwargs["input_tokens"] == 512
        assert kwargs["output_tokens"] == 64
        assert kwargs["cache_read_input_tokens"] == 480
        assert kwargs["cache_creation_input_tokens"] == 32

    def test_response_carries_cache_usage(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()
        monkeypatch.setattr(
            provider.client.messages,
            "create",
            lambda **_: _ok_response(cache_read=480, cache_creation=32),
        )

        resp = provider.converse(_request()).unwrap()

        assert resp.cache_read_input_tokens == 480
        assert resp.cache_creation_input_tokens == 32

    def test_missing_cache_fields_coerce_to_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # No cache_control today → SDK leaves the Usage cache fields as None;
        # telemetry must stay numeric, not propagate None.
        provider = _provider()
        monkeypatch.setattr(
            provider.client.messages,
            "create",
            lambda **_: _ok_response(cache_read=None, cache_creation=None),
        )
        log = MagicMock()
        monkeypatch.setattr("dataraum.llm.providers.anthropic.logger", log)

        resp = provider.converse(_request()).unwrap()

        assert resp.cache_read_input_tokens == 0
        assert resp.cache_creation_input_tokens == 0
        assert log.info.call_args.kwargs["cache_read_input_tokens"] == 0
        assert log.info.call_args.kwargs["cache_creation_input_tokens"] == 0


def _tool() -> ToolDefinition:
    return ToolDefinition(name="t", description="d", input_schema={"type": "object"})


class TestPromptCaching:
    """request.cache (DAT-601) marks the stable tools+system prefix with one
    cache_control breakpoint; default-off leaves the request untouched."""

    def _capture(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
        provider = _provider()
        captured: dict[str, object] = {}

        def capture(**kwargs: object) -> object:
            captured.update(kwargs)
            return _ok_response()

        monkeypatch.setattr(provider.client.messages, "create", capture)
        self._provider = provider
        return captured

    def test_cache_off_sends_plain_system_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured = self._capture(monkeypatch)
        self._provider.converse(
            ConversationRequest(messages=[Message(role="user", content="hi")], system="sys")
        )
        assert captured["system"] == "sys"  # plain string, no cache_control wrapping

    def test_cache_on_wraps_system_and_leaves_tools(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured = self._capture(monkeypatch)
        self._provider.converse(
            ConversationRequest(
                messages=[Message(role="user", content="hi")],
                system="sys",
                tools=[_tool()],
                cache=True,
            )
        )
        system = captured["system"]
        assert isinstance(system, list)
        assert system[0]["text"] == "sys"
        assert system[0]["cache_control"] == {"type": "ephemeral"}
        # A system breakpoint already caches the tools prefix — tools stay unmarked.
        tools = captured["tools"]
        assert isinstance(tools, list)
        assert "cache_control" not in tools[-1]

    def test_cache_on_without_system_marks_last_tool(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured = self._capture(monkeypatch)
        self._provider.converse(
            ConversationRequest(
                messages=[Message(role="user", content="hi")],
                tools=[_tool(), _tool()],
                cache=True,
            )
        )
        tools = captured["tools"]
        assert isinstance(tools, list)
        assert tools[-1]["cache_control"] == {"type": "ephemeral"}
        assert "cache_control" not in tools[0]
