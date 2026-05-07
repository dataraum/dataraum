"""Tests for AnthropicProvider error classification.

Hard-fail policy: callers should be able to distinguish transient
(retry might help) from permanent (user must fix) errors purely from
the Result.fail message.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import anthropic
import httpx
import pytest

from dataraum.llm.providers.anthropic import (
    AnthropicConfig,
    AnthropicProvider,
    _classify_anthropic_error,
)
from dataraum.llm.providers.base import ConversationRequest, Message


def _config() -> AnthropicConfig:
    return AnthropicConfig(
        api_key_env="ANTHROPIC_API_KEY",
        default_model="claude-x",
        models={"fast": "claude-x", "balanced": "claude-x"},
    )


def _provider(monkeypatch: pytest.MonkeyPatch) -> AnthropicProvider:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    return AnthropicProvider(_config())


def _request() -> ConversationRequest:
    return ConversationRequest(messages=[Message(role="user", content="hi")])


def _status_error(status_code: int) -> anthropic.APIStatusError:
    """Build an APIStatusError instance with a given status code."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.headers = {}
    return anthropic.APIStatusError(message=f"http {status_code}", response=response, body=None)


class TestErrorClassification:
    """_classify_anthropic_error sorts SDK exceptions into transient/permanent."""

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 413, 422])
    def test_permanent_status_codes(self, status: int) -> None:
        kind, _ = _classify_anthropic_error(_status_error(status))
        assert kind == "permanent"

    @pytest.mark.parametrize("status", [408, 409, 429, 500, 502, 503, 529])
    def test_transient_status_codes(self, status: int) -> None:
        kind, _ = _classify_anthropic_error(_status_error(status))
        assert kind == "transient"

    def test_timeout_is_transient(self) -> None:
        exc = anthropic.APITimeoutError(request=MagicMock(spec=httpx.Request))
        kind, _ = _classify_anthropic_error(exc)
        assert kind == "transient"

    def test_connection_error_is_transient(self) -> None:
        exc = anthropic.APIConnectionError(request=MagicMock(spec=httpx.Request))
        kind, _ = _classify_anthropic_error(exc)
        assert kind == "transient"

    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_errors_hint_at_api_key(self, status: int) -> None:
        """401/403 errors must mention ANTHROPIC_API_KEY so the practitioner
        knows what to fix."""
        kind, message = _classify_anthropic_error(_status_error(status))
        assert kind == "permanent"
        assert "ANTHROPIC_API_KEY" in message


class TestConverseSurfacesClassification:
    """Result.fail messages encode the error kind so callers don't need to
    inspect the original exception."""

    def test_permanent_error_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider(monkeypatch)

        def boom(**_: object) -> object:
            raise _status_error(401)

        monkeypatch.setattr(provider.client.messages, "create", boom)

        result = provider.converse(_request())
        assert not result.success
        assert "permanent" in (result.error or "")

    def test_transient_error_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider(monkeypatch)

        def boom(**_: object) -> object:
            raise _status_error(529)

        monkeypatch.setattr(provider.client.messages, "create", boom)

        result = provider.converse(_request())
        assert not result.success
        assert "transient" in (result.error or "")
