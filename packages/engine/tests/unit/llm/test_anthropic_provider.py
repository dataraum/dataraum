"""Tests for AnthropicProvider error classification (DAT-503 typed channel).

Hard-fail policy: ``converse`` RAISES a typed provider exception on an API
failure — :class:`TransientProviderError` (retry may help) vs
:class:`PermanentProviderError` (user must fix) — so retryability rides the
exception *type* to the worker's durable boundary instead of a substring of a
Result.error every intermediate layer would have to forward faithfully.
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
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    PermanentProviderError,
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
