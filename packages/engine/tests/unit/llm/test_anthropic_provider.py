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


class _FakeStream:
    """Stand-in for the SDK ``MessageStreamManager``: a context manager whose
    body yields an object with ``get_final_message()``. ``converse`` streams
    (the SDK refuses large-``max_tokens`` non-streaming requests), so tests
    patch ``messages.stream`` — the fake calls through to ``fn`` on entry, so
    a raising ``fn`` surfaces exactly where the SDK would raise."""

    def __init__(self, fn: object, kwargs: dict[str, object]) -> None:
        self._fn = fn
        self._kwargs = kwargs

    def __enter__(self) -> SimpleNamespace:
        response = self._fn(**self._kwargs)  # type: ignore[operator]
        return SimpleNamespace(get_final_message=lambda: response)

    def __exit__(self, *exc: object) -> bool:
        return False


def _patch_stream(monkeypatch: pytest.MonkeyPatch, provider: object, fn: object) -> None:
    """Route ``provider.client.messages.stream(**kw)`` through ``fn``."""
    monkeypatch.setattr(
        provider.client.messages,  # type: ignore[attr-defined]
        "stream",
        lambda **kw: _FakeStream(fn, kw),
    )


class TestConverseRaisesTypedError:
    """converse raises the typed exception so callers don't inspect the SDK
    exception — and so a transient failure stays retryable end-to-end."""

    def test_permanent_error_raises_permanent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()

        def boom(**_: object) -> object:
            raise _status_error(401)

        _patch_stream(monkeypatch, provider, boom)

        with pytest.raises(PermanentProviderError) as ei:
            provider.converse(_request())
        assert "ANTHROPIC_API_KEY" in str(ei.value)

    def test_transient_error_raises_transient(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()

        def boom(**_: object) -> object:
            raise _status_error(529)

        _patch_stream(monkeypatch, provider, boom)

        with pytest.raises(TransientProviderError):
            provider.converse(_request())

    def test_unexpected_error_is_permanent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A non-API failure (a bug in request shaping) is non-retryable — retrying
        # the identical call cannot clear it.
        provider = _provider()

        def boom(**_: object) -> object:
            raise ValueError("malformed kwargs")

        _patch_stream(monkeypatch, provider, boom)

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

        _patch_stream(monkeypatch, provider, boom)

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
    """A minimal stand-in for the final SDK Message a successful stream yields.

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


class TestConverseRequestShape:
    """converse shapes the request per the model's sampling/thinking contract.

    Sonnet 5 / Opus 4.7-4.8 / Fable 5 reject a non-default ``temperature`` (400)
    and default adaptive thinking ON; the engine's forced-tool extraction tier
    wants neither. Older models keep the temperature passthrough and no thinking
    param. These assert the exact kwargs handed to ``messages.create`` — the gap
    that let the Sonnet 5 swap ship a request that 400s against the live API.
    """

    def _capture(self, monkeypatch: pytest.MonkeyPatch, model: str) -> dict[str, object]:
        provider = _provider()
        captured: dict[str, object] = {}

        def capture(**kwargs: object) -> object:
            captured.update(kwargs)
            return _ok_response()

        _patch_stream(monkeypatch, provider, capture)
        provider.converse(
            ConversationRequest(
                messages=[Message(role="user", content="hi")], temperature=0.0, model=model
            )
        ).unwrap()
        return captured

    def test_sonnet_5_omits_temperature_and_disables_thinking(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        kwargs = self._capture(monkeypatch, "claude-sonnet-5")
        assert "temperature" not in kwargs
        assert kwargs["thinking"] == {"type": "disabled"}

    def test_opus_4_8_omits_temperature_and_disables_thinking(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        kwargs = self._capture(monkeypatch, "claude-opus-4-8")
        assert "temperature" not in kwargs
        assert kwargs["thinking"] == {"type": "disabled"}

    def test_fable_5_omits_temperature_but_leaves_thinking_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Fable 5 rejects a non-default temperature AND rejects an explicit
        # thinking:disabled (always-on) — so we omit both and let it default.
        kwargs = self._capture(monkeypatch, "claude-fable-5")
        assert "temperature" not in kwargs
        assert "thinking" not in kwargs

    def test_older_model_keeps_temperature_and_no_thinking(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Haiku 4.5 / Sonnet 4.6 accept temperature and default thinking off.
        kwargs = self._capture(monkeypatch, "claude-haiku-4-5")
        assert kwargs["temperature"] == 0.0
        assert "thinking" not in kwargs


class TestConverseTelemetry:
    """converse emits per-call latency + token telemetry (DAT-600) and surfaces
    the cache-usage fields on the response."""

    def test_logs_label_and_all_token_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()
        _patch_stream(
            monkeypatch,
            provider,
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
        _patch_stream(
            monkeypatch,
            provider,
            lambda **_: _ok_response(cache_read=480, cache_creation=32),
        )

        resp = provider.converse(_request()).unwrap()

        assert resp.cache_read_input_tokens == 480
        assert resp.cache_creation_input_tokens == 32

    def test_missing_cache_fields_coerce_to_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # No cache_control today → SDK leaves the Usage cache fields as None;
        # telemetry must stay numeric, not propagate None.
        provider = _provider()
        _patch_stream(
            monkeypatch,
            provider,
            lambda **_: _ok_response(cache_read=None, cache_creation=None),
        )
        log = MagicMock()
        monkeypatch.setattr("dataraum.llm.providers.anthropic.logger", log)

        resp = provider.converse(_request()).unwrap()

        assert resp.cache_read_input_tokens == 0
        assert resp.cache_creation_input_tokens == 0
        assert log.info.call_args.kwargs["cache_read_input_tokens"] == 0
        assert log.info.call_args.kwargs["cache_creation_input_tokens"] == 0


class TestStrictTools:
    """Forced tools ship strict:true with a normalized schema (DAT-661): the
    API then guarantees the arguments validate — killing the malformed-args
    class (Sonnet 5 stringified a whole payload into one field, 2026-07-02
    smoke). Open-map / oversized schemas opt out via ``strict=False``."""

    def _captured_tool(
        self, monkeypatch: pytest.MonkeyPatch, tool: ToolDefinition
    ) -> dict[str, object]:
        provider = _provider()
        captured: dict[str, object] = {}

        def capture(**kwargs: object) -> object:
            captured.update(kwargs)
            return _ok_response()

        _patch_stream(monkeypatch, provider, capture)
        provider.converse(
            ConversationRequest(
                messages=[Message(role="user", content="hi")],
                tools=[tool],
            )
        ).unwrap()
        return captured["tools"][0]  # type: ignore[index,no-any-return]

    def test_strict_tool_normalizes_schema_and_sets_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        schema = {
            "type": "object",
            "properties": {
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "nested": {
                    "type": "object",
                    "properties": {"name": {"type": "string", "maxLength": 5}},
                },
            },
            "required": ["confidence"],
        }
        sent = self._captured_tool(
            monkeypatch,
            ToolDefinition(name="t", description="d", input_schema=schema, strict=True),
        )
        assert sent["strict"] is True
        sent_schema = sent["input_schema"]
        assert sent_schema["additionalProperties"] is False
        assert "minimum" not in sent_schema["properties"]["confidence"]
        assert "maximum" not in sent_schema["properties"]["confidence"]
        nested = sent_schema["properties"]["nested"]
        assert nested["additionalProperties"] is False
        assert "maxLength" not in nested["properties"]["name"]

    def test_non_strict_tool_passes_schema_verbatim(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Open-map schema (additionalProperties: <schema>) — must go through
        # untouched, with no strict flag.
        schema = {
            "type": "object",
            "properties": {"steps": {"type": "object", "additionalProperties": {"type": "string"}}},
        }
        sent = self._captured_tool(
            monkeypatch,
            ToolDefinition(name="t", description="d", input_schema=schema),
        )
        assert "strict" not in sent
        assert sent["input_schema"] == schema


class TestStringifiedArgCoercion:
    """The provider repairs JSON-stringified container arguments against the
    declared schema (Sonnet 5 stringified a whole payload into one field —
    2026-07-02 smoke) and leaves everything else untouched."""

    def _converse_with_tool_use(
        self, monkeypatch: pytest.MonkeyPatch, tool_input: dict[str, object]
    ) -> dict[str, object]:
        provider = _provider()
        response = SimpleNamespace(
            content=[
                SimpleNamespace(type="tool_use", id="t1", name="emit", input=tool_input)
            ],
            stop_reason="tool_use",
            model="claude-x",
            usage=SimpleNamespace(
                input_tokens=1,
                output_tokens=1,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
            ),
        )
        _patch_stream(monkeypatch, provider, lambda **_: response)
        result = provider.converse(
            ConversationRequest(
                messages=[Message(role="user", content="hi")],
                tools=[
                    ToolDefinition(
                        name="emit",
                        description="d",
                        input_schema={
                            "type": "object",
                            "properties": {
                                "tables": {"type": "array"},
                                "note": {"type": "string"},
                            },
                        },
                    )
                ],
            )
        ).unwrap()
        return result.tool_calls[0].input

    def test_stringified_array_is_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        coerced = self._converse_with_tool_use(
            monkeypatch, {"tables": '[{"table_name": "t"}]', "note": "ok"}
        )
        assert coerced["tables"] == [{"table_name": "t"}]
        assert coerced["note"] == "ok"

    def test_plain_string_field_untouched(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A string field that happens to contain JSON must NOT be parsed —
        # only schema-declared containers are coerced.
        coerced = self._converse_with_tool_use(monkeypatch, {"note": '["not a list field"]'})
        assert coerced["note"] == '["not a list field"]'

    def test_unparseable_string_left_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        coerced = self._converse_with_tool_use(monkeypatch, {"tables": "not json"})
        assert coerced["tables"] == "not json"

    def test_whole_payload_stringified_into_field(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Smoke #6: the model serialized the ENTIRE input object into the
        # container field — the parsed dict's keys match the tool's own
        # properties, so it is adopted as the whole input.
        coerced = self._converse_with_tool_use(
            monkeypatch,
            {"tables": '{"tables": [{"table_name": "t"}], "note": "n"}'},
        )
        assert coerced == {"tables": [{"table_name": "t"}], "note": "n"}
