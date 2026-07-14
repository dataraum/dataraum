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

    def _capture(
        self, monkeypatch: pytest.MonkeyPatch, model: str, *, thinking: bool = False
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
                temperature=0.0,
                model=model,
                thinking=thinking,
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

    def test_thinking_request_sends_explicit_adaptive(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # DAT-603 review fix: EXPLICIT adaptive, never the model default —
        # defaults differ across the family (Sonnet 5 ON, Opus 4.7/4.8 OFF),
        # so an omitted key would silently lose thinking on an Opus tier.
        kwargs = self._capture(monkeypatch, "claude-sonnet-5", thinking=True)
        assert "temperature" not in kwargs
        assert kwargs["thinking"] == {"type": "adaptive"}

    def test_thinking_request_explicit_adaptive_on_opus(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Opus 4.7/4.8 default thinking OFF — relying on the default here would
        # run a "thinking" feature without thinking, silently.
        kwargs = self._capture(monkeypatch, "claude-opus-4-8", thinking=True)
        assert kwargs["thinking"] == {"type": "adaptive"}

    def test_thinking_with_forced_tool_choice_fails_loud(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A forced tool_choice silently SUPPRESSES thinking on the live API
        # (probed 2026-07-03) — a call-site programming error, surfaced as the
        # TYPED permanent error so the Temporal retry policy fails it on
        # attempt 1 (a bare ValueError would be retried 8x, DAT-503).
        provider = _provider()
        _patch_stream(monkeypatch, provider, lambda **kwargs: _ok_response())
        with pytest.raises(PermanentProviderError, match="suppresses thinking"):
            provider.converse(
                ConversationRequest(
                    messages=[Message(role="user", content="hi")],
                    model="claude-sonnet-5",
                    thinking=True,
                    tool_choice={"type": "tool", "name": "generate_sql"},
                )
            )


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
            content=[SimpleNamespace(type="tool_use", id="t1", name="emit", input=tool_input)],
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

    def test_whole_payload_stringified_into_field(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Smoke #6: the model serialized the ENTIRE input object into the
        # container field — the parsed dict's keys match the tool's own
        # properties, so it is adopted as the whole input.
        coerced = self._converse_with_tool_use(
            monkeypatch,
            {"tables": '{"tables": [{"table_name": "t"}], "note": "n"}'},
        )
        assert coerced == {"tables": [{"table_name": "t"}], "note": "n"}

    def test_tool_name_envelope_is_unwrapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Sonnet 5 under a forced tool_choice intermittently wraps the WHOLE
        # argument object under a single key equal to the TOOL NAME
        # ({"emit": {<real args>}}). Left alone, schema-validation at the call
        # site finds no known field and silently defaults every field to empty
        # (the slicing "0 recommendations" flake). Unwrap it.
        coerced = self._converse_with_tool_use(
            monkeypatch, {"emit": {"tables": [{"table_name": "t"}], "note": "ok"}}
        )
        assert coerced == {"tables": [{"table_name": "t"}], "note": "ok"}

    def test_envelope_unwrap_then_coerces_inner(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Unwrap composes with stringified-arg coercion: an enveloped inner whose
        # container was itself stringified is still parsed.
        coerced = self._converse_with_tool_use(
            monkeypatch, {"emit": {"tables": '[{"table_name": "t"}]', "note": "ok"}}
        )
        assert coerced == {"tables": [{"table_name": "t"}], "note": "ok"}

    def test_single_real_property_key_not_unwrapped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A single-key input whose sole key is a real PROPERTY (not the tool name)
        # is NOT an envelope — leave it (the list value passes coercion untouched).
        coerced = self._converse_with_tool_use(
            monkeypatch, {"tables": [{"table_name": "t"}]}
        )
        assert coerced == {"tables": [{"table_name": "t"}]}


class TestPromptCaching:
    """The system prompt ships as a cached block (DAT-601): tools render before
    system, so the one breakpoint caches tools + system across a run's repeated
    calls. Volatile content rides in the user message, past the breakpoint."""

    def test_system_becomes_cached_block(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()
        captured: dict[str, object] = {}

        def capture(**kwargs: object) -> object:
            captured.update(kwargs)
            return _ok_response()

        _patch_stream(monkeypatch, provider, capture)
        provider.converse(
            ConversationRequest(messages=[Message(role="user", content="hi")], system="be terse")
        ).unwrap()
        assert captured["system"] == [
            {
                "type": "text",
                "text": "be terse",
                "cache_control": {"type": "ephemeral"},
            }
        ]


class TestEffort:
    """Per-feature effort (DAT-603) reaches the API as output_config.effort —
    only on models that accept the parameter (Haiku 4.5 rejects it)."""

    def _capture_kwargs(
        self, monkeypatch: pytest.MonkeyPatch, model: str, effort: str | None
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
                model=model,
                effort=effort,
            )
        ).unwrap()
        return captured

    def test_effort_sent_on_supporting_model(self, monkeypatch: pytest.MonkeyPatch) -> None:
        kwargs = self._capture_kwargs(monkeypatch, "claude-sonnet-5", "low")
        assert kwargs["output_config"] == {"effort": "low"}

    def test_effort_dropped_on_haiku(self, monkeypatch: pytest.MonkeyPatch) -> None:
        kwargs = self._capture_kwargs(monkeypatch, "claude-haiku-4-5", "low")
        assert "output_config" not in kwargs

    def test_no_effort_no_output_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        kwargs = self._capture_kwargs(monkeypatch, "claude-sonnet-5", None)
        assert "output_config" not in kwargs


class TestRawContentRoundTrip:
    """Thinking-block continuation plumbing (DAT-699): converse captures the
    turn's blocks VERBATIM in raw_content, and _convert_messages echoes an
    assistant turn's raw_content unchanged — the live API rejects a continued
    conversation whose assistant turn lost its signed thinking blocks."""

    def _fake_message(self) -> SimpleNamespace:
        usage = SimpleNamespace(
            input_tokens=10,
            output_tokens=5,
            cache_read_input_tokens=None,
            cache_creation_input_tokens=None,
        )
        blocks = [
            SimpleNamespace(type="thinking", thinking="chain", signature="sig-1"),
            SimpleNamespace(type="text", text="here"),
            SimpleNamespace(
                type="tool_use", id="tu-1", name="search_values", input={"pattern": "tax"}
            ),
            SimpleNamespace(type="redacted_thinking", data="opaque"),
        ]
        return SimpleNamespace(
            content=blocks, usage=usage, stop_reason="tool_use", model="claude-test"
        )

    def test_converse_captures_blocks_verbatim(self, monkeypatch: pytest.MonkeyPatch) -> None:
        provider = _provider()
        message = self._fake_message()
        _patch_stream(monkeypatch, provider, lambda **_: message)

        response = provider.converse(_request()).unwrap()

        assert response.raw_content == [
            {"type": "thinking", "thinking": "chain", "signature": "sig-1"},
            {"type": "text", "text": "here"},
            {
                "type": "tool_use",
                "id": "tu-1",
                "name": "search_values",
                "input": {"pattern": "tax"},
            },
            {"type": "redacted_thinking", "data": "opaque"},
        ]
        assert [tc.name for tc in response.tool_calls] == ["search_values"]

    def test_assistant_raw_content_echoes_unchanged(self) -> None:
        provider = _provider()
        raw = [
            {"type": "thinking", "thinking": "chain", "signature": "sig-1"},
            {"type": "tool_use", "id": "tu-1", "name": "search_values", "input": {"p": 1}},
        ]
        converted = provider._convert_messages(
            [Message(role="assistant", content="ignored-when-raw", raw_content=raw)]
        )

        assert converted == [{"role": "assistant", "content": raw}]

    def test_assistant_without_raw_content_rebuilds_from_fields(self) -> None:
        provider = _provider()
        converted = provider._convert_messages([Message(role="assistant", content="plain")])

        assert converted == [{"role": "assistant", "content": [{"type": "text", "text": "plain"}]}]


class TestConverseSpans:
    """converse wraps the client call in a GenAI-semconv span (DAT-706).

    Current conventions (semantic-conventions-genai repo): name
    "chat {request model}", kind CLIENT, `gen_ai.provider.name` (NOT the
    retired `gen_ai.system`), usage attrs incl. the cache split, and
    `error.type` on failure. `dataraum.call_site` mirrors the cockpit
    enricher's key so call-site attribution is one cross-stack query.
    """

    def _capture(self, monkeypatch: pytest.MonkeyPatch) -> object:
        """Route the module tracer through an in-memory exporter."""
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        import dataraum.llm.providers.anthropic as module

        exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        monkeypatch.setattr(module, "tracer", tracer_provider.get_tracer("test"))
        return exporter

    def test_success_span_carries_semconv_attributes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from opentelemetry.trace import SpanKind

        exporter = self._capture(monkeypatch)
        provider = _provider()
        _patch_stream(monkeypatch, provider, lambda **_: _ok_response())

        result = provider.converse(
            ConversationRequest(
                messages=[Message(role="user", content="hi")],
                model="claude-sonnet-5",
                effort="low",
                label="graph_sql_generation",
            )
        )
        assert result.success

        (span,) = exporter.get_finished_spans()  # type: ignore[attr-defined]
        assert span.name == "chat claude-sonnet-5"
        assert span.kind == SpanKind.CLIENT
        attrs = dict(span.attributes)
        assert attrs["gen_ai.operation.name"] == "chat"
        assert attrs["gen_ai.provider.name"] == "anthropic"
        assert attrs["gen_ai.request.model"] == "claude-sonnet-5"
        assert attrs["gen_ai.request.max_tokens"] == 4096
        assert attrs["gen_ai.request.stream"] is True
        # Sonnet 5 rejects temperature -> omitted from request AND span;
        # it supports effort -> reasoning level recorded.
        assert "gen_ai.request.temperature" not in attrs
        assert attrs["gen_ai.request.reasoning.level"] == "low"
        assert attrs["dataraum.call_site"] == "graph_sql_generation"
        assert attrs["gen_ai.response.model"] == "claude-x"
        assert attrs["gen_ai.response.finish_reasons"] == ("end_turn",)
        assert attrs["gen_ai.usage.input_tokens"] == 100
        assert attrs["gen_ai.usage.output_tokens"] == 20
        assert attrs["gen_ai.usage.cache_read.input_tokens"] == 0
        assert attrs["gen_ai.usage.cache_creation.input_tokens"] == 0
        # The provider discriminator is the CURRENT semconv key only.
        assert "gen_ai.system" not in attrs

    def test_temperature_recorded_when_sent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        exporter = self._capture(monkeypatch)
        provider = _provider()
        _patch_stream(monkeypatch, provider, lambda **_: _ok_response())

        provider.converse(_request())  # default model claude-x keeps temperature

        (span,) = exporter.get_finished_spans()  # type: ignore[attr-defined]
        attrs = dict(span.attributes)
        assert attrs["gen_ai.request.temperature"] == 0.0
        # No label on the request -> no call-site attribute.
        assert "dataraum.call_site" not in attrs

    def test_error_span_records_error_type(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from opentelemetry.trace import StatusCode

        exporter = self._capture(monkeypatch)
        provider = _provider()

        def boom(**_: object) -> object:
            raise _status_error(529)

        _patch_stream(monkeypatch, provider, boom)

        with pytest.raises(TransientProviderError):
            provider.converse(_request())

        (span,) = exporter.get_finished_spans()  # type: ignore[attr-defined]
        attrs = dict(span.attributes)
        assert attrs["error.type"] == "APIStatusError"
        assert span.status.status_code == StatusCode.ERROR
        assert any(e.name == "exception" for e in span.events)
