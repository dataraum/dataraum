"""Anthropic Claude provider implementation."""

from __future__ import annotations

import json
import time
from typing import Any, cast

import anthropic
from anthropic.types import MessageParam, ToolParam, ToolResultBlockParam, ToolUseBlockParam
from pydantic import BaseModel

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.providers.base import (
    ConversationRequest,
    ConversationResponse,
    LLMProvider,
    Message,
    PermanentProviderError,
    ProviderError,
    ToolCall,
    ToolResult,
    TransientProviderError,
)


class AnthropicConfig(BaseModel):
    """Configuration for Anthropic provider."""

    default_model: str
    models: dict[str, str]


logger = get_logger(__name__)


# --- Model request-shape capabilities (Claude 4.7+ / Sonnet 5 / Fable 5) ---
#
# The engine is the structured-extraction tier (docs/architecture/orchestration.md): every call forces a
# tool for typed output and wants determinism, not agentic reasoning. Two
# request-shape changes landed with this model generation that the tier must
# honour, or Sonnet 5 rejects the call outright:
#
#   * Non-default sampling params (``temperature``/``top_p``/``top_k``) return a
#     400. Our prompt templates ask for temperature 0.0-0.1 for determinism, so
#     on these models we OMIT ``temperature`` and rely on the forced tool +
#     prompt for stable output (temperature 0 never guaranteed identical output
#     anyway).
#   * Thinking defaults DIFFER across the family: Sonnet 5 runs adaptive
#     thinking ON when ``thinking`` is omitted; Opus 4.7/4.8 default it OFF.
#     A forced-tool extractor never wants it (it burns output budget the
#     small-cap calls can't spare, and it diverges from the prior Sonnet 4.6
#     behaviour), so the default path DISABLES it explicitly where a default-on
#     model would otherwise think. A thinking feature (request.thinking) sends
#     an EXPLICIT ``{"type": "adaptive"}`` instead of trusting any default.
#
# Older models (Haiku 4.5, Sonnet 4.6) accept temperature and default thinking
# off, so their request shape is unchanged. Prefix match covers the undated
# aliases and any dated snapshot.
_TEMPERATURE_REJECTING_PREFIXES = (
    "claude-sonnet-5",
    "claude-opus-4-7",
    "claude-opus-4-8",
    "claude-fable-5",
    "claude-mythos-5",
)
# Subset that accepts an explicit ``thinking: disabled`` — sent on the
# non-thinking path so a default-on model (Sonnet 5) doesn't think; harmless
# on Opus 4.7/4.8 (already default-off). Fable 5 / Mythos 5 are always-on and
# REJECT an explicit disable, so they are intentionally excluded — the
# engine's forced-tool tier does not target them.
_THINKING_DEFAULT_ON_PREFIXES = (
    "claude-sonnet-5",
    "claude-opus-4-7",
    "claude-opus-4-8",
)


def _rejects_temperature(model: str) -> bool:
    """True when the model 400s on a non-default sampling param."""
    return model.startswith(_TEMPERATURE_REJECTING_PREFIXES)


def _thinking_defaults_on(model: str) -> bool:
    """True when the model accepts an explicit thinking disable (see above)."""
    return model.startswith(_THINKING_DEFAULT_ON_PREFIXES)


# JSON-Schema keywords strict grammar compilation rejects. Stripping them is
# lossless for correctness: Pydantic re-validates the parsed arguments client-
# side, so range/length constraints are still enforced — strict guarantees the
# SHAPE (no stringified payloads, no missing/extra keys), Pydantic the values.
_STRICT_UNSUPPORTED_KEYS = frozenset(
    {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
    }
)


def _strict_tool_schema(node: Any) -> Any:
    """Normalize a Pydantic JSON schema for ``strict: true`` tool use.

    Recursively sets ``additionalProperties: false`` on every object node
    (strict requires it explicitly; Pydantic never emits it) and strips the
    constraint keywords strict rejects (see ``_STRICT_UNSUPPORTED_KEYS``).
    """
    if isinstance(node, dict):
        out = {
            k: _strict_tool_schema(v) for k, v in node.items() if k not in _STRICT_UNSUPPORTED_KEYS
        }
        if out.get("type") == "object" or "properties" in out:
            out.setdefault("additionalProperties", False)
        return out
    if isinstance(node, list):
        return [_strict_tool_schema(v) for v in node]
    return node


# Models that accept ``output_config.effort``. Haiku 4.5 (the ``fast`` tier)
# rejects the parameter, so it is deliberately absent.
_EFFORT_SUPPORTING_PREFIXES = (
    "claude-sonnet-5",
    "claude-sonnet-4-6",
    "claude-opus-4-5",
    "claude-opus-4-6",
    "claude-opus-4-7",
    "claude-opus-4-8",
    "claude-fable-5",
    "claude-mythos-5",
)


def _supports_effort(model: str) -> bool:
    """True when the model accepts ``output_config.effort``."""
    return model.startswith(_EFFORT_SUPPORTING_PREFIXES)


def _coerce_stringified_args(
    tool_input: dict[str, Any], schema: dict[str, Any], *, label: str | None
) -> dict[str, Any]:
    """Parse tool arguments the model JSON-stringified against the schema.

    Sonnet 5 occasionally serializes a whole array/object argument into a JSON
    string (`{"tables": "[{…}]"}` — 2026-07-02 smoke, semantic_per_table died
    on Pydantic list_type). The declared ``input_schema`` says which top-level
    properties must be containers, so this boundary repairs exactly those:
    a ``str`` value where the schema expects ``array``/``object`` is parsed;
    everything else passes through untouched. Coercions are logged so the
    frequency stays observable (the strict alternative is opt-in per tool —
    see ``ToolDefinition.strict``).
    """
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return tool_input
    out = dict(tool_input)
    for key, value in tool_input.items():
        expected = properties.get(key)
        if not isinstance(expected, dict) or not isinstance(value, str):
            continue
        if expected.get("type") not in ("array", "object"):
            continue
        try:
            parsed = json.loads(value)
        except ValueError:
            continue
        # Whole-payload variant (observed in smoke #6): the model serialized
        # the ENTIRE input object into one field — {"tables": '{"tables": […]}'}.
        # If the parsed dict's keys are the tool's own top-level properties,
        # it IS the input; adopt it wholesale.
        if (
            isinstance(parsed, dict)
            and parsed.keys() <= properties.keys()
            and expected.get("type") != "object"
        ):
            logger.warning(
                "stringified_tool_payload_coerced",
                label=label,
                argument=key,
            )
            return parsed
        if isinstance(parsed, (list, dict)):
            out[key] = parsed
            logger.warning(
                "stringified_tool_arg_coerced",
                label=label,
                argument=key,
                expected_type=expected.get("type"),
            )
    return out


# 4xx codes the user must fix — credentials, schema, request shape.
# 429 (rate limit) and 408/409 are retryable so they are NOT in this set.
_PERMANENT_STATUS_CODES = frozenset({400, 401, 403, 404, 413, 422})


_AUTH_STATUS_CODES = frozenset({401, 403})


def _classify_anthropic_error(exc: anthropic.APIError) -> ProviderError:
    """Build the typed :class:`ProviderError` to raise for an Anthropic exception.

    Returns the exception *instance* whose type carries retryability — a
    :class:`TransientProviderError` (retry may help) or
    :class:`PermanentProviderError` (user must fix) — with the human-readable
    body as its message, plus an actionable hint for auth failures so
    practitioners know exactly what to fix.

    Classification:
        permanent — auth / forbidden / bad request / not found / payload
            too large / unprocessable. Retrying won't help; the user
            needs to fix credentials, the input, or configuration.
        transient — rate limits, 5xx, 408 timeout, 409 conflict, network
            errors, connection / read timeouts. The SDK retries these by
            default; if one surfaces here, the retry budget is exhausted
            but a later retry may still succeed.
    """
    # APIStatusError (and subclasses) carry an HTTP status code.
    if isinstance(exc, anthropic.APIStatusError):
        message = str(exc)
        if exc.status_code in _AUTH_STATUS_CODES:
            message = f"{message}. Check your ANTHROPIC_API_KEY."
        if exc.status_code in _PERMANENT_STATUS_CODES:
            return PermanentProviderError(message)
        return TransientProviderError(message)
    # Connection / timeout errors don't have a status code; they're
    # always transient by definition.
    if isinstance(exc, anthropic.APIConnectionError):
        return TransientProviderError(str(exc))
    # APIResponseValidationError: the server returned something the SDK
    # couldn't parse. Treat as permanent — retrying the same request
    # likely produces the same malformed response.
    if isinstance(exc, anthropic.APIResponseValidationError):
        return PermanentProviderError(str(exc))
    # Anything else under APIError — default to transient (retry is
    # the safer default than surfacing as a user error).
    return TransientProviderError(str(exc))


class AnthropicProvider(LLMProvider):
    """Anthropic Claude provider implementation.

    Uses the Anthropic sync client to make API calls to Claude models.
    Supports both JSON and text response formats.
    """

    def __init__(self, config: AnthropicConfig, api_key: str):
        """Initialize Anthropic provider.

        Args:
            config: Provider configuration (models, defaults).
            api_key: Anthropic API key. The factory resolves this from
                typed settings (``settings.anthropic_api_key``) — the
                provider itself stays free of env / settings coupling.

        Raises:
            ImportError: If anthropic package not installed.
            ValueError: If ``api_key`` is empty.
        """
        if anthropic is None:
            raise ImportError(
                "anthropic package not installed. Install with: pip install anthropic"
            )

        if not api_key:
            raise ValueError("Anthropic API key is empty. Set ANTHROPIC_API_KEY.")

        self.config = config

        # Create sync client
        self.client = anthropic.Anthropic(api_key=api_key)

    def get_model_for_tier(self, tier: str) -> str:
        """Get Claude model name for tier.

        Args:
            tier: Model tier ('fast' or 'balanced')

        Returns:
            Model name (e.g., 'claude-sonnet-4-20250514')
        """
        return self.config.models.get(tier, self.config.default_model)

    def converse(self, request: ConversationRequest) -> Result[ConversationResponse]:
        """Send a conversation request with optional tool use.

        Supports multi-turn conversations and tool use with Claude.

        Args:
            request: Conversation request with messages, tools, etc.

        Returns:
            Result containing ConversationResponse or error message
        """
        # Request-shape validation: thinking with a forced tool_choice is a
        # call-site programming error. Probed live (2026-07-03): the first-party
        # API does NOT 400 on the combination — it silently SUPPRESSES thinking
        # (forced choice returned no thinking block; auto did) — worse than an
        # error, the feature quietly stops working. Raise the TYPED permanent
        # error (DAT-503) so the Temporal boundary fails loud on attempt 1: a
        # bare ValueError would not be classified and the _LLM_RETRY policy
        # would retry a deterministic misconfiguration 8x.
        if request.thinking and (request.tool_choice or {}).get("type") in ("tool", "any"):
            raise PermanentProviderError(
                "thinking=True with a forced tool_choice "
                f"({request.tool_choice}) silently suppresses thinking; use "
                '{"type": "auto", "disable_parallel_tool_use": True} and '
                "mandate the tool call in the prompt"
            )
        try:
            model = request.model or self.config.default_model

            # Convert our messages to Anthropic format
            messages = self._convert_messages(request.messages)

            # Convert tools to Anthropic format
            tools: list[ToolParam] | None = None
            if request.tools:
                tools = [
                    cast(
                        ToolParam,
                        {
                            "name": t.name,
                            "description": t.description,
                            "input_schema": _strict_tool_schema(t.input_schema)
                            if t.strict
                            else t.input_schema,
                            **({"strict": True} if t.strict else {}),
                        },
                    )
                    for t in request.tools
                ]

            # Make API call
            kwargs: dict[str, Any] = {
                "model": model,
                "max_tokens": request.max_tokens,
                "messages": messages,
            }

            # Sonnet 5 / Opus 4.7-4.8 / Fable 5 reject a non-default temperature
            # (400). Omit temperature on those; pass it through on the older
            # models that still honour it. Thinking is per-REQUEST (DAT-603):
            # the mechanical extractors run with it explicitly disabled (output
            # budget + Sonnet 4.6 parity); a reasoning-heavy feature (metric
            # grounding) opts in with an EXPLICIT {"type": "adaptive"} — never
            # by relying on the model default, because the defaults DIFFER
            # across the family (Sonnet 5 defaults thinking ON, Opus 4.7/4.8
            # default OFF): an omitted key would silently run a thinking
            # feature without thinking the moment a tier repoints to Opus.
            # Explicit adaptive is accepted by the whole family, including the
            # always-on Fable/Mythos.
            if _rejects_temperature(model):
                if request.thinking:
                    kwargs["thinking"] = {"type": "adaptive"}
                elif _thinking_defaults_on(model):
                    kwargs["thinking"] = {"type": "disabled"}
            else:
                kwargs["temperature"] = request.temperature

            if request.system:
                # DAT-601: cache the stable prefix. Tools render before system,
                # so this one breakpoint caches tools + system together across
                # the run's repeated calls (identical template per phase; the
                # metrics/validation fan-outs repeat it 10-wide). Prefixes under
                # the model's minimum cacheable size silently don't cache — no
                # error, just cache_creation_input_tokens=0 in the telemetry.
                # Volatile per-call data rides in the user message (the
                # render_split contract), never ahead of this breakpoint.
                kwargs["system"] = [
                    {
                        "type": "text",
                        "text": request.system,
                        "cache_control": {"type": "ephemeral"},
                    }
                ]

            if tools:
                kwargs["tools"] = tools

            if request.tool_choice:
                kwargs["tool_choice"] = request.tool_choice

            # DAT-603: per-feature effort. Extraction agents run thinking-off +
            # effort:low (shorter output = lower serial-decode latency). Only
            # sent where the model supports the parameter — Haiku 4.5 rejects
            # it, so the fast tier drops it silently.
            if request.effort and _supports_effort(model):
                kwargs["output_config"] = {"effort": request.effort}

            # Stream + accumulate instead of a one-shot create: the SDK refuses
            # a non-streaming request whose max_tokens it estimates could exceed
            # ~10 minutes (ValueError "Streaming is required…"), and the Sonnet 5
            # output budget (24000) trips that guard — every pipeline call died
            # on it in the 2026-07-02 smoke. Streaming lifts the ceiling; callers
            # still receive one final Message via get_final_message().
            start = time.perf_counter()
            with self.client.messages.stream(**kwargs) as stream:
                response = stream.get_final_message()
            elapsed_ms = round((time.perf_counter() - start) * 1000)

            # Extract content and tool calls from response. raw_content keeps
            # the turn's blocks VERBATIM (incl. signed thinking blocks) so a
            # continued conversation can echo the assistant turn back — the
            # thinking-model API rejects a continuation whose assistant turn
            # lost its thinking blocks (DAT-699).
            text_content = ""
            tool_calls: list[ToolCall] = []
            raw_content: list[dict[str, Any]] = []

            schemas_by_name = {t.name: t.input_schema for t in request.tools}
            for block in response.content:
                if block.type == "text":
                    text_content += block.text
                    raw_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    original_input = dict(block.input) if block.input else {}
                    # Echo the ORIGINAL input in raw_content (exact round-trip);
                    # the coerced form is for OUR consumers only.
                    raw_content.append(
                        {
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": original_input,
                        }
                    )
                    coerced = original_input
                    tool_schema = schemas_by_name.get(block.name)
                    if tool_schema is not None:
                        coerced = _coerce_stringified_args(
                            original_input, tool_schema, label=request.label
                        )
                    tool_calls.append(
                        ToolCall(
                            id=block.id,
                            name=block.name,
                            input=coerced,
                        )
                    )
                elif block.type == "thinking":
                    raw_content.append(
                        {
                            "type": "thinking",
                            "thinking": block.thinking,
                            "signature": block.signature,
                        }
                    )
                elif block.type == "redacted_thinking":
                    raw_content.append({"type": "redacted_thinking", "data": block.data})

            # Cache-usage fields are optional on the SDK Usage object (None when
            # no cache_control is in play — the engine today, until DAT-601);
            # coerce only None to 0 so telemetry stays numeric while preserving a
            # genuine int(0) ("caching configured, nothing read") once 601 lands.
            usage = response.usage
            cache_read = (
                usage.cache_read_input_tokens if usage.cache_read_input_tokens is not None else 0
            )
            cache_creation = (
                usage.cache_creation_input_tokens
                if usage.cache_creation_input_tokens is not None
                else 0
            )

            # Per-call telemetry (DAT-600): elapsed + token usage, tagged by the
            # caller's agent/phase label. Latency is output-decode-dominated, so
            # output_tokens vs elapsed_ms is the wall-clock signal; the cache
            # fields are the DAT-601 cost lever (zero until caching lands).
            logger.info(
                "llm_call",
                label=request.label,
                model=response.model,
                elapsed_ms=elapsed_ms,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_read_input_tokens=cache_read,
                cache_creation_input_tokens=cache_creation,
            )

            return Result.ok(
                ConversationResponse(
                    content=text_content,
                    tool_calls=tool_calls,
                    raw_content=raw_content or None,
                    stop_reason=response.stop_reason or "end_turn",
                    model=response.model,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    cache_read_input_tokens=cache_read,
                    cache_creation_input_tokens=cache_creation,
                )
            )

        except anthropic.APIError as e:
            provider_error = _classify_anthropic_error(e)
            logger.error(
                "anthropic_api_error",
                error=str(e),
                model=model,
                kind=type(provider_error).__name__,
            )
            # Raise the typed exception (DAT-503): retryability rides the type
            # through the phase chain to the worker's durable boundary, not a
            # substring of a Result.error every layer could reword.
            raise provider_error from e
        except Exception as e:
            logger.error("anthropic_unexpected_error", error=str(e), model=model)
            # Unexpected (non-API) failures are non-retryable — retrying the
            # identical call is unlikely to clear a bug in our request shaping.
            raise PermanentProviderError(f"Unexpected error calling Anthropic: {e}") from e

    def _convert_messages(self, messages: list[Message]) -> list[MessageParam]:
        """Convert our Message format to Anthropic's MessageParam format.

        Args:
            messages: List of our Message objects

        Returns:
            List of Anthropic MessageParam objects
        """
        result: list[MessageParam] = []

        for msg in messages:
            if msg.role == "user":
                # User message - could be text or tool results
                if isinstance(msg.content, list):
                    # Tool results - msg.content is list[ToolResult]
                    tool_results: list[ToolResult] = msg.content
                    content: list[ToolResultBlockParam] = [
                        cast(
                            ToolResultBlockParam,
                            {
                                "type": "tool_result",
                                "tool_use_id": tr.tool_use_id,
                                "content": tr.content,
                                "is_error": tr.is_error,
                            },
                        )
                        for tr in tool_results
                    ]
                    result.append(cast(MessageParam, {"role": "user", "content": content}))
                else:
                    # Plain text
                    result.append(cast(MessageParam, {"role": "user", "content": msg.content}))

            elif msg.role == "assistant":
                # Verbatim continuation (DAT-699): an echoed assistant turn uses
                # its captured blocks unchanged — signed thinking blocks must
                # round-trip exactly or the API rejects the continuation.
                if msg.raw_content:
                    result.append(
                        cast(MessageParam, {"role": "assistant", "content": msg.raw_content})
                    )
                    continue

                # Assistant message - could have text and/or tool calls
                content_blocks: list[Any] = []

                if msg.content and isinstance(msg.content, str):
                    content_blocks.append({"type": "text", "text": msg.content})

                if msg.tool_calls:
                    for tc in msg.tool_calls:
                        content_blocks.append(
                            cast(
                                ToolUseBlockParam,
                                {
                                    "type": "tool_use",
                                    "id": tc.id,
                                    "name": tc.name,
                                    "input": tc.input,
                                },
                            )
                        )

                if content_blocks:
                    result.append(
                        cast(MessageParam, {"role": "assistant", "content": content_blocks})
                    )

        return result
