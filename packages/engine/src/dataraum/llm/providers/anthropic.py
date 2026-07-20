"""Anthropic Claude provider implementation."""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, cast

import anthropic
from anthropic.types import MessageParam, ToolParam, ToolResultBlockParam, ToolUseBlockParam
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from pydantic import BaseModel

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.prompt_log import dump_prompt, dump_response
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

# Resolves against the worker's global TracerProvider (worker/telemetry.py);
# a no-op tracer when telemetry is off, so the span brackets below cost ~nothing.
tracer = trace.get_tracer(__name__)


# --- Model request-shape capabilities (Claude 4.7+ / Sonnet 5 / Fable 5) ---
#
# The engine is the structured-extraction tier (ADR-0004): every call asks for a
# typed result and wants determinism, not agentic reasoning. That typed result
# comes from Anthropic STRUCTURED OUTPUTS — ``output_config.format`` constrains
# decoding to the caller's JSON Schema and the answer arrives as message content
# (DAT-807). It is NOT a forced ``tool_choice`` any more: forcing a tool the
# model was never meant to call was a structured-output stand-in from before the
# API had one, and it produced the malformation class this tier spent a year
# compensating for (stringified payloads, paraphrased envelopes that silently
# zeroed every field). A tool in this tier is now a tool the model genuinely
# calls (``search_values``). Two request-shape changes landed with this model
# generation that the tier must honour, or Sonnet 5 rejects the call outright:
#
#   * Non-default sampling params (``temperature``/``top_p``/``top_k``) return a
#     400. Our prompt templates ask for temperature 0.0-0.1 for determinism, so
#     on these models we OMIT ``temperature`` and rely on the constrained
#     grammar + prompt for stable output (temperature 0 never guaranteed
#     identical output anyway).
#   * Thinking defaults DIFFER across the family: Sonnet 5 runs adaptive
#     thinking ON when ``thinking`` is omitted; Opus 4.7/4.8 default it OFF.
#     A mechanical extractor never wants it (it burns output budget the
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
# engine's extraction tier does not target them.
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


# JSON-Schema keywords constrained-grammar compilation rejects — the same set
# for ``output_config.format`` and for a ``strict: true`` tool. Stripping them
# is lossless for correctness: Pydantic re-validates the decoded payload
# client-side, so range/length constraints are still enforced — the grammar
# guarantees the SHAPE (no stringified payloads, no missing/extra keys),
# Pydantic the values.
_CONSTRAINED_UNSUPPORTED_KEYS = frozenset(
    {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        "maxItems",
    }
)

# ``minItems`` is the ONE array constraint the grammar accepts, and only at the
# values 0 and 1 (Anthropic structured-outputs reference). It is therefore NOT
# in the unsupported set above: a schema that says "this array must be
# non-empty" is the only way to make the model produce at least one element,
# and stripping it silently turns a mandatory list into an optional one. Any
# other value is rejected by the compiler, so those are dropped.
_MIN_ITEMS_SUPPORTED_VALUES = (0, 1)


def _constrained_schema(node: Any) -> Any:
    """Normalize a Pydantic JSON schema for constrained decoding.

    One normalization serves both constrained-decoding surfaces —
    ``output_config.format`` (the typed RESULT of every engine agent) and a
    ``strict: true`` tool's ``input_schema``: recursively set
    ``additionalProperties: false`` on every object node (both require it
    explicitly; Pydantic never emits it) and strip the constraint keywords the
    grammar compiler rejects (see ``_CONSTRAINED_UNSUPPORTED_KEYS``).

    ``additionalProperties: false`` also FORBIDS an open map — a
    ``dict[str, Model]`` field cannot be expressed under constrained decoding
    and must be modelled as a list of ``{key, value}`` entries.
    """
    if isinstance(node, dict):
        out = {
            k: _constrained_schema(v)
            for k, v in node.items()
            if k not in _CONSTRAINED_UNSUPPORTED_KEYS
            and not (k == "minItems" and v not in _MIN_ITEMS_SUPPORTED_VALUES)
        }
        if out.get("type") == "object" or "properties" in out:
            out.setdefault("additionalProperties", False)
        return out
    if isinstance(node, list):
        return [_constrained_schema(v) for v in node]
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
                            "input_schema": _constrained_schema(t.input_schema)
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

            # ONE output_config carries both knobs the API nests there.
            #   * effort (DAT-603) — per-feature output effort. Extraction agents
            #     run thinking-off + effort:low (shorter output = lower serial-
            #     decode latency). Only sent where the model supports the
            #     parameter — Haiku 4.5 rejects it, so the fast tier drops it.
            #   * format (DAT-807) — the structured-output grammar. This is how
            #     every engine agent gets its typed result; the schema is
            #     normalized exactly like a strict tool's (recursive
            #     additionalProperties:false, unsupported constraint keywords
            #     stripped) because constrained decoding compiles both the same way.
            output_config: dict[str, Any] = {}
            if request.effort and _supports_effort(model):
                output_config["effort"] = request.effort
            if request.output_schema is not None:
                output_config["format"] = {
                    "type": "json_schema",
                    "schema": _constrained_schema(request.output_schema),
                }
            if output_config:
                kwargs["output_config"] = output_config

            # GenAI-semconv span (DAT-706) around the client call. Current
            # conventions (the relocated semantic-conventions-genai repo):
            # name "{operation} {model}", kind CLIENT, provider discriminator
            # `gen_ai.provider.name` — NOT the retired `gen_ai.system`. The
            # worker's TracingInterceptor makes the enclosing activity span
            # current in this thread (temporalio copies contextvars into the
            # ThreadPoolExecutor), so the span nests into the run trace with
            # no explicit context plumbing.
            span_attributes: dict[str, Any] = {
                "gen_ai.operation.name": "chat",
                "gen_ai.provider.name": "anthropic",
                "gen_ai.request.model": model,
                "gen_ai.request.max_tokens": request.max_tokens,
                "gen_ai.request.stream": True,
            }
            if "temperature" in kwargs:
                span_attributes["gen_ai.request.temperature"] = request.temperature
            if "effort" in output_config:
                span_attributes["gen_ai.request.reasoning.level"] = request.effort
            if request.label:
                # Same key the cockpit's otelMiddleware enricher stamps, so
                # "which call site dominates" is one cross-stack query (DAT-599).
                span_attributes["dataraum.call_site"] = request.label

            # Stream + accumulate instead of a one-shot create: the SDK refuses
            # a non-streaming request whose max_tokens it estimates could exceed
            # ~10 minutes (ValueError "Streaming is required…"), and the Sonnet 5
            # output budget (24000) trips that guard — every pipeline call died
            # on it in the 2026-07-02 smoke. Streaming lifts the ceiling; callers
            # still receive one final Message via get_final_message().
            start = time.perf_counter()
            with tracer.start_as_current_span(
                f"chat {model}",
                kind=SpanKind.CLIENT,
                attributes=span_attributes,
            ) as span:
                try:
                    with self.client.messages.stream(**kwargs) as stream:
                        response = stream.get_final_message()
                except Exception as e:
                    # semconv `error.type` (low-cardinality class name); the
                    # exception event + ERROR status are recorded by the span
                    # context manager as the raise propagates through it.
                    span.set_attribute("error.type", type(e).__qualname__)
                    raise
                elapsed_ms = round((time.perf_counter() - start) * 1000)

                # Cache-usage fields are optional on the SDK Usage object (None
                # when no cache_control is in play); coerce only None to 0 so
                # telemetry stays numeric while preserving a genuine int(0)
                # ("caching configured, nothing read").
                usage = response.usage
                cache_read = (
                    usage.cache_read_input_tokens
                    if usage.cache_read_input_tokens is not None
                    else 0
                )
                cache_creation = (
                    usage.cache_creation_input_tokens
                    if usage.cache_creation_input_tokens is not None
                    else 0
                )
                span.set_attributes(
                    {
                        "gen_ai.response.model": response.model,
                        "gen_ai.response.finish_reasons": [response.stop_reason or "end_turn"],
                        "gen_ai.usage.input_tokens": usage.input_tokens,
                        "gen_ai.usage.output_tokens": usage.output_tokens,
                        "gen_ai.usage.cache_read.input_tokens": cache_read,
                        "gen_ai.usage.cache_creation.input_tokens": cache_creation,
                    }
                )

            # Extract content and tool calls from response. Under
            # ``output_config.format`` the typed result IS the text content —
            # constrained decoding guarantees it parses against the request's
            # schema, so there is nothing to coerce, unwrap or repair on this
            # boundary (DAT-807 deleted all three). raw_content keeps the turn's
            # blocks VERBATIM (incl. signed thinking blocks) so a continued
            # conversation can echo the assistant turn back — the thinking-model
            # API rejects a continuation whose assistant turn lost its thinking
            # blocks (DAT-699).
            text_content = ""
            tool_calls: list[ToolCall] = []
            raw_content: list[dict[str, Any]] = []

            for block in response.content:
                if block.type == "text":
                    text_content += block.text
                    raw_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    tool_input = dict(block.input) if block.input else {}
                    raw_content.append(
                        {
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": tool_input,
                        }
                    )
                    tool_calls.append(ToolCall(id=block.id, name=block.name, input=tool_input))
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

            # Narrative log line — the gen_ai span above is the analysis path
            # (DAT-706); this ships to Loki with trace correlation like every
            # structlog event (DAT-707), so it stays a human-readable marker,
            # not an aggregation surface.
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

            # Dump the ACTUAL rendered prompt + the model's raw response — the
            # structured-output JSON plus any real tool call — for EVERY call at
            # the provider chokepoint, not just the graph agent (DAT-758/759
            # diag). Gated by settings.prompt_dump_dir (no-op otherwise);
            # best-effort. This is the only way to see what an extractor really
            # produced: a schema-valid but EMPTY payload is invisible in the
            # token log (it reads the same as a rich one), and the eval reads
            # these dumps keyed by (label, prompt_hash).
            _label = request.label or "unlabeled"
            _user = "\n\n".join(m.content for m in request.messages if isinstance(m.content, str))
            _phash = hashlib.sha256(_user.encode("utf-8")).hexdigest()[:16]
            dump_prompt(
                label=_label,
                key=_label,
                prompt_hash=_phash,
                system=request.system,
                user=_user,
                model=response.model,
            )
            _resp_body = text_content + "".join(
                f"\n[tool_use {tc.name}]\n{json.dumps(tc.input, indent=2, default=str)}"
                for tc in tool_calls
            )
            dump_response(
                label=_label,
                key=_label,
                prompt_hash=_phash,
                body=f"stop_reason={response.stop_reason}\n{_resp_body}",
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
