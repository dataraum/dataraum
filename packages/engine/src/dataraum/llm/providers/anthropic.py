"""Anthropic Claude provider implementation."""

from __future__ import annotations

from typing import Any, cast

import anthropic
from anthropic.types import MessageParam, ToolParam, ToolResultBlockParam, ToolUseBlockParam
from pydantic import BaseModel

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.providers.base import (
    PERMANENT_ERROR_KIND,
    TRANSIENT_ERROR_KIND,
    ConversationRequest,
    ConversationResponse,
    LLMProvider,
    Message,
    ToolCall,
    ToolResult,
    format_api_error,
)


class AnthropicConfig(BaseModel):
    """Configuration for Anthropic provider."""

    default_model: str
    models: dict[str, str]


logger = get_logger(__name__)


# 4xx codes the user must fix — credentials, schema, request shape.
# 429 (rate limit) and 408/409 are retryable so they are NOT in this set.
_PERMANENT_STATUS_CODES = frozenset({400, 401, 403, 404, 413, 422})


_AUTH_STATUS_CODES = frozenset({401, 403})


def _classify_anthropic_error(exc: anthropic.APIError) -> tuple[str, str]:
    """Categorize an Anthropic exception so the caller can act on it.

    Returns:
        (kind, message): ``kind`` is "permanent" (user must fix) or
        "transient" (retry may help). ``message`` is the human-readable
        body of the error, with an actionable hint appended for auth
        failures so practitioners know exactly what to fix.

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
            return PERMANENT_ERROR_KIND, message
        return TRANSIENT_ERROR_KIND, message
    # Connection / timeout errors don't have a status code; they're
    # always transient by definition.
    if isinstance(exc, anthropic.APIConnectionError):
        return TRANSIENT_ERROR_KIND, str(exc)
    # APIResponseValidationError: the server returned something the SDK
    # couldn't parse. Treat as permanent — retrying the same request
    # likely produces the same malformed response.
    if isinstance(exc, anthropic.APIResponseValidationError):
        return PERMANENT_ERROR_KIND, str(exc)
    # Anything else under APIError — default to transient (retry is
    # the safer default than surfacing as a user error).
    return TRANSIENT_ERROR_KIND, str(exc)


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
                            "input_schema": t.input_schema,
                        },
                    )
                    for t in request.tools
                ]

            # Make API call
            kwargs: dict[str, Any] = {
                "model": model,
                "max_tokens": request.max_tokens,
                "temperature": request.temperature,
                "messages": messages,
            }

            if request.system:
                kwargs["system"] = request.system

            if tools:
                kwargs["tools"] = tools

            if request.tool_choice:
                kwargs["tool_choice"] = request.tool_choice

            response = self.client.messages.create(**kwargs)

            # Extract content and tool calls from response
            text_content = ""
            tool_calls: list[ToolCall] = []

            for block in response.content:
                if block.type == "text":
                    text_content += block.text
                elif block.type == "tool_use":
                    tool_calls.append(
                        ToolCall(
                            id=block.id,
                            name=block.name,
                            input=dict(block.input) if block.input else {},
                        )
                    )

            return Result.ok(
                ConversationResponse(
                    content=text_content,
                    tool_calls=tool_calls,
                    stop_reason=response.stop_reason or "end_turn",
                    model=response.model,
                    input_tokens=response.usage.input_tokens,
                    output_tokens=response.usage.output_tokens,
                )
            )

        except anthropic.APIError as e:
            kind, message = _classify_anthropic_error(e)
            logger.error("anthropic_api_error", error=str(e), model=model, kind=kind)
            return Result.fail(format_api_error("Anthropic", kind, message))
        except Exception as e:
            logger.error("anthropic_unexpected_error", error=str(e), model=model)
            return Result.fail(f"Unexpected error calling Anthropic: {e}")

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
