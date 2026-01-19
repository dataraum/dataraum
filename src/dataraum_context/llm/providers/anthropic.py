"""Anthropic Claude provider implementation."""

import os
from typing import Any, cast

import anthropic
from anthropic.types import MessageParam, ToolParam, ToolResultBlockParam, ToolUseBlockParam
from pydantic import BaseModel

from dataraum_context.core.models.base import Result
from dataraum_context.llm.providers.base import (
    ConversationRequest,
    ConversationResponse,
    LLMProvider,
    LLMRequest,
    LLMResponse,
    Message,
    ToolCall,
    ToolResult,
)


class AnthropicConfig(BaseModel):
    """Configuration for Anthropic provider."""

    api_key_env: str
    default_model: str
    models: dict[str, str]


class AnthropicProvider(LLMProvider):
    """Anthropic Claude provider implementation.

    Uses the Anthropic sync client to make API calls to Claude models.
    Supports both JSON and text response formats.
    """

    def __init__(self, config: AnthropicConfig):
        """Initialize Anthropic provider.

        Args:
            config: Provider configuration

        Raises:
            ImportError: If anthropic package not installed
            ValueError: If API key environment variable not set
        """
        if anthropic is None:
            raise ImportError(
                "anthropic package not installed. Install with: pip install anthropic"
            )

        self.config = config

        # Get API key from environment
        api_key = os.getenv(config.api_key_env)
        if not api_key:
            raise ValueError(
                f"Missing environment variable: {config.api_key_env}. "
                f"Set your Anthropic API key in .env file."
            )

        # Create sync client
        self.client = anthropic.Anthropic(api_key=api_key)

    def complete(self, request: LLMRequest) -> Result[LLMResponse]:
        """Send completion request to Claude API.

        Args:
            request: The LLM request

        Returns:
            Result containing LLMResponse or error message
        """
        try:
            model = self.config.default_model

            # Build messages
            messages: list[MessageParam] = [
                cast(MessageParam, {"role": "user", "content": request.prompt})
            ]

            # Handle JSON mode via system prompt
            # Claude doesn't have native JSON mode like OpenAI, so we use system prompt
            system_prompt = None
            if request.response_format == "json":
                system_prompt = (
                    "Respond with valid JSON only. "
                    "Do not use markdown code blocks or any other formatting. "
                    "Your entire response should be parseable as JSON."
                )

            # Make API call
            if system_prompt:
                response = self.client.messages.create(
                    model=model,
                    max_tokens=request.max_tokens,
                    temperature=request.temperature,
                    messages=messages,
                    system=system_prompt,
                )
            else:
                response = self.client.messages.create(
                    model=model,
                    max_tokens=request.max_tokens,
                    temperature=request.temperature,
                    messages=messages,
                )

            # Extract text content from response
            # Response.content is a list of ContentBlock objects
            content_blocks = []
            for block in response.content:
                if block.type == "text":
                    content_blocks.append(block.text)

            content = "".join(content_blocks)

            if not content:
                return Result.fail(
                    f"No text content in response. Content blocks: {[b.type for b in response.content]}"
                )

            # Create response
            llm_response = LLMResponse(
                content=content,
                model=response.model,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                cached=False,
                provider_cached=False,  # Anthropic doesn't expose cache hits in API
            )

            return Result.ok(llm_response)

        except anthropic.APIError as e:
            return Result.fail(f"Anthropic API error: {e}")
        except Exception as e:
            return Result.fail(f"Unexpected error calling Anthropic: {e}")

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
            return Result.fail(f"Anthropic API error: {e}")
        except Exception as e:
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
