"""Anthropic Claude provider implementation."""

import os
from typing import cast

import anthropic
from anthropic.types import MessageParam
from pydantic import BaseModel

from dataraum_context.core.models.base import Result
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest, LLMResponse


class AnthropicConfig(BaseModel):
    """Configuration for Anthropic provider."""

    api_key_env: str
    default_model: str
    models: dict[str, str]


class AnthropicProvider(LLMProvider):
    """Anthropic Claude provider implementation.

    Uses the Anthropic async client to make API calls to Claude models.
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

        # Create async client
        self.client = anthropic.AsyncAnthropic(api_key=api_key)

    async def complete(self, request: LLMRequest) -> Result[LLMResponse]:
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
                response = await self.client.messages.create(
                    model=model,
                    max_tokens=request.max_tokens,
                    temperature=request.temperature,
                    messages=messages,
                    system=system_prompt,
                )
            else:
                response = await self.client.messages.create(
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
