"""OpenAI provider stub - to be implemented later."""

from pydantic import BaseModel

from dataraum_context.core.models.base import Result
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest, LLMResponse


class OpenAIConfig(BaseModel):
    """Configuration for OpenAI provider."""

    api_key_env: str
    default_model: str
    models: dict[str, str]


class OpenAIProvider(LLMProvider):
    """OpenAI provider stub - not yet implemented.

    To contribute OpenAI support:
    1. Install openai package: pip install openai
    2. Implement complete() method using openai.OpenAI
    3. Use native JSON mode: response_format={"type": "json_object"}
    4. Handle token counting and errors
    """

    def __init__(self, config: OpenAIConfig):
        """Initialize OpenAI provider.

        Args:
            config: Provider configuration

        Raises:
            NotImplementedError: Always - not yet implemented
        """
        raise NotImplementedError(
            "OpenAI provider not yet implemented. "
            "Use 'anthropic' provider or contribute OpenAI support! "
            "See docs/LLM_FEATURES.md for implementation guide."
        )

    def complete(self, request: LLMRequest) -> Result[LLMResponse]:
        """Not implemented."""
        raise NotImplementedError()

    def get_model_for_tier(self, tier: str) -> str:
        """Not implemented."""
        raise NotImplementedError()
