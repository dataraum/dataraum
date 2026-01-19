"""Local LLM provider stub - to be implemented later."""

from pydantic import BaseModel

from dataraum_context.core.models.base import Result
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest, LLMResponse


class LocalConfig(BaseModel):
    """Configuration for local LLM provider."""

    base_url_env: str
    api_key_env: str  # Often "not-needed" for local
    default_model: str
    models: dict[str, str]


class LocalProvider(LLMProvider):
    """Local LLM provider stub - not yet implemented.

    For local LLMs via Ollama, vLLM, or other OpenAI-compatible endpoints.

    To contribute local LLM support:
    1. Accept base_url from environment variable
    2. Use openai package with custom base_url
    3. Handle local model naming conventions
    4. Token counting may be approximate
    """

    def __init__(self, config: LocalConfig):
        """Initialize local provider.

        Args:
            config: Provider configuration

        Raises:
            NotImplementedError: Always - not yet implemented
        """
        raise NotImplementedError(
            "Local LLM provider not yet implemented. "
            "Use 'anthropic' provider or contribute local support! "
            "See docs/LLM_FEATURES.md for implementation guide."
        )

    def complete(self, request: LLMRequest) -> Result[LLMResponse]:
        """Not implemented."""
        raise NotImplementedError()

    def get_model_for_tier(self, tier: str) -> str:
        """Not implemented."""
        raise NotImplementedError()
