"""Abstract base class for LLM providers.

This module defines the interface that all LLM providers must implement.
"""

from abc import ABC, abstractmethod

from pydantic import BaseModel

from dataraum_context.core.models.base import Result


class LLMRequest(BaseModel):
    """Request to LLM provider."""

    prompt: str
    max_tokens: int = 4000
    temperature: float = 0.0
    response_format: str = "json"  # "json" or "text"


class LLMResponse(BaseModel):
    """Response from LLM provider."""

    content: str
    model: str
    input_tokens: int
    output_tokens: int
    cached: bool = False  # True if from our cache
    provider_cached: bool = False  # True if provider cache hit


class LLMProvider(ABC):
    """Abstract base for LLM providers.

    All LLM providers (Anthropic, OpenAI, Local) must implement this interface.
    """

    @abstractmethod
    async def complete(self, request: LLMRequest) -> Result[LLMResponse]:
        """Send completion request to provider.

        Args:
            request: The LLM request with prompt and parameters

        Returns:
            Result containing LLMResponse or error message
        """
        pass

    @abstractmethod
    def get_model_for_tier(self, tier: str) -> str:
        """Get model name for a given tier.

        Args:
            tier: Model tier ('fast', 'balanced')

        Returns:
            Model name/identifier for the provider
        """
        pass
