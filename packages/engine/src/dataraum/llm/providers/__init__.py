"""LLM provider implementations and factory."""

from typing import Any

from dataraum.core.logging import get_logger
from dataraum.llm.providers.base import LLMProvider

__all__ = ["LLMProvider", "create_provider"]

logger = get_logger(__name__)


def create_provider(provider_name: str, provider_config: dict[str, Any]) -> LLMProvider:
    """Create LLM provider based on configuration.

    Args:
        provider_name: Provider name ('anthropic')
        provider_config: Provider-specific configuration dict

    Returns:
        Initialized LLM provider

    Raises:
        ValueError: If provider name is unknown
    """
    if provider_name == "anthropic":
        from dataraum.llm.providers.anthropic import AnthropicConfig, AnthropicProvider

        anthropic_config = AnthropicConfig(**provider_config)
        logger.debug("llm_provider_created", provider=provider_name)
        return AnthropicProvider(anthropic_config)

    raise ValueError(f"Unknown LLM provider: {provider_name}. Supported providers: anthropic")
