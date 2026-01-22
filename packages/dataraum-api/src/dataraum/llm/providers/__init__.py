"""LLM provider implementations and factory."""

from typing import Any

from dataraum.llm.providers.base import LLMProvider, LLMRequest, LLMResponse

__all__ = ["LLMProvider", "LLMRequest", "LLMResponse", "create_provider"]


def create_provider(provider_name: str, provider_config: dict[str, Any]) -> LLMProvider:
    """Create LLM provider based on configuration.

    Args:
        provider_name: Provider name ('anthropic', 'openai', 'local')
        provider_config: Provider-specific configuration dict

    Returns:
        Initialized LLM provider

    Raises:
        ValueError: If provider name is unknown
        NotImplementedError: If provider is not yet implemented
    """
    if provider_name == "anthropic":
        from dataraum.llm.providers.anthropic import AnthropicConfig, AnthropicProvider

        anthropic_config = AnthropicConfig(**provider_config)
        return AnthropicProvider(anthropic_config)

    elif provider_name == "openai":
        from dataraum.llm.providers.openai import OpenAIConfig, OpenAIProvider

        openai_config = OpenAIConfig(**provider_config)
        return OpenAIProvider(openai_config)

    elif provider_name == "local":
        from dataraum.llm.providers.local import LocalConfig, LocalProvider

        local_config = LocalConfig(**provider_config)
        return LocalProvider(local_config)

    else:
        raise ValueError(
            f"Unknown LLM provider: {provider_name}. Supported providers: anthropic, openai, local"
        )
