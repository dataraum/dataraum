"""LLM module - AI-powered intelligent analysis.

This module provides LLM-powered features for semantic analysis.
The SemanticAnalysisFeature is co-located with enrichment module.

Example usage:

    from dataraum_context.llm import LLMService
    from dataraum_context.llm.config import load_llm_config

    # Initialize service
    config = load_llm_config()
    service = LLMService(config)

    # Run semantic analysis
    result = await service.analyze_semantics(
        session=db_session,
        table_ids=["table-123"],
        ontology="financial_reporting"
    )

    if result.success:
        for annotation in result.value.annotations:
            print(f"{annotation.column_ref}: {annotation.semantic_role}")
"""

from typing import Any

from dataraum_context.llm.cache import LLMCache
from dataraum_context.llm.config import LLMConfig, load_llm_config
from dataraum_context.llm.prompts import PromptRenderer
from dataraum_context.llm.providers import create_provider

__all__ = [
    "LLMService",
    "LLMConfig",
    "load_llm_config",
    "LLMCache",
]


class LLMService:
    """Main service facade for LLM features.

    Provides a unified interface to LLM-powered analysis features.
    Manages provider initialization, caching, and prompt rendering.
    """

    def __init__(self, config: LLMConfig):
        """Initialize LLM service.

        Args:
            config: LLM configuration

        Raises:
            ValueError: If provider configuration is invalid
            ImportError: If required provider package not installed
        """
        self.config = config

        # Get provider config for active provider
        if config.active_provider not in config.providers:
            raise ValueError(
                f"Active provider '{config.active_provider}' not found in config. "
                f"Available: {list(config.providers.keys())}"
            )

        provider_config = config.providers[config.active_provider]

        # Create provider
        self.provider = create_provider(
            config.active_provider,
            provider_config.model_dump(),
        )

        # Create shared components
        self.cache = LLMCache()
        self.renderer = PromptRenderer()

        # Create semantic analysis feature (co-located with enrichment)
        from dataraum_context.enrichment.llm_feature import SemanticAnalysisFeature

        self.semantic = SemanticAnalysisFeature(
            config=config,
            provider=self.provider,
            prompt_renderer=self.renderer,
            cache=self.cache,
        )

    # Convenience methods that delegate to features

    async def analyze_semantics(self, *args: Any, **kwargs: Any) -> Any:
        """Run semantic analysis. See SemanticAnalysisFeature.analyze()."""
        return await self.semantic.analyze(*args, **kwargs)
