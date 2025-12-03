"""LLM module - AI-powered intelligent analysis.

This module provides LLM-powered features for semantic analysis,
quality rule generation, suggested queries, and context summaries.

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
from dataraum_context.llm.features.quality import QualityRulesFeature

# DISABLED: Depend on context module which is not yet implemented
# from dataraum_context.llm.features.queries import SuggestedQueriesFeature
# from dataraum_context.llm.features.summary import ContextSummaryFeature
from dataraum_context.llm.features.semantic import SemanticAnalysisFeature
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

    Provides a unified interface to all LLM-powered analysis features.
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

        # Create features
        self.semantic = SemanticAnalysisFeature(
            config=config,
            provider=self.provider,
            prompt_renderer=self.renderer,
            cache=self.cache,
        )

        self.quality = QualityRulesFeature(
            config=config,
            provider=self.provider,
            prompt_renderer=self.renderer,
            cache=self.cache,
        )

        # DISABLED: Depend on context module which is not yet implemented
        self.queries = None  # type: ignore[assignment]
        self.summary = None  # type: ignore[assignment]

    # Convenience methods that delegate to features

    async def analyze_semantics(self, *args: Any, **kwargs: Any) -> Any:
        """Run semantic analysis. See SemanticAnalysisFeature.analyze()."""
        return await self.semantic.analyze(*args, **kwargs)

    async def generate_quality_rules(self, *args: Any, **kwargs: Any) -> Any:
        """Generate quality rules. See QualityRulesFeature.generate_rules()."""
        return await self.quality.generate_rules(*args, **kwargs)

    async def generate_suggested_queries(self, *args: Any, **kwargs: Any) -> Any:
        """Generate suggested queries. See SuggestedQueriesFeature.generate_queries()."""
        if self.queries is None:
            raise NotImplementedError(
                "Suggested queries feature is not yet implemented. "
                "Requires context module to be completed."
            )
        return await self.queries.generate_queries(*args, **kwargs)

    async def generate_context_summary(self, *args: Any, **kwargs: Any) -> Any:
        """Generate context summary. See ContextSummaryFeature.generate_summary()."""
        if self.summary is None:
            raise NotImplementedError(
                "Context summary feature is not yet implemented. "
                "Requires context module to be completed."
            )
        return await self.summary.generate_summary(*args, **kwargs)
