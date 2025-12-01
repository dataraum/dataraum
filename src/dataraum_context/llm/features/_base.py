"""Base class for LLM features with common functionality."""

from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.llm.cache import LLMCache
from dataraum_context.llm.config import LLMConfig
from dataraum_context.llm.prompts import PromptRenderer
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest, LLMResponse


class LLMFeature:
    """Base class for LLM features.

    Provides common functionality:
    - LLM calling with caching
    - Configuration access
    - Prompt rendering
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        cache: LLMCache,
    ):
        """Initialize LLM feature.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
            cache: Response cache
        """
        self.config = config
        self.provider = provider
        self.renderer = prompt_renderer
        self.cache = cache

    async def _call_llm(
        self,
        session: AsyncSession,
        feature_name: str,
        prompt: str,
        temperature: float,
        model_tier: str,
        source_id: str | None = None,
        table_ids: list[str] | None = None,
        ontology: str | None = None,
    ) -> Result[LLMResponse]:
        """Call LLM with caching.

        Args:
            session: Database session for cache
            feature_name: Feature name (semantic_analysis, etc.)
            prompt: Rendered prompt text
            temperature: Temperature for generation
            model_tier: Model tier ('fast' or 'balanced')
            source_id: Optional source ID
            table_ids: Optional table IDs
            ontology: Optional ontology name

        Returns:
            Result containing LLMResponse or error
        """
        # Get model for tier
        model = self.provider.get_model_for_tier(model_tier)

        # Check cache first
        cached = await self.cache.get(
            session=session,
            feature=feature_name,
            prompt=prompt,
            model=model,
            table_ids=table_ids,
        )

        if cached:
            return Result.ok(cached)

        # Call provider
        request = LLMRequest(
            prompt=prompt,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            response_format="json",
        )

        result = await self.provider.complete(request)
        if not result.success or not result.value:
            return result

        # Store in cache
        await self.cache.put(
            session=session,
            feature=feature_name,
            prompt=prompt,
            response=result.value,
            source_id=source_id,
            table_ids=table_ids,
            ontology=ontology,
            ttl_seconds=self.config.limits.cache_ttl_seconds,
        )

        return result
