"""Base class for LLM features with common functionality."""

from dataraum.llm.config import LLMConfig
from dataraum.llm.prompts import PromptRenderer
from dataraum.llm.providers.base import LLMProvider


class LLMFeature:
    """Base class for LLM features.

    Provides common functionality:
    - Configuration access
    - Provider access
    - Prompt rendering
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
    ):
        """Initialize LLM feature.

        Args:
            config: LLM configuration
            provider: LLM provider instance
            prompt_renderer: Prompt template renderer
        """
        self.config = config
        self.provider = provider
        self.renderer = prompt_renderer
