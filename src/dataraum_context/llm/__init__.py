"""LLM module - AI-powered intelligent analysis infrastructure.

This module provides shared LLM infrastructure:
- LLMConfig: Configuration for LLM providers and features
- LLMCache: Response caching
- PromptRenderer: Template rendering
- LLM Providers: Anthropic, OpenAI, etc.

Agents that use LLM are co-located with their domain modules:
- enrichment/agent.py: SemanticAgent for semantic analysis
- graphs/agent.py: GraphAgent for SQL generation

Example usage:

    from dataraum_context.llm import LLMConfig, load_llm_config
    from dataraum_context.llm.cache import LLMCache
    from dataraum_context.llm.prompts import PromptRenderer
    from dataraum_context.llm.providers import create_provider
    from dataraum_context.enrichment.agent import SemanticAgent

    # Initialize infrastructure
    config = load_llm_config()
    provider = create_provider(config.active_provider, config.providers[config.active_provider].model_dump())
    cache = LLMCache()
    renderer = PromptRenderer()

    # Create agent
    agent = SemanticAgent(config=config, provider=provider, prompt_renderer=renderer, cache=cache)

    # Run semantic analysis
    result = await agent.analyze(session=db_session, table_ids=["table-123"], ontology="financial_reporting")
"""

from dataraum_context.llm.cache import LLMCache
from dataraum_context.llm.config import LLMConfig, load_llm_config
from dataraum_context.llm.prompts import PromptRenderer
from dataraum_context.llm.providers import create_provider

__all__ = [
    "LLMConfig",
    "load_llm_config",
    "LLMCache",
    "PromptRenderer",
    "create_provider",
]
