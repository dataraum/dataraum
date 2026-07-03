"""LLM configuration models and loader.

Loads configuration from config/llm.yaml and provides typed access
to all LLM settings: providers, features, limits, privacy.
"""

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ProviderConfig(BaseModel):
    """Configuration for a single LLM provider.

    The API key is no longer named here — it is read from typed settings
    (``settings.anthropic_api_key``), which is the single source of truth.
    """

    default_model: str
    models: dict[str, str]


class FeatureConfig(BaseModel):
    """Configuration for an LLM feature.

    Extra fields from YAML (e.g. batch_size, baseline_filter) are preserved
    and accessible via getattr().
    """

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    model_tier: str = "balanced"
    # Output effort for this feature's calls (DAT-603). "low" suits mechanical
    # extraction (shorter output, lower latency); None = API default. On a
    # thinking-enabled feature, effort also governs thinking depth.
    effort: str | None = None
    # Adaptive thinking for this feature's calls (DAT-603). Default False: the
    # mechanical extractors run thinking-off. A reasoning-heavy feature (metric
    # grounding) sets true — the model reflects before committing, which is the
    # quality lever now that Sonnet 5-class models expose no sampling knobs.
    # Requires the call site to use a non-forced tool_choice (API constraint).
    thinking: bool = False


class LLMFeatures(BaseModel):
    """All LLM features configuration.

    Every feature key in ``llm/config.yaml`` MUST be declared here. Extras are
    FORBIDDEN because the silent alternative already bit us (DAT-603): pydantic's
    default drops unknown keys, so a YAML-only feature (``sql_repair`` for months)
    parses cleanly, ``config.features.<name>`` comes back None, and the feature is
    disabled without a trace. A typo'd or unregistered key now fails loud at boot.
    """

    model_config = ConfigDict(extra="forbid")

    # Active features with implementations
    semantic_analysis: FeatureConfig
    column_annotation: FeatureConfig | None = None
    slicing_analysis: FeatureConfig | None = None
    validation: FeatureConfig | None = None
    business_cycles: FeatureConfig | None = None
    entropy_query_interpretation: FeatureConfig | None = None
    enrichment_analysis: FeatureConfig | None = None
    why_analysis: FeatureConfig | None = None
    # Metric grounding (GraphAgent._generate_sql) — tier + effort for the
    # pipeline's most central agent (DAT-603). None keeps the built-in defaults
    # (balanced tier, API-default effort).
    graph_sql_generation: FeatureConfig | None = None
    # LLM repair of failed step SQL (GraphAgent._repair_sql).
    sql_repair: FeatureConfig | None = None


class LLMLimits(BaseModel):
    """Cost control limits."""

    max_output_tokens_per_request: int = 16000


class LLMPrivacy(BaseModel):
    """Privacy settings for data sent to LLM."""

    max_sample_values: int = 10
    redacted_sample_count: int = 3
    sensitive_patterns: list[str] = Field(default_factory=list)


class LLMConfig(BaseModel):
    """Complete LLM configuration from llm.yaml."""

    version: str = "1.0.0"
    providers: dict[str, ProviderConfig]
    active_provider: str
    features: LLMFeatures
    limits: LLMLimits
    privacy: LLMPrivacy


def load_llm_config(config_path: Path | None = None) -> LLMConfig:
    """Load LLM configuration from YAML.

    Args:
        config_path: Path to llm.yaml. If None, uses config/llm.yaml

    Returns:
        Parsed LLM configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If YAML is invalid
        pydantic.ValidationError: If config doesn't match schema
    """
    if config_path is None:
        from dataraum.core.config import get_config_file

        config_path = get_config_file("llm/config.yaml")

    if not config_path.exists():
        raise FileNotFoundError(
            f"LLM config not found: {config_path}. Create config/llm.yaml from the template."
        )

    with open(config_path) as f:
        data = yaml.safe_load(f)

    return LLMConfig(**data)
