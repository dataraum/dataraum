"""LLM configuration models and loader.

Loads configuration from config/llm.yaml and provides typed access
to all LLM settings: providers, features, limits, privacy.
"""

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ProviderConfig(BaseModel):
    """Configuration for a single LLM provider."""

    api_key_env: str
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


class LLMFeatures(BaseModel):
    """All LLM features configuration."""

    # Active features with implementations
    semantic_analysis: FeatureConfig
    column_annotation: FeatureConfig | None = None
    slicing_analysis: FeatureConfig | None = None
    quality_summary: FeatureConfig | None = None
    validation: FeatureConfig | None = None
    business_cycles: FeatureConfig | None = None
    entropy_interpretation: FeatureConfig | None = None
    entropy_query_interpretation: FeatureConfig | None = None
    enrichment_analysis: FeatureConfig | None = None
    vertical_creation: FeatureConfig | None = None


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
