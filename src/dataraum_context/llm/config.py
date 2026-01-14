"""LLM configuration models and loader.

Loads configuration from config/llm.yaml and provides typed access
to all LLM settings: providers, features, limits, privacy.
"""

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class ProviderConfig(BaseModel):
    """Configuration for a single LLM provider."""

    api_key_env: str
    default_model: str
    models: dict[str, str]
    base_url_env: str | None = None  # For local providers


class FeatureConfig(BaseModel):
    """Configuration for an LLM feature."""

    enabled: bool = True
    model_tier: str = "balanced"
    prompt_file: str | None = None  # Some features use inline prompts
    description: str = ""


class LLMFeatures(BaseModel):
    """All LLM features configuration."""

    # Active features with implementations
    semantic_analysis: FeatureConfig
    slicing_analysis: FeatureConfig | None = None
    quality_summary: FeatureConfig | None = None
    validation: FeatureConfig | None = None
    entropy_interpretation: FeatureConfig | None = None
    entropy_query_interpretation: FeatureConfig | None = None


class LLMLimits(BaseModel):
    """Cost and rate control limits."""

    max_input_tokens_per_request: int = 8000
    max_output_tokens_per_request: int = 4000
    max_columns_per_batch: int = 30
    max_requests_per_minute: int = 20
    cache_ttl_seconds: int = 86400  # 24 hours


class LLMPrivacy(BaseModel):
    """Privacy settings for data sent to LLM."""

    max_sample_values: int = 10
    use_synthetic_samples: bool = False  # SDV integration (future)
    synthetic_sample_count: int = 20
    sensitive_patterns: list[str] = Field(default_factory=list)


class LLMConfig(BaseModel):
    """Complete LLM configuration from llm.yaml."""

    version: str = "1.0.0"
    providers: dict[str, ProviderConfig]
    active_provider: str
    features: LLMFeatures
    limits: LLMLimits
    privacy: LLMPrivacy
    fallback: dict[str, str] = Field(default_factory=dict)


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
        config_path = Path("config/llm.yaml")

    if not config_path.exists():
        raise FileNotFoundError(
            f"LLM config not found: {config_path}. Create config/llm.yaml from the template."
        )

    with open(config_path) as f:
        data = yaml.safe_load(f)

    return LLMConfig(**data)
