"""Tests for LLM configuration loading."""

from dataraum_context.llm.config import load_llm_config


def test_load_llm_config():
    """Test loading LLM configuration from YAML."""
    config = load_llm_config()

    assert config.active_provider in config.providers
    assert config.providers
    assert config.features
    assert config.limits
    assert config.privacy


def test_llm_config_has_anthropic():
    """Test that Anthropic provider is configured."""
    config = load_llm_config()

    assert "anthropic" in config.providers
    provider = config.providers["anthropic"]
    assert provider.api_key_env == "ANTHROPIC_API_KEY"
    assert provider.default_model
    assert "fast" in provider.models
    assert "balanced" in provider.models


def test_llm_features_enabled():
    """Test that LLM features are configured."""
    config = load_llm_config()

    # Core implemented features
    assert config.features.semantic_analysis.enabled
    assert config.features.slicing_analysis is not None
    assert config.features.quality_summary is not None
    assert config.features.validation is not None
    assert config.features.entropy_interpretation is not None
