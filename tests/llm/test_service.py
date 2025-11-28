"""Tests for LLM service initialization."""

import pytest

from dataraum_context.llm import LLMService, load_llm_config


@pytest.fixture
def mock_anthropic_key(monkeypatch):
    """Mock Anthropic API key for testing."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key-for-testing")


def test_llm_service_initialization(mock_anthropic_key):
    """Test that LLM service initializes correctly."""
    config = load_llm_config()
    service = LLMService(config)

    assert service.config == config
    assert service.provider is not None
    assert service.cache is not None
    assert service.renderer is not None
    assert service.semantic is not None
    assert service.quality is not None
    assert service.queries is not None
    assert service.summary is not None


def test_llm_service_with_invalid_provider(mock_anthropic_key):
    """Test that LLM service fails gracefully with invalid provider."""
    config = load_llm_config()
    config.active_provider = "nonexistent"

    with pytest.raises(ValueError, match="Active provider.*not found"):
        LLMService(config)


def test_prompt_renderer_loads_templates(mock_anthropic_key):
    """Test that prompt renderer can load templates."""
    config = load_llm_config()
    service = LLMService(config)

    # Load semantic analysis template
    template = service.renderer.load_template("semantic_analysis")
    assert template.name == "semantic_analysis"
    assert template.prompt
    assert template.temperature >= 0


def test_prompt_renderer_renders_template(mock_anthropic_key):
    """Test that prompt renderer can render with context."""
    config = load_llm_config()
    service = LLMService(config)

    context = {
        "tables_json": "[]",
        "ontology_name": "test",
        "ontology_concepts": "None",
    }

    rendered, temperature = service.renderer.render("semantic_analysis", context)

    assert rendered
    assert "[]" in rendered
    assert temperature == 0.0
