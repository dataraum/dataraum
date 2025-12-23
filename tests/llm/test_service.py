"""Tests for LLM infrastructure initialization."""

import pytest

# Skip all tests if anthropic is not installed
pytest.importorskip("anthropic", reason="anthropic package not installed")

from dataraum_context.llm import LLMCache, PromptRenderer, create_provider, load_llm_config


@pytest.fixture
def mock_anthropic_key(monkeypatch):
    """Mock Anthropic API key for testing."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key-for-testing")


def test_llm_config_loads(mock_anthropic_key):
    """Test that LLM config loads correctly."""
    config = load_llm_config()

    assert config is not None
    assert config.active_provider == "anthropic"
    assert "anthropic" in config.providers


def test_llm_provider_creation(mock_anthropic_key):
    """Test that LLM provider can be created."""
    config = load_llm_config()
    provider_config = config.providers[config.active_provider]
    provider = create_provider(config.active_provider, provider_config.model_dump())

    assert provider is not None


def test_llm_cache_initialization():
    """Test that LLM cache initializes correctly."""
    cache = LLMCache()
    assert cache is not None


def test_prompt_renderer_loads_templates(mock_anthropic_key):
    """Test that prompt renderer can load templates."""
    renderer = PromptRenderer()

    # Load semantic analysis template
    template = renderer.load_template("semantic_analysis")
    assert template.name == "semantic_analysis"
    # Template can use either legacy `prompt` or new `system_prompt`/`user_prompt` format
    assert template.prompt or (template.system_prompt or template.user_prompt)
    assert template.temperature >= 0


def test_prompt_renderer_renders_template(mock_anthropic_key):
    """Test that prompt renderer can render with context."""
    renderer = PromptRenderer()

    context = {
        "tables_json": "[]",
        "ontology_name": "financial_reporting",
        "ontology_concepts": "- revenue: Total income from sales",
        "relationship_candidates": "No candidates detected",
        "within_table_correlations": "No correlations detected",
    }

    rendered, temperature = renderer.render("semantic_analysis", context)

    assert rendered
    assert "[]" in rendered
    assert temperature == 0.0
