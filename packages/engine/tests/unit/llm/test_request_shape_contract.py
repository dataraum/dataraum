"""Per-label request-shape contract (DAT-807).

Pins, for every LLM label the engine ships, the EXACT kwargs the provider
chokepoint hands to the Anthropic client — ``model``, ``max_tokens``,
``output_config.effort``, ``thinking``, ``temperature`` — read from the REAL
``llm/config.yaml`` rather than a fixture, so a config edit that silently moves
an agent's configuration fails here instead of in an eval run.

Why this exists: the DAT-807 mechanism swap (forced ``tool_choice`` →
``output_config.format``) had to be a pure mechanism change. The eval compares
one post-change run against the on-disk baseline, so ANY drift in model, token
budget, effort, thinking, or temperature would confound the comparison. The
last test states that invariant directly: the only structured-output-related
difference in the request is ``output_config.format``.

These are the CONFIGURED values, not aspirations — update the table only
together with a deliberate config change.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dataraum.llm.config import LLMConfig, load_llm_config
from dataraum.llm.providers.anthropic import AnthropicConfig, AnthropicProvider
from dataraum.llm.providers.base import ConversationRequest, Message

# label -> (feature key on LLMFeatures, model tier)
#
# ``dimension_conform`` and ``dimension_alias`` are two labels over ONE feature
# entry (the judge runs both templates); ``why_analysis`` is the cockpit's, not
# an engine agent, so it is deliberately absent.
_LABELS: list[tuple[str, str]] = [
    ("semantic_per_table", "semantic_analysis"),
    ("column_annotation", "column_annotation"),
    ("slicing_analysis", "slicing_analysis"),
    ("business_cycles", "business_cycles"),
    ("enrichment_analysis", "enrichment_analysis"),
    ("dimension_conform", "dimension_identity_judgment"),
    ("dimension_alias", "dimension_identity_judgment"),
    ("validation_sql", "validation"),
    ("graph_sql_generation", "graph_sql_generation"),
]

# The configuration each label MUST run with. Effort is declared for every agent
# (DAT-807) — no label inherits the server-side default any more.
_EXPECTED: dict[str, dict[str, Any]] = {
    "semantic_per_table": {"tier": "balanced", "effort": "high", "thinking": False},
    "column_annotation": {"tier": "balanced", "effort": "low", "thinking": False},
    "slicing_analysis": {"tier": "balanced", "effort": "medium", "thinking": False},
    "business_cycles": {"tier": "balanced", "effort": "high", "thinking": False},
    "enrichment_analysis": {"tier": "balanced", "effort": "high", "thinking": False},
    "dimension_conform": {"tier": "balanced", "effort": "high", "thinking": False},
    "dimension_alias": {"tier": "balanced", "effort": "high", "thinking": False},
    "validation_sql": {"tier": "balanced", "effort": "low", "thinking": False},
    "graph_sql_generation": {"tier": "balanced", "effort": "high", "thinking": True},
}

# A schema shaped like the agents' — one required string — so the assertions are
# about the request, not about any particular agent's model.
_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
}


@pytest.fixture(scope="module")
def config() -> LLMConfig:
    return load_llm_config()


def _provider(config: LLMConfig) -> AnthropicProvider:
    provider_config = config.providers[config.active_provider]
    return AnthropicProvider(
        AnthropicConfig(default_model=provider_config.default_model, models=provider_config.models),
        "sk-ant-test",
    )


def _capture(
    monkeypatch: pytest.MonkeyPatch, provider: AnthropicProvider, request: ConversationRequest
) -> dict[str, Any]:
    """The exact kwargs ``converse`` hands to ``client.messages.stream``."""
    captured: dict[str, Any] = {}

    def stream(**kwargs: Any) -> Any:
        captured.update(kwargs)
        response = SimpleNamespace(
            content=[SimpleNamespace(type="text", text='{"answer": "x"}')],
            stop_reason="end_turn",
            model=kwargs["model"],
            usage=SimpleNamespace(
                input_tokens=1,
                output_tokens=1,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
            ),
        )
        return _FakeStream(response)

    monkeypatch.setattr(provider.client.messages, "stream", stream)
    provider.converse(request).unwrap()
    return captured


class _FakeStream:
    def __init__(self, response: Any) -> None:
        self._response = response

    def __enter__(self) -> Any:
        return SimpleNamespace(get_final_message=lambda: self._response)

    def __exit__(self, *exc: object) -> bool:
        return False


def _request_for(label: str, config: LLMConfig, *, with_schema: bool) -> ConversationRequest:
    """The request the label's agent builds, assembled from the REAL config."""
    expected = _EXPECTED[label]
    provider_config = config.providers[config.active_provider]
    return ConversationRequest(
        messages=[Message(role="user", content="user prompt")],
        system="system prompt",
        model=provider_config.models[expected["tier"]],
        max_tokens=config.limits.max_output_tokens_per_request,
        effort=expected["effort"],
        thinking=expected["thinking"],
        temperature=0.0,
        label=label,
        output_schema=_SCHEMA if with_schema else None,
    )


@pytest.mark.parametrize("label,feature_key", _LABELS)
def test_configured_values_match_the_pinned_contract(
    label: str, feature_key: str, config: LLMConfig
) -> None:
    """The shipped ``llm/config.yaml`` still says what this table says."""
    feature = getattr(config.features, feature_key)
    assert feature is not None, f"{feature_key} is not configured"
    expected = _EXPECTED[label]
    assert feature.model_tier == expected["tier"]
    assert feature.effort == expected["effort"], (
        f"{label}: effort must stay declared and pinned (DAT-807) — no label "
        "may fall back to the server-side default"
    )
    assert feature.thinking == expected["thinking"]


@pytest.mark.parametrize("label,_feature_key", _LABELS)
def test_request_kwargs_are_pinned_per_label(
    label: str, _feature_key: str, config: LLMConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The chokepoint builds exactly these kwargs — nothing more, nothing less."""
    provider = _provider(config)
    kwargs = _capture(monkeypatch, provider, _request_for(label, config, with_schema=True))
    expected = _EXPECTED[label]
    model = config.providers[config.active_provider].models[expected["tier"]]

    assert kwargs["model"] == model
    assert kwargs["max_tokens"] == config.limits.max_output_tokens_per_request
    assert kwargs["output_config"]["effort"] == expected["effort"]
    assert kwargs["output_config"]["format"]["type"] == "json_schema"
    # Sonnet 5-class models reject a non-default temperature (400); the whole
    # engine tier runs on one, so temperature must never be sent.
    assert "temperature" not in kwargs
    # Thinking is decided EXPLICITLY per request, never by the model default —
    # the defaults differ across the family.
    assert kwargs["thinking"] == (
        {"type": "adaptive"} if expected["thinking"] else {"type": "disabled"}
    )


@pytest.mark.parametrize("label,_feature_key", _LABELS)
def test_only_structured_output_difference_is_output_config_format(
    label: str, _feature_key: str, config: LLMConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The DAT-807 invariant, asserted directly.

    Two requests identical except for ``output_schema`` must produce kwargs that
    differ ONLY by ``output_config["format"]``. If anything else moves — model,
    max_tokens, effort, thinking, temperature, tools, tool_choice — the mechanism
    swap stopped being a mechanism swap and the eval comparison is confounded.
    """
    provider = _provider(config)
    with_schema = _capture(monkeypatch, provider, _request_for(label, config, with_schema=True))
    without = _capture(monkeypatch, provider, _request_for(label, config, with_schema=False))

    assert set(with_schema) == set(without)
    for key in without:
        if key == "output_config":
            continue
        assert with_schema[key] == without[key], f"{label}: {key} moved with the output schema"

    assert set(with_schema["output_config"]) - set(without["output_config"]) == {"format"}
    assert with_schema["output_config"]["effort"] == without["output_config"]["effort"]


def test_a_request_without_tools_sends_neither_tools_nor_tool_choice(
    config: LLMConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Structured output alone needs no tool surface at all.

    This asserts the CHOKEPOINT, not the agents: a request carrying only an
    output schema must not grow a ``tools``/``tool_choice`` key. That the eight
    tool-less agents actually build such a request — and that
    ``graph_sql_generation`` builds one WITH ``search_values`` on ``auto`` — is
    pinned per agent in the agent tests (test_synthesis_output.py,
    test_judge.py, test_contract_repair.py, test_agent_request_shape.py); a
    fixture here could only ever assert itself.
    """
    provider = _provider(config)
    kwargs = _capture(
        monkeypatch, provider, _request_for("semantic_per_table", config, with_schema=True)
    )
    assert "tool_choice" not in kwargs
    assert "tools" not in kwargs
