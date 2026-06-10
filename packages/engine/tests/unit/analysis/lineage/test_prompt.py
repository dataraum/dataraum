"""The aggregation_lineage prompt template renders with the agent's variables.

``PromptRenderer._prepare_context`` passes through ONLY variables declared in
the template's ``inputs:`` block — a template/agent mismatch renders fine in
mocked-agent unit tests and fails only on a live run ("Available context: []",
how DAT-491's first e2e died). This renders the REAL template with the exact
context keys ``AggregationLineageAgent.propose`` passes.
"""

from __future__ import annotations

from dataraum.llm import PromptRenderer


def test_template_renders_with_agent_context() -> None:
    system, user, temperature = PromptRenderer().render_split(
        "aggregation_lineage", {"schema": "SCHEMA_BLOCK", "entities": "ENTITIES_BLOCK"}
    )
    assert system is not None and "MEASURE" in system
    assert "SCHEMA_BLOCK" in user
    assert "ENTITIES_BLOCK" in user
    assert temperature == 0.0
