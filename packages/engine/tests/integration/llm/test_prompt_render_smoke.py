"""Real-render smoke for the SQL-authoring prompts (DAT-645).

The unit tests mock the renderer, so a placeholder in a prompt's body that is NOT
declared in its `inputs:` section is invisible to them — the renderer drops
undeclared keys and then raises KeyError at substitution, which the agents swallow
into a silent Result.fail. These tests render the REAL config prompts with a minimal
context, proving every declared placeholder substitutes (no inputs/body mismatch) and
that the conventions block actually reaches the rendered text.
"""

from __future__ import annotations

import pytest

from dataraum.llm.prompts import PromptRenderer

_MARKER = "SIGN_RULE_MARKER"


def _ctx_for(template, **extra) -> dict[str, str]:
    """Minimal context satisfying the template's required inputs, plus overrides."""
    ctx = {name: "x" for name, spec in template.inputs.items() if spec.get("required")}
    ctx.update(extra)
    return ctx


@pytest.mark.parametrize(
    ("prompt_name", "conventions_key"),
    [
        ("graph_sql_generation", "vertical_conventions"),
        ("validation_sql", "conventions"),
    ],
)
def test_sql_prompt_renders_and_pipes_conventions(prompt_name: str, conventions_key: str) -> None:
    renderer = PromptRenderer()
    template = renderer.load_template(prompt_name)
    # The conventions key MUST be a declared (optional) input — else it is dropped and
    # the {placeholder} in the body raises KeyError at render.
    assert conventions_key in template.inputs, (
        f"{prompt_name}: '{conventions_key}' placeholder is in the body but not declared "
        f"in inputs: — the renderer would drop it and KeyError at substitution"
    )
    ctx = _ctx_for(template, **{conventions_key: _MARKER})

    system, user, _temperature = renderer.render_split(prompt_name, ctx)

    # Real substitution happened (no KeyError) and the conventions reached the prompt.
    assert _MARKER in (system + user)


@pytest.mark.parametrize("prompt_name", ["graph_sql_generation", "validation_sql"])
def test_sql_prompt_renders_with_empty_conventions(prompt_name: str) -> None:
    """The conventions input is optional — rendering with none declared still works."""
    renderer = PromptRenderer()
    template = renderer.load_template(prompt_name)
    # Required-only context (conventions omitted → its default "" fills the slot).
    renderer.render_split(prompt_name, _ctx_for(template))


def test_every_shipped_template_satisfies_the_schema() -> None:
    """Every prompt in dataraum-config loads under the strict schema.

    ``PromptTemplate`` forbids unknown keys, so this is the test that catches a
    typo'd field in a real prompt file — the unit contract test proves the model
    rejects one, this proves none of the shipped templates HAS one.
    """
    renderer = PromptRenderer()
    names = sorted(p.stem for p in renderer.prompts_dir.glob("*.yaml"))
    assert names, "no prompt templates found — wrong config dir?"

    for name in names:
        template = renderer.load_template(name)  # raises ValidationError on any defect
        assert template.system_prompt, f"{name}: empty system_prompt"
        assert template.user_prompt, f"{name}: empty user_prompt"
