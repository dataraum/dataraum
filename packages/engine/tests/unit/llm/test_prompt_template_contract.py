"""The PromptTemplate schema contract: every key required, no key unknown.

Prompt templates are hand-authored YAML in ``dataraum-config/llm/prompts/``. Both
halves of the contract exist so an authoring mistake fails at load instead of
shipping a prompt that looks fine and is missing a piece:

- a MISSING key raises, so ``system_prompt``/``user_prompt`` can never render empty
- an UNKNOWN key raises, so a misspelled ``validation``/``inputs``/``output_schema``
  is not silently dropped to its ``{}`` default

Both are load-time failures; there is no path where a malformed template reaches a
provider call.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataraum.llm.prompts import PromptTemplate

_VALID: dict[str, object] = {
    "name": "t",
    "version": "1.0.0",
    "description": "d",
    "temperature": 0.0,
    "system_prompt": "sys",
    "user_prompt": "usr",
}


def test_minimal_template_loads() -> None:
    """The six required scalars are sufficient; the three dict fields default."""
    t = PromptTemplate(**_VALID)
    assert t.inputs == {}
    assert t.output_schema == {}
    assert t.validation == {}


@pytest.mark.parametrize("missing", ["system_prompt", "user_prompt"])
def test_missing_prompt_half_raises(missing: str) -> None:
    """Neither half may be absent — the single-``prompt`` fallback is gone."""
    data = {k: v for k, v in _VALID.items() if k != missing}
    with pytest.raises(ValidationError):
        PromptTemplate(**data)


def test_unknown_key_raises() -> None:
    """A typo'd field is an error, not a key to ignore.

    ``validaton`` would otherwise be dropped and ``validation`` would default to
    ``{}`` — the template ships without its rules and nothing says so.
    """
    with pytest.raises(ValidationError):
        PromptTemplate(**_VALID, validaton={"x": ["y"]})
