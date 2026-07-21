"""The business_cycles prompt contract, pinned against the shipped YAML.

The DAT-853 AP forensics found the judge inventing a direction: it discounted
a served vendor-code annotation because of its low annotation_confidence and
defaulted to the receivable reading. The tie-break rules below are the fix's
wording half (the evidence half is the chain-conditioned label serving) — these
tests read the REAL config template so a prompt edit that softens them fails
here instead of in an eval run.
"""

from __future__ import annotations

import pytest

from dataraum.llm.prompts import PromptRenderer, PromptTemplate


def _flat(text: str) -> str:
    """Whitespace-normalized view — YAML line wrapping is not part of the contract."""
    return " ".join(text.split())


@pytest.fixture(scope="module")
def cycles() -> PromptTemplate:
    return PromptRenderer().load_template("business_cycles")


def test_renders_with_the_declared_input() -> None:
    system, user, temperature = PromptRenderer().render_split(
        "business_cycles", {"context": "metadata"}
    )
    assert system and user
    assert temperature == 0.0


def test_names_the_chain_conditioned_evidence(cycles: PromptTemplate) -> None:
    """The context description tells the judge what a conditioned line IS —
    served evidence it was never taught to read stays unread."""
    system = _flat(cycles.system_prompt)
    assert "chain-conditioned label samples" in system
    assert "ONLY the rows that resolve across that join" in system


def test_counterparty_direction_tie_break_is_hardened(cycles: PromptTemplate) -> None:
    """The DAT-853 evasion closers, all present:

    1. a coded counterparty/vendor axis outweighs flow-shape priors AND
       annotation-confidence discounting;
    2. low confidence on a code annotation is never a license to invert it;
    3. with no decisive direction evidence, no order-to-cash default — the
       judge states what served evidence would decide the direction.
    """
    system = _flat(cycles.system_prompt)
    assert "decisive direction evidence" in system
    assert "outweighs flow-shape priors" in system
    assert "outweighs annotation-confidence discounting" in system
    assert "human-correctable evidence" in system
    assert "never a license to invert it" in system
    assert "If the counterparty axis says vendors, the invoices are payables" in system
    assert "do not default to the order-to-cash / receivable reading" in system
    assert "what would decide the direction" in system


def test_the_prior_entity_evidence_rules_survive(cycles: PromptTemplate) -> None:
    """Hardening extends the existing discipline — it must not displace it."""
    system = _flat(cycles.system_prompt)
    assert "Do not carry an upstream hedge" in system
    assert "never the flow shape alone" in system
