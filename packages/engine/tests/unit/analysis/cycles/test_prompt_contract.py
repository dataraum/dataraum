"""The business_cycles prompt contract, pinned against the shipped YAML.

The DAT-853 AP forensics found the judge inventing a direction: it discounted
a served counterparty-code annotation because of its low annotation_confidence
and defaulted to the wrong direction reading. The tie-break rules below are the
fix's wording half (the evidence half is the chain-conditioned label + measure
serving) — these tests read the REAL config template so a prompt edit that
softens them fails here instead of in an eval run.

The rule is deliberately GENERIC: the engine and its prompts stay domain-free,
and who-owes-whom semantics for direction-typed cycle types live only in the
vertical's declared config data (served via the DOMAIN KNOWLEDGE section). The
tripwire test at the bottom enforces that boundary verbatim.
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
    assert "chain-conditioned measure ranges" in system
    assert "the flow sign the chain itself carries" in system


def test_counterparty_direction_tie_break_is_hardened(cycles: PromptTemplate) -> None:
    """The DAT-853 evasion closers, all present — in domain-free wording:

    1. for vertical-declared direction-typed cycle types, a coded counterparty
       axis plus chain-conditioned evidence outweighs flow-shape priors AND
       annotation-confidence discounting;
    2. low confidence on a code annotation is never a license to invert it;
    3. with no decisive direction evidence, no default to ANY reading — the
       judge states what served evidence would decide the direction.
    """
    system = _flat(cycles.system_prompt)
    assert "the DOMAIN KNOWLEDGE section declares who-owes-whom semantics" in system
    assert "decisive direction evidence" in system
    assert "read per the vertical's declared semantics" in system
    assert "outweighs flow-shape priors" in system
    assert "outweighs annotation-confidence discounting" in system
    assert "human-correctable evidence" in system
    assert "never a license to invert it" in system
    assert "do not default to any reading" in system
    assert "what served evidence would decide the direction" in system


def test_the_prior_entity_evidence_rules_survive(cycles: PromptTemplate) -> None:
    """Hardening extends the existing discipline — it must not displace it."""
    system = _flat(cycles.system_prompt)
    assert "Do not carry an upstream hedge" in system
    assert "never the flow shape alone" in system


def test_direction_axis_output_contract_is_pinned(cycles: PromptTemplate) -> None:
    """The direction axis has a STRUCTURED output home (DAT-856): the judge is told to
    set family + direction from the CYCLE FAMILIES section, and that `undetermined` is
    the honest detected-but-undirected answer — all GENERIC (the leak tripwire below
    proves the family/member vocabulary stays DATA, never hardcoded here)."""
    system = _flat(cycles.system_prompt)
    assert "CYCLE FAMILIES section" in system
    assert "set `family`" in system
    assert "honest detected-but-undirected answer" in system
    assert "never guess a label" in system
    assert "at most ONE cycle per declared family" in system


def test_generic_prompt_carries_no_domain_vocabulary() -> None:
    """Leak tripwire (DAT-853 lead ruling): the generic prompt is domain-free.

    Finance direction semantics live ONLY in the finance vertical's declared
    cycles config; a W4 lane once leaked them into this generic template. The
    raw FILE is checked (not the parsed template) so a leak in any key —
    description, comments, examples — trips it.
    """
    raw = (PromptRenderer().prompts_dir / "business_cycles.yaml").read_text().lower()
    for leaked in (
        "payable",
        "receivable",
        "vendor",
        "order-to-cash",
        "order_to_cash",
        "supplier",
        "customer",
        "invoice",
    ):
        assert leaked not in raw, f"domain vocabulary leaked into the generic prompt: {leaked!r}"
