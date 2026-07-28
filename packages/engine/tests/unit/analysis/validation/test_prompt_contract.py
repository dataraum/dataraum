"""The validation induction + binder prompt contracts, pinned against the shipped YAML.

DAT-876: generated existence checks graded structural absence as a violation. The
root cause was a loophole shared by BOTH prompts — an existence check could bind
against "the declared target of a served reference/relationship", which a
deterministically-detected SELF-referential hierarchy edge (a parent-id column
pointing back into its own fact table) satisfied, making the activity table its own
"enumerator". The fix's wording half closes that branch: a served reference qualifies
its target as an enumerator ONLY when that target is itself dimension-role; a
self-referential edge on a fact/snapshot table never establishes an existence
universe. (Fix (a) is the wording layer that fully closes the self-referential path
and narrows the reference branch to dimension-role targets; fix (b) serves the
no-enumerator absence as a positive fact.) These tests read the REAL config
templates so a prompt edit that reopens the loophole fails here, not in an eval run.

The rules stay GENERIC: the engine and its prompts are domain-free (verticals supply
their data as served config), enforced by the leak tripwire at the bottom.
"""

from __future__ import annotations

import pytest

from dataraum.llm.prompts import PromptRenderer, PromptTemplate


def _flat(text: str) -> str:
    """Whitespace-normalized view — YAML line wrapping is not part of the contract."""
    return " ".join(text.split())


@pytest.fixture(scope="module")
def induction() -> PromptTemplate:
    return PromptRenderer().load_template("validation_induction")


@pytest.fixture(scope="module")
def binder() -> PromptTemplate:
    return PromptRenderer().load_template("validation_sql")


def test_induction_renders_with_declared_inputs() -> None:
    system, user, temperature = PromptRenderer().render_split(
        "validation_induction", {"served_graph": "graph", "conventions": "None"}
    )
    assert system and user
    assert temperature == 0.0


def test_binder_renders_with_declared_inputs() -> None:
    system, user, temperature = PromptRenderer().render_split(
        "validation_sql",
        {
            "spec_name": "n",
            "spec_description": "d",
            "check_type": "constraint",
            "parameters": "None",
            "schema": "<tables></tables>",
        },
    )
    assert system and user
    assert temperature == 0.0


def test_induction_existence_loophole_is_closed(induction: PromptTemplate) -> None:
    """The served-reference branch qualifies ONLY a dimension-role target, and the
    self-referential activity-table edge is named as the excluded anti-pattern."""
    system = _flat(induction.system_prompt)
    assert "a table whose role is DIMENSION" in system
    assert "qualifies its TARGET table as such an enumerator ONLY when that target" in system
    assert "target is itself dimension-role" in system
    assert "self-referential edge whose target is the same activity table" in system
    assert "never establishes an enumerating universe" in system
    assert "When no served table enumerates the entity, do not propose the check." in system


def test_binder_existence_loophole_is_closed(binder: PromptTemplate) -> None:
    """The binder's <existence_checks> block closes the same branch: a served
    relationship enumerates only through a dimension-role target, self-ref excluded."""
    system = _flat(binder.system_prompt)
    assert 'a table whose role is dimension (role="dimension")' in system
    assert "qualifies its TARGET table as such an enumerator ONLY when that target" in system
    assert 'that target is itself role="dimension"' in system
    assert "self-referential edge whose target is the SAME activity table" in system
    assert "does NOT establish an enumerating universe" in system
    assert "no dimension-role table that enumerates the referenced entity" in system


def test_induction_temporal_form_floor_is_pinned(induction: PromptTemplate) -> None:
    """DAT-874: a movement is never proposed directly against a level, and the
    coherent alternatives are named so the rule redirects instead of only forbidding.

    The floor shipped prompt-only, with nothing pinning it — a later revision could
    have dropped it and only an eval run would have noticed.
    """
    system = _flat(induction.system_prompt)
    assert "TEMPORAL-FORM coherence" in system
    assert "flow = a per-period movement (additive across periods)" in system
    assert "stock = a level as of its period" in system
    assert (
        "NEVER compare a per-period flow (or a net of flows) directly against a stock level"
        in system
    )
    # The coherent redirects, including the completeness condition on cumulation.
    assert "flow vs the CHANGE in a stock between consecutive periods" in system
    assert "the flow's history is complete from the stock's origin" in system


def test_induction_undetermined_form_is_a_stated_absence(induction: PromptTemplate) -> None:
    """The UNDETERMINED case reads off a SERVED statement, not a blank cell (DAT-874).

    ``format_served_context`` renders a missing Materialization verdict as an empty
    table cell; ``induction._render_temporal_form`` names those measures instead, and
    the rule points at that statement — the DAT-876 absence-falls-loud doctrine.
    """
    system = _flat(induction.system_prompt)
    assert 'restated per table under "Temporal form of the measures"' in system
    assert "list as having NO temporal-form verdict is UNDETERMINED" in system
    assert "that absence is STATED, never to be read as flow" in system


def test_binder_temporal_form_floor_is_pinned(binder: PromptTemplate) -> None:
    """The binder carries the same floor (DAT-874): a bound check cannot repair a
    coherent proposal into an incoherent comparison, and an unbindable one declares
    can_validate=false rather than guessing a form."""
    system = _flat(binder.system_prompt)
    assert "Same temporal form: both sides must be the same KIND of quantity" in system
    assert (
        "A per-period movement (or a net of movements) NEVER compares directly against a level"
        in system
    )
    assert "the CHANGE in the level between consecutive periods" in system
    assert "movement history is complete from the level's origin" in system
    assert "If neither coherent form is groundable from the served facts, set can_validate=false" in (
        system
    )
    assert "Never aggregate a point_in_time column across periods." in _flat(system)


def test_validation_prompts_carry_no_domain_vocabulary() -> None:
    """Leak tripwire: the generic validation prompts are domain-free (DAT-876 fence).

    Vertical vocabulary reaches these prompts ONLY as served data (the {served_graph},
    {schema}, {conventions} slots) — never hardcoded in the template. The raw FILE is
    checked (not the parsed template) so a leak in any key — description, comments,
    guidelines, examples — trips it. (``customer`` is excluded: it appears solely as a
    generic column-quoting example, "Customer ID", not as corpus vocabulary.)
    """
    renderer = PromptRenderer()
    for name in ("validation_induction", "validation_sql"):
        raw = (renderer.prompts_dir / f"{name}.yaml").read_text().lower()
        for leaked in (
            "account",
            "invoice",
            "ledger",
            "journal",
            "trial balance",
            "receivable",
            "payable",
            "vendor",
            "supplier",
            "debit",
            "credit",
            "posting",
        ):
            assert leaked not in raw, f"domain vocabulary leaked into {name}.yaml: {leaked!r}"
