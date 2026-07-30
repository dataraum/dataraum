"""The taxonomy reaches BOTH table-grain agents as evidence (DAT-724).

The two table-level facts are produced in different phases since DAT-823 —
``is_fact_table`` by the per-table structural agent, ``detected_entity_type`` by the
catalogue agent — so both prompts carry the declaration.

These assert on the ASSEMBLED CONTEXT with the real store behind it (seeded rows →
``load_workspace_concepts`` → the prompt formatter), because that whole chain is
where a regression would land: a lift dropped from ``model_construct``, a context key
renamed, a formatter that stops emitting roles. Each test stops the agent AT the
renderer — the subject is what the agent assembled, not what a stubbed model would
have replied.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

from sqlalchemy.orm import Session

from dataraum.analysis.catalogue.agent import CatalogueSemanticsAgent
from dataraum.analysis.cycles.cycle_type_store import ensure_cycle_types_seeded
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.concept_store import ensure_concepts_seeded
from dataraum.analysis.semantic.entity_store import ensure_entities_seeded
from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.core.models.base import ColumnRef, Result

_STOP = "stop after context assembly"


def _capturing_renderer(captured: dict[str, Any]) -> MagicMock:
    """A renderer that records the context, then stops the turn.

    Raising here is a HANDLED branch in both agents (each wraps ``render_split`` and
    returns ``Result.fail``), so the agent stays on a real code path — no LLM output
    has to be modelled just to inspect the inputs.
    """

    def _capture(_name: str, context: dict[str, Any]) -> tuple[str, str]:
        captured.update(context)
        raise RuntimeError(_STOP)

    renderer = MagicMock()
    renderer.render_split.side_effect = _capture
    return renderer


def _config() -> MagicMock:
    config = MagicMock()
    config.features.semantic_analysis = MagicMock(enabled=True, model_tier="balanced", effort=None)
    config.limits.max_output_tokens_per_request = 8192
    config.privacy.max_sample_values = 10
    config.privacy.max_sample_value_chars = 100
    return config


def _seed_finance(session: Session) -> None:
    ensure_concepts_seeded(session, "finance")
    ensure_cycle_types_seeded(session, "finance")
    ensure_entities_seeded(session, "finance")


def _profiles() -> list[ColumnProfile]:
    """One real profile — the per-table agent refuses an empty load before it
    assembles any context, so the taxonomy would never be reached."""
    return [
        ColumnProfile(
            column_id="journal_lines.debit",
            column_ref=ColumnRef(table_name="journal_lines", column_name="debit"),
            profiled_at=datetime.now(UTC),
            total_count=100,
            null_count=0,
            distinct_count=50,
            null_ratio=0.0,
            cardinality_ratio=0.5,
        )
    ]


def _per_table_context(session: Session, ontology: str) -> dict[str, Any]:
    captured: dict[str, Any] = {}
    agent = SemanticAgent(_config(), MagicMock(), _capturing_renderer(captured))
    with patch.object(SemanticAgent, "_load_profiles", return_value=Result.ok(_profiles())):
        result = agent.synthesize_tables(session, ["t1"], ontology=ontology)
    assert not result.success  # the renderer stopped the turn, as designed
    return captured


def _catalogue_context(session: Session, ontology: str) -> dict[str, Any]:
    captured: dict[str, Any] = {}
    agent = CatalogueSemanticsAgent(_config(), MagicMock(), _capturing_renderer(captured))
    with (
        patch("dataraum.analysis.catalogue.agent.build_catalogue_inputs", return_value={}),
        patch("dataraum.analysis.catalogue.agent._required_standard_fields", return_value=[]),
    ):
        result = agent.author(
            session,
            MagicMock(),
            table_ids=["t1"],
            session_table_ids=["t1"],
            ontology=ontology,
            run_id="r1",
        )
    assert not result.success
    return captured


# --- (a) a declared table kind arrives as evidence, in both prompts -----------


def test_per_table_agent_receives_the_declared_taxonomy(session: Session) -> None:
    """The seam that grounds ``is_fact_table``: the declared role rides in with the
    physical names the kind shows up under, so the model can recognize the table."""
    _seed_finance(session)

    taxonomy = _per_table_context(session, "finance")["entity_taxonomy"]

    assert "- gl_line [fact]" in taxonomy
    assert "journal_lines" in taxonomy  # the alias that makes the kind recognizable
    assert "- trial_balance [periodic_snapshot]" in taxonomy
    # The two ticket-named ambiguity cases arrive SETTLED rather than absent.
    assert "- gl_entry [fact]" in taxonomy
    assert "- fx_rate [dimension]" in taxonomy


def test_catalogue_agent_receives_the_declared_taxonomy(session: Session) -> None:
    """The seam that grounds ``detected_entity_type``: the same declaration, carrying
    the concepts each kind is expected to bear."""
    _seed_finance(session)

    taxonomy = _catalogue_context(session, "finance")["entity_taxonomy"]

    assert "- gl_line [fact]" in taxonomy
    assert "Carries concepts: debit, credit" in taxonomy
    assert "- ar_invoice [fact]" in taxonomy


def test_both_agents_receive_the_same_taxonomy(session: Session) -> None:
    """One declaration, one formatter, two consumers — the two table-grain phases must
    not drift into disagreeing about what the domain declares."""
    _seed_finance(session)
    assert (
        _per_table_context(session, "finance")["entity_taxonomy"]
        == _catalogue_context(session, "finance")["entity_taxonomy"]
    )


# --- (b) an undeclared vertical keeps free-text behaviour --------------------


def test_a_vertical_with_no_taxonomy_still_runs_and_defers_to_the_data(session: Session) -> None:
    """The framed-vertical / novel-table case, which is the COMMON one. Declaring no
    taxonomy must not fail the turn and must not read as an empty list of permitted
    answers — the slot says the data decides.

    The intro is absent here, not merely contradicted: it rides inside the formatted
    block, so BOTH agents' <entity_taxonomy> reads coherently with zero entities."""
    ensure_concepts_seeded(session, "finance")  # concepts only: no entities seeded

    for context in (
        _per_table_context(session, "finance"),
        _catalogue_context(session, "finance"),
    ):
        assert context["entity_taxonomy"] == (
            "No table entity taxonomy declared — judge each table on its own data."
        )
        # Every other evidence surface is untouched — this is an ADDITION to the
        # context, not a reshaping of it.
        assert context["ontology_concepts"]
        assert context["ontology_name"] == "finance"


def test_seeding_the_taxonomy_changes_only_the_taxonomy_slot(session: Session) -> None:
    """The rest of the assembled context is byte-identical with and without a declared
    taxonomy — a table that matches nothing is classified on exactly the evidence it
    had before."""
    ensure_concepts_seeded(session, "finance")
    before = _per_table_context(session, "finance")

    ensure_cycle_types_seeded(session, "finance")
    ensure_entities_seeded(session, "finance")
    after = _per_table_context(session, "finance")

    assert before.keys() == after.keys()
    assert {k: v for k, v in before.items() if k != "entity_taxonomy"} == {
        k: v for k, v in after.items() if k != "entity_taxonomy"
    }
    assert before["entity_taxonomy"] != after["entity_taxonomy"]
