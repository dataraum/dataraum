"""Prompt contracts of the DAT-823 rebalance, pinned against the shipped YAML.

Two templates carry the split: ``catalogue_semantics`` (the authoring turn) and
``semantic_per_table`` (structural only after the shrink). These tests read the
REAL config templates so an authoring edit that re-opens the split — an entity
claim smuggled back into the structural prompt, the ambiguous contract dropped
from the catalogue prompt — fails here instead of in an eval run.
"""

from __future__ import annotations

import pytest

from dataraum.llm.prompts import PromptRenderer, PromptTemplate


def _flat(text: str) -> str:
    """Whitespace-normalized view — YAML line wrapping is not part of the contract."""
    return " ".join(text.split())


@pytest.fixture(scope="module")
def renderer() -> PromptRenderer:
    return PromptRenderer()


@pytest.fixture(scope="module")
def catalogue(renderer: PromptRenderer) -> PromptTemplate:
    return renderer.load_template("catalogue_semantics")


@pytest.fixture(scope="module")
def per_table(renderer: PromptRenderer) -> PromptTemplate:
    return renderer.load_template("semantic_per_table")


class TestCataloguePrompt:
    def test_renders_with_all_declared_inputs(self, renderer: PromptRenderer) -> None:
        context = {
            "structural_tables": "t",
            "column_annotations": "a",
            "relationship_catalogue": "r",
            "enriched_views": "v",
            "shared_axes": "s",
            "ontology_name": "general",
            "ontology_concepts": "c",
            "required_standard_fields": "- revenue",
            "entity_taxonomy": "- gl_line [fact]",
        }
        system, user = renderer.render_split("catalogue_semantics", context)
        assert system and user

    def test_grounds_entity_type_on_the_declared_taxonomy(self, catalogue: PromptTemplate) -> None:
        """DAT-724: entity_type was free text answered against no declared
        alternative. The taxonomy PROPOSES — the escape hatch for a table matching
        no declared kind is part of the contract, not a nicety: without it the
        framed-vertical / novel-table case degrades into forced misclassification."""
        system = _flat(catalogue.system_prompt)
        assert "Where the declared entity taxonomy names a kind this table IS" in system
        assert "answer with that entity's name verbatim" in system
        assert "Where none fits, name the entity in your own words" in system
        user = _flat(catalogue.user_prompt)
        assert "<entity_taxonomy>" in user
        assert "Evidence for entity_type, not a menu" in user

    def test_carries_the_ambiguous_contract(self, catalogue: PromptTemplate) -> None:
        """'ambiguous' is declared ignorance WITH a meaning present (DAT-769/823)."""
        system = _flat(catalogue.system_prompt)
        assert "'ambiguous'" in system
        assert "still carries a meaning" in system
        assert "what evidence would settle it" in system

    def test_authors_both_surfaces_for_every_item(self, catalogue: PromptTemplate) -> None:
        system = catalogue.system_prompt
        assert "table_readings entry for EVERY table" in system
        assert "column_concepts entry for EVERY column" in system
        # The settled structure is input, never re-opened.
        assert "Do not re-open the settled structure" in system

    def test_names_the_chain_conditioned_lines(self, catalogue: PromptTemplate) -> None:
        """DAT-853: the relationships header is the reader's decoder ring for
        the conditioned lines — served evidence the agent was never taught to
        read stays unread, so softening this wording fails here, not in an
        eval run (the cycles prompt carries the same pin)."""
        user = _flat(catalogue.user_prompt)
        assert 'Lines marked "(<fk>-joined rows)" are chain-conditioned' in user
        assert "ONLY the rows that resolve across the join" in user
        assert "the conditioned values are the evidence for what the chain itself carries" in user


class TestPerTablePromptShrink:
    def test_renders_with_all_declared_inputs(self, renderer: PromptRenderer) -> None:
        context = {
            "tables_json": "[]",
            "ontology_name": "general",
            "ontology_concepts": "c",
            "entity_taxonomy": "- gl_line [fact]",
            "relationship_candidates": "r",
            "column_annotations": "a",
        }
        system, user = renderer.render_split("semantic_per_table", context)
        assert system and user

    def test_grounds_the_fact_dimension_answer_on_the_taxonomy(
        self, per_table: PromptTemplate
    ) -> None:
        """DAT-724: the declared role is EVIDENCE for is_fact_table, never a
        substitute for structure. Both halves are pinned — softening the
        'strong evidence' steer makes the taxonomy inert, and dropping the
        'never overrides' guard turns a declaration into an override, which
        ``derive_table_role``'s structural periodic_snapshot refinement (which
        this value never reaches) is there to keep owning."""
        system = _flat(per_table.system_prompt)
        assert "Where the declared entity taxonomy names a table kind this table" in system
        assert "strong evidence for that answer" in system
        assert "where none matches, decide on the data" in system
        user = _flat(per_table.user_prompt)
        assert "<entity_taxonomy>" in user
        assert "Evidence, not a menu" in user
        assert "A declaration never overrides what a table's own structure plainly shows" in user

    def test_identity_note_is_structural_only(self, per_table: PromptTemplate) -> None:
        """The lead-adjacent ruling (DAT-823): identity notes are STRUCTURAL
        observations — an entity claim in a note would smuggle the business
        reading past the split (a note like "the customer being invoiced" is a
        counterparty claim the catalogue phase owns)."""
        system = per_table.system_prompt
        assert "The note is a STRUCTURAL observation only" in system
        assert "Do NOT name the business entity" in system

    def test_no_business_authoring_remains(self, per_table: PromptTemplate) -> None:
        """The authoring half moved out whole: no column_concepts section, no
        meaning/unit-source instructions, no entity-classification duty."""
        system = per_table.system_prompt
        assert "<column_concepts>" not in system
        assert "unit_source_column" not in system
        assert "derived_formula_hypothesis" not in system
        assert "Classify each table as the business entity" not in system

    def test_relationship_evaluation_survives(self, per_table: PromptTemplate) -> None:
        """The W3-D relationship half (v2.5.0 period-caution + role-lines) is
        byte-preserved through the shrink — spot-pin its load-bearing lines."""
        system = _flat(per_table.system_prompt)
        assert "<relationship_evaluation>" in system
        assert "Period and calendar identifiers (fiscal period, year-month," in system
        assert "the candidate line carries its established role as [role: L=... R=...]" in system
        assert "Confidence encodes EXISTENCE" in system
