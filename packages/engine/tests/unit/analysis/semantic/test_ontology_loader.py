"""Tests for ontology loading from config files."""

from pathlib import Path

import pytest

from dataraum.analysis.semantic import OntologyLoader
from dataraum.analysis.semantic.ontology import (
    OntologyConcept,
    OntologyConvention,
    OntologyDefinition,
)
from dataraum.core.vertical import set_framed_concept_resolver


class TestOntologyLoader:
    """Test OntologyLoader."""

    def test_load_nonexistent_vertical_returns_none(self):
        """A vertical with no on-disk file AND no overlay rows is unknown → None."""
        loader = OntologyLoader()
        ontology = loader.load("nonexistent_vertical")

        assert ontology is None

    def test_load_framed_vertical_resolves_to_empty_concepts(self) -> None:
        """A framed vertical has no on-disk directory and, post-DAT-728, no concept
        overlay rows — its concept vocabulary lives in the typed ``concepts`` table,
        read via ``concept_store`` at runtime, not this loader. ``load()`` still
        resolves it to a (non-None) definition: the vertical is KNOWN (framed via
        the typed-concept resolver), it simply carries EMPTY concepts through this
        path — grounding reads the real vocabulary from the table.
        """
        set_framed_concept_resolver(lambda: {"sales"})
        try:
            ontology = OntologyLoader().load("sales")
            assert ontology is not None
            assert ontology.name == "sales"
            assert ontology.concepts == []
        finally:
            set_framed_concept_resolver(None)

    def test_format_concepts_for_prompt(self):
        """Test formatting concepts for LLM prompt."""
        loader = OntologyLoader()
        ontology = loader.load("finance")

        formatted = loader.format_concepts_for_prompt(ontology)

        assert "revenue" in formatted.lower()
        assert "No specific ontology" not in formatted

    def test_format_concepts_includes_unit_from_concept(self):
        """unit_from_concept is fed to the agent (DAT-647) — the concept-level unit teach.

        Finance measures declare `unit_from_concept: currency`; the formatter must
        surface it so the agent grounds unit_source_column on the currency column.
        """
        loader = OntologyLoader()
        ontology = loader.load("finance")

        formatted = loader.format_concepts_for_prompt(ontology)

        assert "Unit from concept: currency" in formatted

    def test_format_concepts_for_prompt_none(self):
        """Test formatting when ontology is None."""
        loader = OntologyLoader()

        formatted = loader.format_concepts_for_prompt(None)

        assert "No specific ontology concepts defined" in formatted

    def test_load_adhoc_baseline_is_empty(self) -> None:
        """The baked-in _adhoc vertical exists but ships with no concepts.

        Cold-start runs of semantic_per_column populate it via overlay
        rows (DAT-371); the file itself stays at ``concepts: []``.
        """
        loader = OntologyLoader()
        ontology = loader.load("_adhoc")

        assert ontology is not None
        assert ontology.name == "_adhoc"
        assert ontology.concepts == []

    def test_custom_verticals_dir(self, tmp_path: Path) -> None:
        """Test using a custom verticals directory (bypasses overlay)."""
        # Create a test vertical with ontology file
        vertical_dir = tmp_path / "test_vertical"
        vertical_dir.mkdir()
        ontology_file = vertical_dir / "ontology.yaml"
        ontology_file.write_text("""
name: test_ontology
version: "1.0.0"
description: Test ontology
concepts:
  - name: test_concept
    description: A test concept
    indicators:
      - test
      - example
""")

        loader = OntologyLoader(verticals_dir=tmp_path)
        ontology = loader.load("test_vertical")

        assert ontology is not None
        assert ontology.name == "test_ontology"
        assert len(ontology.concepts) == 1
        assert ontology.concepts[0].name == "test_concept"


class TestConventions:
    """Vertical conventions piped to SQL-authoring agents (DAT-645)."""

    @staticmethod
    def _ontology(conventions: list[dict]) -> dict:
        return {
            "name": "t",
            "concepts": [
                {"name": "revenue"},
                {"name": "cost_of_goods_sold"},
            ],
            "conventions": conventions,
        }

    def test_finance_conventions_render_for_extraction(self) -> None:
        """The shipped finance sign convention loads and renders for extraction."""
        loader = OntologyLoader()
        ontology = loader.load("finance")
        out = loader.format_conventions_for_prompt(ontology, "extraction")
        assert "natural-balance" in out.lower()
        # Group labels + members are emitted verbatim for the LLM.
        assert "credit_normal:" in out and "revenue" in out
        assert "debit_normal:" in out and "cost_of_goods_sold" in out

    def test_conventions_routed_by_target(self) -> None:
        """A convention renders only for a target it lists — broad or specific."""
        loader = OntologyLoader()
        ontology = loader.load("finance")
        # finance's conventions target `extraction` (broad) + `qa` (broad, the
        # cockpit Q&A agent) only — no convention carries a validation-scoped
        # target since DAT-725 band 3 retired the shipped validation YAMLs (a
        # GENERATED check that needs a convention now declares the dependency
        # from its own side via `relevant_conventions`, see
        # `test_include_ids_pulls_regardless_of_targets` below). The per-spec
        # qualifier ROUND-TRIP mechanism itself is pinned on synthetic fixtures
        # in `test_qualifier_matches_specific_target` below.
        assert loader.format_conventions_for_prompt(ontology, "extraction")
        assert loader.format_conventions_for_prompt(ontology, "qa")
        # Broad `validation` (no qualifier) does NOT match — no convention opts in.
        assert loader.format_conventions_for_prompt(ontology, "validation") == ""
        # No convention carries a validation-scoped target anymore — every
        # qualifier resolves empty against the real finance data.
        assert (
            loader.format_conventions_for_prompt(
                ontology, "validation", qualifier="sign_conventions"
            )
            == ""
        )
        assert (
            loader.format_conventions_for_prompt(ontology, "validation", qualifier="trial_balance")
            == ""
        )

    def test_qualifier_matches_specific_target(self) -> None:
        """A `target:qualifier` target is reached only with the matching qualifier."""
        loader = OntologyLoader()
        ont = OntologyDefinition(
            **self._ontology(
                [
                    {
                        "id": "c",
                        "targets": ["validation:sign_conventions"],
                        "statement": "rule",
                        "concept_groups": {},
                    }
                ]
            )
        )
        assert loader.format_conventions_for_prompt(ont, "validation") == ""
        assert loader.format_conventions_for_prompt(ont, "validation", qualifier="other") == ""
        assert loader.format_conventions_for_prompt(ont, "validation", qualifier="sign_conventions")

    def test_broad_target_matches_even_with_qualifier(self) -> None:
        """A BROAD `validation` target reaches every spec — the qualifier doesn't
        narrow a convention that already opted into all of them."""
        loader = OntologyLoader()
        ont = OntologyDefinition(
            **self._ontology(
                [{"id": "c", "targets": ["validation"], "statement": "rule", "concept_groups": {}}]
            )
        )
        # Broad target matches with OR without a qualifier.
        assert loader.format_conventions_for_prompt(ont, "validation")
        assert loader.format_conventions_for_prompt(ont, "validation", qualifier="anything")

    def test_include_ids_pulls_regardless_of_targets(self) -> None:
        """A check's declared `relevant_conventions` pull a convention in by id
        (DAT-865) — the convention-side targets can only name checks that exist at
        authoring time, so a GENERATED check selects its dependencies from the
        other side. Selection only: an unrelated id still renders nothing."""
        loader = OntologyLoader()
        ont = OntologyDefinition(
            **self._ontology(
                [
                    {
                        "id": "sign_rule",
                        "targets": ["validation:sign_conventions"],
                        "statement": "the sign rule",
                        "concept_groups": {},
                    }
                ]
            )
        )
        # An unqualified generated check without the declaration sees nothing…
        assert loader.format_conventions_for_prompt(ont, "validation", qualifier="gen_check") == ""
        # …and pulls the convention by declaring it.
        pulled = loader.format_conventions_for_prompt(
            ont, "validation", qualifier="gen_check", include_ids=["sign_rule"]
        )
        assert "the sign rule" in pulled
        # A declared id that names no convention selects nothing (no fabrication path).
        assert (
            loader.format_conventions_for_prompt(
                ont, "validation", qualifier="gen_check", include_ids=["ghost"]
            )
            == ""
        )

    def test_format_conventions_none(self) -> None:
        assert OntologyLoader().format_conventions_for_prompt(None, "extraction") == ""

    def test_valid_convention_resolves_and_is_disjoint(self) -> None:
        ont = OntologyDefinition(
            **self._ontology(
                [
                    {
                        "id": "sign",
                        "targets": ["extraction"],
                        "statement": "rule",
                        "concept_groups": {
                            "credit_normal": ["revenue"],
                            "debit_normal": ["cost_of_goods_sold"],
                        },
                    }
                ]
            )
        )
        assert len(ont.conventions) == 1

    def test_lint_rejects_unknown_concept(self) -> None:
        """A group member that is not a declared concept fails loud at load."""
        with pytest.raises(ValueError, match="not a declared concept"):
            OntologyDefinition(
                **self._ontology(
                    [
                        {
                            "id": "sign",
                            "statement": "rule",
                            "concept_groups": {"credit_normal": ["nonexistent_concept"]},
                        }
                    ]
                )
            )

    def test_lint_rejects_concept_in_two_groups(self) -> None:
        """A concept assigned to two groups (contradiction) fails loud."""
        with pytest.raises(ValueError, match="disjoint"):
            OntologyDefinition(
                **self._ontology(
                    [
                        {
                            "id": "sign",
                            "statement": "rule",
                            "concept_groups": {
                                "credit_normal": ["revenue"],
                                "debit_normal": ["revenue"],
                            },
                        }
                    ]
                )
            )

    def test_no_conventions_is_valid(self) -> None:
        """Verticals without conventions (the common case) load fine."""
        ont = OntologyDefinition(name="t", concepts=[OntologyConcept(name="x")])
        assert ont.conventions == []


def test_convention_model_defaults() -> None:
    """A convention needs only id + statement; groups/targets default empty."""
    conv = OntologyConvention(id="c", statement="s")
    assert conv.targets == [] and conv.concept_groups == {}


class TestCompositions:
    """Concept compositions → part_of edges (DAT-729)."""

    @staticmethod
    def _ontology(compositions: list[dict]) -> dict:
        return {
            "name": "t",
            "concepts": [
                {"name": "current_assets"},
                {"name": "cash"},
                {"name": "inventory"},
            ],
            "compositions": compositions,
        }

    def test_finance_ships_compositions(self) -> None:
        """The shipped finance vertical declares the balance-sheet compositions."""
        ontology = OntologyLoader().load("finance")
        wholes = {c.whole for c in ontology.compositions}
        assert {"current_assets", "current_liabilities"} <= wholes
        current_assets = next(c for c in ontology.compositions if c.whole == "current_assets")
        assert set(current_assets.parts) == {"cash", "accounts_receivable", "inventory"}

    def test_valid_composition_resolves(self) -> None:
        ont = OntologyDefinition(
            **self._ontology([{"whole": "current_assets", "parts": ["cash", "inventory"]}])
        )
        assert len(ont.compositions) == 1

    def test_lint_rejects_unknown_part(self) -> None:
        """A part that is not a declared concept fails loud at load."""
        with pytest.raises(ValueError, match="not a declared concept"):
            OntologyDefinition(
                **self._ontology([{"whole": "current_assets", "parts": ["nonexistent"]}])
            )

    def test_lint_rejects_unknown_whole(self) -> None:
        """The whole must also be a declared concept."""
        with pytest.raises(ValueError, match="not a declared concept"):
            OntologyDefinition(**self._ontology([{"whole": "nonexistent", "parts": ["cash"]}]))

    def test_lint_rejects_self_part(self) -> None:
        """A concept cannot be part_of itself — a self-loop the graph must never carry."""
        with pytest.raises(ValueError, match="cannot be part_of itself"):
            OntologyDefinition(
                **self._ontology([{"whole": "current_assets", "parts": ["cash", "current_assets"]}])
            )

    def test_no_compositions_is_valid(self) -> None:
        ont = OntologyDefinition(name="t", concepts=[OntologyConcept(name="x")])
        assert ont.compositions == []


class TestEntities:
    """The table-entity taxonomy (DAT-724) — parse + the authoring lint."""

    @staticmethod
    def _ontology(entities: list[dict]) -> dict:
        return {
            "name": "t",
            "concepts": [{"name": "debit"}, {"name": "credit"}],
            "entities": entities,
        }

    def test_finance_ships_a_taxonomy_covering_the_corpus_kinds(self) -> None:
        ontology = OntologyLoader().load("finance")
        by_name = {e.name: e for e in ontology.entities}
        assert len(by_name) == 15
        # The ledger spine and the two ticket-named ambiguity cases.
        assert by_name["gl_line"].role == "fact"
        assert by_name["gl_entry"].role == "fact"
        assert by_name["fx_rate"].role == "dimension"
        assert by_name["trial_balance"].role == "periodic_snapshot"

    def test_sales_order_does_not_claim_the_bare_orders_name(self) -> None:
        """A deliberate omission, easily "fixed" back: the corpus ships a role-play
        probe skeleton literally named ``orders`` (order_id x order_date plus injected
        FK-role columns). It is genuinely a fact, but carries neither fiscal_period nor
        order_to_cash — so claiming the bare name would misattach this declaration to
        it. An alias is a recognition hint; a wrong one is worse than none."""
        ontology = OntologyLoader().load("finance")
        sales_order = next(e for e in ontology.entities if e.name == "sales_order")
        assert "orders" not in sales_order.aliases
        assert "sales_orders" in sales_order.aliases

    def test_no_alias_is_claimed_by_two_entities(self) -> None:
        """Two kinds answering to one physical name makes the hint ambiguous exactly
        where it is supposed to disambiguate."""
        ontology = OntologyLoader().load("finance")
        claimed: dict[str, str] = {}
        for entity in ontology.entities:
            for alias in entity.aliases:
                assert alias not in claimed, (
                    f"alias {alias!r} claimed by both {claimed.get(alias)!r} and {entity.name!r}"
                )
                claimed[alias] = entity.name

    def test_every_declared_role_is_a_table_role_value(self) -> None:
        """The declaration reuses the persisted vocabulary rather than a parallel one."""
        from dataraum.analysis.semantic.db_models import TableRole

        valid = {r.value for r in TableRole}
        ontology = OntologyLoader().load("finance")
        assert {e.role for e in ontology.entities} <= valid

    def test_round_trips_every_field(self) -> None:
        ont = OntologyDefinition(
            **self._ontology(
                [
                    {
                        "name": "gl_line",
                        "role": "fact",
                        "description": "a posting line",
                        "concepts": ["debit", "credit"],
                        "cycles": ["journal_entry_cycle"],
                        "aliases": ["journal_lines"],
                    }
                ]
            )
        )
        (entity,) = ont.entities
        assert entity.role == "fact"
        assert entity.concepts == ["debit", "credit"]
        assert entity.cycles == ["journal_entry_cycle"]
        assert entity.aliases == ["journal_lines"]

    def test_lint_rejects_an_unknown_concept_reference(self) -> None:
        """An entity naming a concept that does not exist is a typo or a rename that
        would silently degrade the served evidence."""
        with pytest.raises(ValueError, match="not a declared concept"):
            OntologyDefinition(
                **self._ontology([{"name": "gl_line", "role": "fact", "concepts": ["revenue"]}])
            )

    def test_lint_rejects_a_duplicate_entity_name(self) -> None:
        """Two entities sharing a name would collide on the active-row unique index at
        seed — a far worse place to discover it."""
        with pytest.raises(ValueError, match="declared twice"):
            OntologyDefinition(
                **self._ontology(
                    [{"name": "gl_line", "role": "fact"}, {"name": "gl_line", "role": "dimension"}]
                )
            )

    def test_role_and_cycles_are_not_linted_here(self) -> None:
        """Both resolve against vocabularies this document cannot see — they are
        born-loud at SEED instead (the OntologyConcept.kind discipline)."""
        ont = OntologyDefinition(
            **self._ontology([{"name": "gl_line", "role": "nonsense", "cycles": ["nope"]}])
        )
        assert ont.entities[0].role == "nonsense"

    def test_no_entities_is_valid(self) -> None:
        """A framed vertical mid-authoring declares none and must still parse."""
        ont = OntologyDefinition(name="t", concepts=[OntologyConcept(name="x")])
        assert ont.entities == []


class TestFormatEntitiesForPrompt:
    """What the two table-grain agents actually receive as evidence (DAT-724)."""

    def test_renders_role_concepts_cycles_and_aliases(self) -> None:
        ont = OntologyDefinition(
            name="t",
            concepts=[OntologyConcept(name="debit")],
            entities=[
                {
                    "name": "gl_line",
                    "role": "fact",
                    "description": "a posting line",
                    "concepts": ["debit"],
                    "cycles": ["journal_entry_cycle"],
                    "aliases": ["journal_lines", "postings"],
                }
            ],
        )
        rendered = OntologyLoader().format_entities_for_prompt(ont)
        assert "- gl_line [fact]: a posting line" in rendered
        assert "Carries concepts: debit" in rendered
        assert "Business cycles: journal_entry_cycle" in rendered
        # "Commonly named", not "matches": these are hints for the model's judgment,
        # never an engine-side string match.
        assert "Commonly named: journal_lines, postings" in rendered

    def test_the_non_empty_block_carries_its_own_framing(self) -> None:
        """The intro belongs to the FORMATTER, not the prompt YAML: the template
        engine has no conditionals, so a static intro would sit above the
        nothing-declared sentence and contradict it. Both guards are pinned here —
        'not a menu' keeps the taxonomy from reading as a closed list of permitted
        answers, and 'never overrides' keeps a declaration from displacing structural
        evidence (which ``derive_table_role`` owns and never sees this value)."""
        ont = OntologyDefinition(name="t", entities=[{"name": "widget", "role": "dimension"}])
        rendered = OntologyLoader().format_entities_for_prompt(ont)
        assert rendered.startswith("Table kinds this domain declares")
        assert "Evidence, not a menu" in rendered
        assert "an undeclared table is a normal case, not an error" in rendered
        assert (
            "A declaration never overrides what a table's own structure plainly shows" in rendered
        )

    def test_omits_the_lines_a_sparse_entity_has_nothing_for(self) -> None:
        ont = OntologyDefinition(name="t", entities=[{"name": "widget", "role": "dimension"}])
        rendered = OntologyLoader().format_entities_for_prompt(ont)
        assert rendered.endswith("\n\n- widget [dimension]")

    def test_an_undeclared_taxonomy_defers_to_the_data(self) -> None:
        """The empty case must not read as an empty list of PERMITTED answers — a
        framed vertical declares no taxonomy and must keep undeclared detection.

        Phase-NEUTRAL wording: one formatter feeds a structural classifier working
        against a schema (``is_fact_table``) and a naming turn (``entity_type``), so
        'describe each table' would misaddress the first. Their shared instruction is
        to judge the table on its own data."""
        for ontology in (None, OntologyDefinition(name="t")):
            rendered = OntologyLoader().format_entities_for_prompt(ontology)
            assert rendered == (
                "No table entity taxonomy declared — judge each table on its own data."
            )
            assert "Table kinds this domain declares" not in rendered
