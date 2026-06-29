"""Tests for ontology loading from config files."""

from pathlib import Path

import pytest

from dataraum.analysis.semantic import OntologyLoader
from dataraum.analysis.semantic.ontology import (
    OntologyConcept,
    OntologyConvention,
    OntologyDefinition,
)
from dataraum.core.overlay import (
    OverlayRow,
    reset_overlay_resolver_for_tests,
    set_overlay_resolver,
)


class TestOntologyLoader:
    """Test OntologyLoader."""

    def test_load_nonexistent_vertical_returns_none(self):
        """A vertical with no on-disk file AND no overlay rows is unknown → None."""
        loader = OntologyLoader()
        ontology = loader.load("nonexistent_vertical")

        assert ontology is None

    def test_load_framed_vertical_resolves_overlay_only(self) -> None:
        """A framed vertical has no on-disk directory (the config tree is
        read-only); its concepts live only as `concept` overlay rows. load()
        resolves it by layering those rows over an empty base — so the engine
        grounds against a cockpit-declared vertical that never touched disk.
        """
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept",
                    payload={
                        "vertical": "sales",
                        "name": "deal_value",
                        "indicators": ["amount", "value"],
                    },
                )
            ]
        )
        try:
            ontology = OntologyLoader().load("sales")
            assert ontology is not None
            assert ontology.name == "sales"
            assert [c.name for c in ontology.concepts] == ["deal_value"]
        finally:
            reset_overlay_resolver_for_tests()

    def test_format_concepts_for_prompt(self):
        """Test formatting concepts for LLM prompt."""
        loader = OntologyLoader()
        ontology = loader.load("finance")

        formatted = loader.format_concepts_for_prompt(ontology)

        assert "revenue" in formatted.lower()
        assert "No specific ontology" not in formatted

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
                {"name": "revenue", "typical_role": "measure"},
                {"name": "cost_of_goods_sold", "typical_role": "measure"},
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
        # finance's sign convention targets `extraction` (broad) + the SPECIFIC
        # `validation:sign_conventions` — NOT every validation.
        assert loader.format_conventions_for_prompt(ontology, "extraction")
        assert loader.format_conventions_for_prompt(ontology, "qa") == ""
        # Broad `validation` (no qualifier) does NOT match the scoped target.
        assert loader.format_conventions_for_prompt(ontology, "validation") == ""
        # The named validation gets it; an unrelated one does not.
        assert loader.format_conventions_for_prompt(
            ontology, "validation", qualifier="sign_conventions"
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
        assert loader.format_conventions_for_prompt(
            ont, "validation", qualifier="sign_conventions"
        )

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
