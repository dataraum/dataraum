"""Tests for ontology loading from config files."""

from pathlib import Path

from dataraum.analysis.semantic import OntologyLoader


class TestOntologyLoader:
    """Test OntologyLoader."""

    def test_load_nonexistent_vertical_returns_none(self):
        """Test that loading a nonexistent vertical returns None."""
        loader = OntologyLoader()
        ontology = loader.load("nonexistent_vertical")

        assert ontology is None

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
