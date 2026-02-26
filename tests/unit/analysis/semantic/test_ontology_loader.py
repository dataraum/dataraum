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

    def test_ontology_caching(self):
        """Test that ontologies are cached after first load."""
        loader = OntologyLoader()

        # First load
        ontology1 = loader.load("finance")
        # Second load (should hit cache)
        ontology2 = loader.load("finance")

        # Should be the same object (cached)
        assert ontology1 is ontology2

    def test_clear_cache(self):
        """Test clearing the cache."""
        loader = OntologyLoader()

        # Load to populate cache
        ontology1 = loader.load("finance")
        loader.clear_cache()

        # Load again (should be new object)
        ontology2 = loader.load("finance")

        # Different objects after cache clear
        assert ontology1 is not ontology2

    def test_custom_verticals_dir(self, tmp_path: Path) -> None:
        """Test using a custom verticals directory."""
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

