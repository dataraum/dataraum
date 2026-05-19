"""Tests for ontology loading from config files."""

from pathlib import Path

import yaml

from dataraum.analysis.semantic import OntologyLoader
from dataraum.analysis.semantic.ontology import OntologyConcept, OntologyDefinition


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

    def test_save_writes_yaml(self, tmp_path: Path) -> None:
        """Test save() writes a valid ontology YAML file."""
        loader = OntologyLoader(verticals_dir=tmp_path)
        (tmp_path / "test_save").mkdir()

        definition = OntologyDefinition(
            name="induced",
            description="Auto-generated",
            concepts=[
                OntologyConcept(
                    name="revenue",
                    description="Total income",
                    indicators=["revenue", "sales", "income"],
                    typical_role="measure",
                    temporal_behavior="additive",
                ),
            ],
        )

        path = loader.save("test_save", definition)

        assert path.exists()
        data = yaml.safe_load(path.read_text())
        assert data["name"] == "induced"
        assert len(data["concepts"]) == 1
        assert data["concepts"][0]["name"] == "revenue"

    def test_save_invalidates_cache(self, tmp_path: Path) -> None:
        """Test save() clears the cache for the vertical."""
        loader = OntologyLoader(verticals_dir=tmp_path)

        # Write initial ontology
        vertical_dir = tmp_path / "cached_test"
        vertical_dir.mkdir()
        initial = OntologyDefinition(name="v1", concepts=[])
        loader.save("cached_test", initial)

        # Load to populate cache
        loaded = loader.load("cached_test")
        assert loaded is not None
        assert loaded.name == "v1"

        # Save updated version
        updated = OntologyDefinition(
            name="v2",
            concepts=[OntologyConcept(name="test", indicators=["test"])],
        )
        loader.save("cached_test", updated)

        # Load again — should get v2, not cached v1
        reloaded = loader.load("cached_test")
        assert reloaded is not None
        assert reloaded.name == "v2"
        assert len(reloaded.concepts) == 1
