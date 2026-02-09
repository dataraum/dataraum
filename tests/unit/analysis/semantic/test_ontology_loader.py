"""Tests for ontology loading from config files."""

from pathlib import Path

from dataraum.analysis.semantic import OntologyLoader


class TestOntologyLoader:
    """Test OntologyLoader."""

    def test_load_financial_reporting_ontology(self):
        """Test loading the financial_reporting ontology from config."""
        loader = OntologyLoader()
        ontology = loader.load("financial_reporting")

        assert ontology is not None
        assert ontology.name == "financial_reporting"
        assert ontology.version == "1.0.0"
        assert len(ontology.concepts) > 0
        assert len(ontology.metrics) > 0

    def test_load_nonexistent_ontology_returns_none(self):
        """Test that loading a nonexistent ontology returns None."""
        loader = OntologyLoader()
        ontology = loader.load("nonexistent_ontology")

        assert ontology is None

    def test_list_ontologies(self):
        """Test listing available ontologies."""
        loader = OntologyLoader()
        ontologies = loader.list_ontologies()

        assert "financial_reporting" in ontologies

    def test_format_concepts_for_prompt(self):
        """Test formatting concepts for LLM prompt."""
        loader = OntologyLoader()
        ontology = loader.load("financial_reporting")

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
        ontology1 = loader.load("financial_reporting")
        # Second load (should hit cache)
        ontology2 = loader.load("financial_reporting")

        # Should be the same object (cached)
        assert ontology1 is ontology2

    def test_clear_cache(self):
        """Test clearing the cache."""
        loader = OntologyLoader()

        # Load to populate cache
        ontology1 = loader.load("financial_reporting")
        loader.clear_cache()

        # Load again (should be new object)
        ontology2 = loader.load("financial_reporting")

        # Different objects after cache clear
        assert ontology1 is not ontology2

    def test_custom_ontologies_dir(self, tmp_path: Path) -> None:
        """Test using a custom ontologies directory."""
        # Create a test ontology file
        ontology_file = tmp_path / "test_ontology.yaml"
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
metrics: []
quality_rules: []
semantic_hints: []
""")

        loader = OntologyLoader(ontologies_dir=tmp_path)
        ontology = loader.load("test_ontology")

        assert ontology is not None
        assert ontology.name == "test_ontology"
        assert len(ontology.concepts) == 1
        assert ontology.concepts[0].name == "test_concept"

    def test_concept_properties(self):
        """Test that concept properties are correctly loaded."""
        loader = OntologyLoader()
        ontology = loader.load("financial_reporting")

        assert ontology is not None
        revenue_concept = next((c for c in ontology.concepts if c.name == "revenue"), None)

        assert revenue_concept is not None
        assert "revenue" in revenue_concept.indicators
        assert "sales" in revenue_concept.indicators
        assert revenue_concept.temporal_behavior == "additive"
        assert revenue_concept.typical_role == "measure"

    def test_metric_properties(self):
        """Test that metric properties are correctly loaded."""
        loader = OntologyLoader()
        ontology = loader.load("financial_reporting")

        assert ontology is not None
        gross_profit = next((m for m in ontology.metrics if m.name == "gross_profit"), None)

        assert gross_profit is not None
        assert "revenue - cost_of_goods_sold" in gross_profit.formula
        assert "revenue" in gross_profit.required_concepts
        assert "cost_of_goods_sold" in gross_profit.required_concepts
