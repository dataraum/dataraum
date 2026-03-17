"""Ontology loading from configuration files.

Loads ontology definitions from config/verticals/<vertical>/ontology.yaml.
Follows the same pattern as PromptRenderer for YAML config loading.
"""

from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from dataraum.core.config import get_config_dir


class OntologyConcept(BaseModel):
    """A concept within an ontology."""

    name: str
    description: str | None = None
    indicators: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)
    temporal_behavior: str | None = None
    typical_role: str | None = None
    typical_values: list[str] = Field(default_factory=list)
    unit_from_concept: str | None = None  # Which concept provides this measure's unit
    is_unit_dimension: bool = False  # Whether this concept defines units for measures


class OntologyDefinition(BaseModel):
    """A complete ontology definition from YAML."""

    name: str
    version: str = "1.0.0"
    description: str | None = None
    concepts: list[OntologyConcept] = Field(default_factory=list)


class OntologyLoader:
    """Load ontology definitions from YAML configuration files.

    Loads ontologies from config/verticals/<vertical>/ontology.yaml.
    """

    def __init__(self, verticals_dir: Path | None = None):
        """Initialize ontology loader.

        Args:
            verticals_dir: Root verticals directory.
                          If None, uses config/verticals/
        """
        if verticals_dir is None:
            verticals_dir = get_config_dir("verticals")

        self.verticals_dir = verticals_dir
        self._cache: dict[str, OntologyDefinition] = {}

    def load(self, vertical: str) -> OntologyDefinition | None:
        """Load and cache an ontology definition for a vertical.

        Args:
            vertical: Vertical name (e.g. 'finance')

        Returns:
            Loaded ontology definition, or None if not found
        """
        if vertical in self._cache:
            return self._cache[vertical]

        ontology_path = self.verticals_dir / vertical / "ontology.yaml"
        if not ontology_path.exists():
            return None

        with open(ontology_path) as f:
            data = yaml.safe_load(f)

        ontology = OntologyDefinition(**data)
        self._cache[vertical] = ontology
        return ontology

    def list_verticals(self) -> list[str]:
        """List available verticals with ontology definitions."""
        if not self.verticals_dir.exists():
            return []

        return [p.parent.name for p in self.verticals_dir.glob("*/ontology.yaml")]

    def format_concepts_for_prompt(self, ontology: OntologyDefinition | None) -> str:
        """Format ontology concepts for inclusion in LLM prompts.

        Args:
            ontology: Ontology definition, or None

        Returns:
            Formatted string describing concepts, or default message
        """
        if ontology is None or not ontology.concepts:
            return "No specific ontology concepts defined"

        lines = []
        for concept in ontology.concepts:
            indicators_str = ", ".join(concept.indicators) if concept.indicators else ""
            if concept.description:
                lines.append(f"- {concept.name}: {concept.description}")
                if indicators_str:
                    lines.append(f"  Indicators: {indicators_str}")
            elif indicators_str:
                lines.append(f"- {concept.name}: {indicators_str}")
            else:
                lines.append(f"- {concept.name}")

        return "\n".join(lines)


__all__ = [
    "OntologyConcept",
    "OntologyDefinition",
    "OntologyLoader",
]
