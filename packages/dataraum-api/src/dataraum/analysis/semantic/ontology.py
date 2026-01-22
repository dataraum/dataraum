"""Ontology loading from configuration files.

Loads ontology definitions from config/ontologies/*.yaml files.
Follows the same pattern as PromptRenderer for YAML config loading.
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class OntologyConcept(BaseModel):
    """A concept within an ontology."""

    name: str
    description: str | None = None
    indicators: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)
    temporal_behavior: str | None = None
    typical_role: str | None = None
    typical_values: list[str] = Field(default_factory=list)


class OntologyMetric(BaseModel):
    """A computable metric within an ontology."""

    name: str
    formula: str
    description: str | None = None
    required_concepts: list[str] = Field(default_factory=list)
    output_type: str | None = None
    typical_range: list[float] | None = None
    aliases: list[str] = Field(default_factory=list)


class OntologyRule(BaseModel):
    """A quality rule within an ontology."""

    name: str
    description: str | None = None
    applies_to: str | None = None
    rule_type: str
    expression: str | None = None
    severity: str = "warning"


class SemanticHint(BaseModel):
    """A semantic hint for column patterns."""

    pattern: str
    likely_type: str | None = None
    likely_role: str | None = None
    likely_concept: str | None = None


class OntologyDefinition(BaseModel):
    """A complete ontology definition from YAML."""

    name: str
    version: str = "1.0.0"
    description: str | None = None
    concepts: list[OntologyConcept] = Field(default_factory=list)
    metrics: list[OntologyMetric] = Field(default_factory=list)
    quality_rules: list[OntologyRule] = Field(default_factory=list)
    semantic_hints: list[SemanticHint] = Field(default_factory=list)
    suggested_queries: list[dict[str, Any]] = Field(default_factory=list)


class OntologyLoader:
    """Load ontology definitions from YAML configuration files.

    Loads ontologies from config/ontologies/*.yaml and caches them in memory.
    """

    def __init__(self, ontologies_dir: Path | None = None):
        """Initialize ontology loader.

        Args:
            ontologies_dir: Directory containing ontology YAML files.
                          If None, uses config/ontologies/
        """
        if ontologies_dir is None:
            ontologies_dir = Path("config/ontologies")

        self.ontologies_dir = ontologies_dir
        self._cache: dict[str, OntologyDefinition] = {}

    def load(self, name: str) -> OntologyDefinition | None:
        """Load and cache an ontology definition.

        Args:
            name: Ontology name (without .yaml extension)

        Returns:
            Loaded ontology definition, or None if not found
        """
        if name in self._cache:
            return self._cache[name]

        ontology_path = self.ontologies_dir / f"{name}.yaml"
        if not ontology_path.exists():
            return None

        with open(ontology_path) as f:
            data = yaml.safe_load(f)

        ontology = OntologyDefinition(**data)
        self._cache[name] = ontology
        return ontology

    def list_ontologies(self) -> list[str]:
        """List available ontology names."""
        if not self.ontologies_dir.exists():
            return []

        return [p.stem for p in self.ontologies_dir.glob("*.yaml")]

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

    def clear_cache(self) -> None:
        """Clear the ontology cache."""
        self._cache.clear()


__all__ = [
    "OntologyConcept",
    "OntologyMetric",
    "OntologyRule",
    "SemanticHint",
    "OntologyDefinition",
    "OntologyLoader",
]
