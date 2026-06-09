"""Ontology loading from configuration files.

Loads ontology definitions from config/verticals/<vertical>/ontology.yaml
through the shared :class:`~dataraum.core.vertical_loader.VerticalLoader`
(DAT-481), so the workspace's active overlay rows (DAT-343; ``concept`` and
``concept_property`` types per DAT-371) are merged onto the baked-in YAML.
Custom ``verticals_dir`` (test fixtures) bypasses the overlay — deterministic.
"""

from pathlib import Path

from pydantic import BaseModel, Field

from dataraum.core.config import get_config_dir
from dataraum.core.vertical import VerticalKind, resolve_vertical
from dataraum.core.vertical_loader import Family, VerticalLoader


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
            verticals_dir: Root verticals directory. ``None`` (production)
                routes loads through :func:`load_yaml_config` so workspace
                overlay rows are merged. A custom path (tests / fixtures)
                reads YAML directly and bypasses the overlay —
                deterministic for unit tests.
        """
        self.verticals_dir = verticals_dir
        # No cache: the overlay-aware production path must reflect newly
        # inserted overlay rows on the next call (e.g. _adhoc induction
        # inserts rows then re-reads in the same phase). Per-load latency
        # is one filesystem read + dict copies; not a hotspot.

    def load(self, vertical: str) -> OntologyDefinition | None:
        """Load an ontology definition for a vertical.

        Resolution is the shared :class:`VerticalLoader.collection` (DAT-481):
        production (``verticals_dir`` is ``None``) reads the shipped baseline ⊕
        overlay rows; the test path (explicit ``verticals_dir``) reads raw YAML
        and bypasses the overlay.

        Args:
            vertical: Vertical name (e.g. ``'finance'`` or ``'_adhoc'``).

        Returns:
            The (overlay-merged) ontology definition — a builtin resolves its
            baked-in YAML, a framed vertical resolves overlay-only. ``None`` only
            on the production path for an UNKNOWN vertical (a typo or one never
            framed) — the single ``None`` case, owned by ``resolve_vertical``
            (DAT-480); every known/placeholder/framed name resolves to a
            (possibly empty) definition.
        """
        # DAT-480: an UNKNOWN production vertical is the only None case. The test
        # path is deterministic (no overlay), so the guard is production-only.
        if self.verticals_dir is None and resolve_vertical(vertical) is VerticalKind.UNKNOWN:
            return None
        data = VerticalLoader(vertical, self.verticals_dir).collection(Family.CONCEPTS)
        return OntologyDefinition(**data)

    def list_verticals(self) -> list[str]:
        """List the shipped (on-disk) verticals.

        File-globs ``verticals/*/ontology.yaml`` only — *framed* verticals
        (declared via the cockpit ``frame`` stage; they live entirely in overlay
        rows with no on-disk file, DAT-480) are NOT enumerated here. Returns
        ``[]`` if the verticals root doesn't exist.
        """
        root = self.verticals_dir if self.verticals_dir is not None else get_config_dir("verticals")
        if not root.exists():
            return []
        return [p.parent.name for p in root.glob("*/ontology.yaml")]

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
