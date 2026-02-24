"""Entropy layer data models.

This module defines the core data structures for the entropy layer.

Key models:
- EntropyObject: Core measurement with evidence and resolution options
- ResolutionOption: Actionable fix with effort and expected entropy reduction

Default thresholds loaded from config/entropy/thresholds.yaml.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from dataraum.entropy.config import EntropyConfig


def _get_config() -> EntropyConfig:
    """Get entropy config, avoiding circular import."""
    from dataraum.entropy.config import get_entropy_config

    return get_entropy_config()


@dataclass
class ResolutionOption:
    """An actionable resolution that can reduce entropy.

    Each resolution has an action type, parameters, expected impact,
    and effort level for prioritization.
    """

    action: str  # e.g., "document_unit", "document_description", "transform_filter_nulls"
    parameters: dict[str, Any]  # Action-specific parameters
    expected_entropy_reduction: float  # Expected reduction (0.0-1.0)
    effort: str  # "low", "medium", "high"
    description: str = ""  # Human-readable description

    # Cascade tracking - which other dimensions this resolution affects
    cascade_dimensions: list[str] = field(default_factory=list)

    def priority_score(self) -> float:
        """Calculate priority based on reduction vs effort.

        Higher score = higher priority.
        Effort factors loaded from config/entropy/thresholds.yaml.
        """
        config = _get_config()
        effort_factor = config.effort_factor(self.effort)
        return self.expected_entropy_reduction / effort_factor


@dataclass
class EntropyObject:
    """Core entropy measurement object.

    Represents a single entropy measurement for a specific dimension/sub-dimension
    applied to a specific target (column, table, or relationship).
    """

    # Identity
    object_id: str = field(default_factory=lambda: str(uuid4()))
    layer: str = ""  # structural, semantic, value, computational
    dimension: str = ""  # schema, types, relations, business_meaning, units, etc.
    sub_dimension: str = ""  # naming_clarity, type_fidelity, etc.
    target: str = ""  # column:{table}.{column}, table:{table_name}, relationship:{t1}-{t2}

    # Measurement
    score: float = 0.0  # 0.0 = deterministic, 1.0 = maximum uncertainty
    confidence: float = 1.0  # How confident are we in this score (0.0-1.0)

    # Evidence (dimension-specific)
    evidence: list[dict[str, Any]] = field(default_factory=list)

    # Resolution options
    resolution_options: list[ResolutionOption] = field(default_factory=list)

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_analysis_ids: list[str] = field(default_factory=list)  # Links to source analyses
    detector_id: str = ""  # Which detector produced this

    @property
    def dimension_path(self) -> str:
        """Return full dimension path (layer.dimension.sub_dimension)."""
        return f"{self.layer}.{self.dimension}.{self.sub_dimension}"
