"""Entropy layer data models.

This module defines the core data structures for the entropy layer.

Key models:
- EntropyObject: Core measurement with evidence and resolution options
- ResolutionOption: Actionable fix with effort level

Default thresholds loaded from config/entropy/thresholds.yaml.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


@dataclass
class ResolutionOption:
    """An actionable resolution that can reduce entropy.

    Each resolution has an action type, parameters, and effort level.
    Prioritization is done by the Bayesian network's impact_delta.
    """

    action: str  # e.g., "document_unit", "document_description", "transform_filter_nulls"
    parameters: dict[str, Any]  # Action-specific parameters
    effort: str  # "low", "medium", "high"
    description: str = ""  # Human-readable description


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

    # Evidence (dimension-specific)
    evidence: list[dict[str, Any]] = field(default_factory=list)

    # Resolution options
    resolution_options: list[ResolutionOption] = field(default_factory=list)

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_analysis_ids: list[str] = field(default_factory=list)  # Links to source analyses
    detector_id: str = ""  # Which detector produced this

    # Business pattern filter (set by pattern_filter at gate time)
    expected_business_pattern: str | None = None
    business_rule: str | None = None
    filter_confidence: float | None = None

    @property
    def dimension_path(self) -> str:
        """Return full dimension path (layer.dimension.sub_dimension)."""
        return f"{self.layer}.{self.dimension}.{self.sub_dimension}"
