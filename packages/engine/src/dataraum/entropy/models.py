"""Entropy layer data models.

This module defines the core data structures for the entropy layer.

Key models:
- EntropyObject: Core measurement with evidence

Default thresholds loaded from config/entropy/thresholds.yaml.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


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

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_analysis_ids: list[str] = field(default_factory=list)  # Links to source analyses
    detector_id: str = ""  # Which detector produced this

    @property
    def dimension_path(self) -> str:
        """Return full dimension path (layer.dimension.sub_dimension)."""
        return f"{self.layer}.{self.dimension}.{self.sub_dimension}"
