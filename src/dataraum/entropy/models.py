"""Entropy layer data models.

This module defines the core data structures for the entropy layer.
All models follow the specification in docs/ENTROPY_MODELS.md.

Key models:
- EntropyObject: Core measurement with evidence and resolution options
- ResolutionOption: Actionable fix with effort and expected entropy reduction
- CompoundRisk: Dangerous dimension combination with multiplied impact
- ResolutionCascade: Single fix affecting multiple entropy dimensions
- CompoundRiskDefinition: Definition pattern for compound risks

For aggregated entropy views, use:
- entropy.analysis.aggregator: ColumnSummary, TableSummary, RelationshipSummary
- entropy.views: EntropyForGraph, EntropyForQuery, EntropyForDashboard

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

    def is_high_entropy(self, threshold: float = 0.6) -> bool:
        """Check if entropy exceeds threshold."""
        return self.score >= threshold

    def is_critical(self, threshold: float = 0.8) -> bool:
        """Check if entropy is at critical level."""
        return self.score >= threshold

    @property
    def dimension_path(self) -> str:
        """Return full dimension path (layer.dimension.sub_dimension)."""
        return f"{self.layer}.{self.dimension}.{self.sub_dimension}"


@dataclass
class CompoundRisk:
    """A dangerous combination of entropy dimensions.

    Some dimension pairs create multiplicative risk that exceeds
    the sum of individual scores.
    """

    risk_id: str = field(default_factory=lambda: str(uuid4()))
    target: str = ""  # column, table, or relationship

    # Which dimensions are combined
    dimensions: list[str] = field(default_factory=list)
    # e.g., ["semantic.units", "computational.aggregations"]
    dimension_scores: dict[str, float] = field(default_factory=dict)

    # Risk assessment
    risk_level: str = "medium"  # "critical", "high", "medium"
    impact: str = ""  # Description of what can go wrong
    multiplier: float = 1.0  # Risk multiplier (1.0-3.0)

    # Combined score after multiplier
    combined_score: float = 0.0

    # Mitigation
    mitigation_options: list[ResolutionOption] = field(default_factory=list)

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def from_scores(
        cls,
        target: str,
        dimensions: list[str],
        scores: dict[str, float],
        risk_level: str,
        impact: str,
        multiplier: float,
    ) -> CompoundRisk:
        """Create a CompoundRisk from dimension scores."""
        dimension_scores = {d: scores.get(d, 0.0) for d in dimensions}
        avg_score = sum(dimension_scores.values()) / len(dimension_scores) if dimensions else 0.0
        combined = min(1.0, avg_score * multiplier)

        return cls(
            target=target,
            dimensions=dimensions,
            dimension_scores=dimension_scores,
            risk_level=risk_level,
            impact=impact,
            multiplier=multiplier,
            combined_score=combined,
        )


@dataclass
class CompoundRiskDefinition:
    """Definition of a compound risk pattern.

    Loaded from config/entropy/compound_risks.yaml.
    """

    risk_type: str  # e.g., "units_aggregations"
    dimensions: list[str]  # Dimensions that must both be high
    threshold: float = 0.5  # Score threshold for each dimension
    risk_level: str = "high"  # Resulting risk level
    impact_template: str = ""  # Template for impact description
    multiplier: float = 1.5  # Risk multiplier


@dataclass
class ResolutionCascade:
    """A single resolution that improves multiple entropy dimensions.

    Used to prioritize resolutions that have broad impact.
    """

    cascade_id: str = field(default_factory=lambda: str(uuid4()))
    action: str = ""  # e.g., "document_relationship", "transform_fix_referential_integrity"
    parameters: dict[str, Any] = field(default_factory=dict)

    # What gets fixed
    affected_targets: list[str] = field(default_factory=list)  # Columns/tables affected
    entropy_reductions: dict[str, float] = field(default_factory=dict)  # dimension -> reduction

    # Aggregated impact
    total_reduction: float = 0.0  # Sum of all reductions
    dimensions_improved: int = 0  # Count of dimensions improved

    # Effort and priority
    effort: str = "medium"  # low, medium, high
    priority_score: float = 0.0  # total_reduction / effort_factor

    # Description
    description: str = ""

    def calculate_priority(self) -> float:
        """Calculate priority score."""
        effort_factor = {"low": 1.0, "medium": 2.0, "high": 4.0}.get(self.effort, 2.0)
        self.total_reduction = sum(self.entropy_reductions.values())
        self.dimensions_improved = len(self.entropy_reductions)
        self.priority_score = self.total_reduction / effort_factor
        return self.priority_score
