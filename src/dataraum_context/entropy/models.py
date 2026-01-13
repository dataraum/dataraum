"""Entropy layer data models.

This module defines the core data structures for the entropy layer.
All models follow the specification in docs/ENTROPY_MODELS.md.

Key models:
- EntropyObject: Core measurement with evidence, resolution options, context
- ResolutionOption: Actionable fix with effort and expected entropy reduction
- CompoundRisk: Dangerous dimension combination with multiplied impact
- ResolutionCascade: Single fix affecting multiple entropy dimensions
- EntropyContext: Aggregated entropy for graph agent consumption

Default thresholds loaded from config/entropy/thresholds.yaml.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from dataraum_context.entropy.config import EntropyConfig
    from dataraum_context.entropy.interpretation import EntropyInterpretation


def _get_config() -> EntropyConfig:
    """Get entropy config, avoiding circular import."""
    from dataraum_context.entropy.config import get_entropy_config

    return get_entropy_config()


@dataclass
class LLMContext:
    """Context for LLM/query agents.

    Provides information the agent needs to:
    - Understand the uncertainty
    - Decide how to handle it (answer, ask, refuse)
    - Generate appropriate caveats or assumptions
    """

    description: str = ""  # Plain language description of the entropy
    query_impact: str = ""  # How this affects query generation/results

    # For answering with assumptions
    best_guess: str | None = None  # Best guess if forced to assume
    best_guess_confidence: float = 0.0  # Confidence in the guess
    assumption_if_unresolved: str | None = None  # Assumption to state

    # For query generation
    filter_recommendation: str | None = None  # Suggested WHERE clause
    aggregation_recommendation: str | None = None  # Suggested aggregation
    join_recommendation: str | None = None  # Suggested join approach

    # Warnings and caveats
    warning: str | None = None  # Warning to include in response
    caveat_template: str | None = None  # Template for caveat in answer


@dataclass
class HumanContext:
    """Context for human users and administrators.

    Provides information for UI display, alerting, and manual resolution.
    """

    severity: str = "medium"  # none, low, medium, high, critical
    category: str = ""  # e.g., "Data Types", "Business Definitions", "Schema Design"
    message: str = ""  # Short human-readable message
    recommendation: str = ""  # Actionable recommendation

    # For UI display
    icon: str | None = None  # Suggested icon
    color: str | None = None  # Suggested color (for severity visualization)


@dataclass
class ResolutionOption:
    """An actionable resolution that can reduce entropy.

    Each resolution has an action type, parameters, expected impact,
    and effort level for prioritization.
    """

    action: str  # e.g., "add_column_alias", "declare_unit", "add_definition"
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

    # Context for different consumers
    llm_context: LLMContext = field(default_factory=LLMContext)
    human_context: HumanContext = field(default_factory=HumanContext)

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
class ColumnEntropyProfile:
    """Aggregated entropy profile for a single column."""

    column_id: str = ""
    column_name: str = ""
    table_name: str = ""

    # Per-layer scores (0.0-1.0)
    structural_entropy: float = 0.0
    semantic_entropy: float = 0.0
    value_entropy: float = 0.0
    computational_entropy: float = 0.0

    # Composite score
    composite_score: float = 0.0

    # Detailed breakdown by dimension
    dimension_scores: dict[str, float] = field(default_factory=dict)
    # e.g., {"structural.types.type_fidelity": 0.3, "semantic.units.unit_declared": 0.8}

    # High-entropy dimensions (score > 0.5)
    high_entropy_dimensions: list[str] = field(default_factory=list)

    # Resolution hints (top 3 by priority)
    top_resolution_hints: list[ResolutionOption] = field(default_factory=list)

    # Compound risks affecting this column
    compound_risks: list[CompoundRisk] = field(default_factory=list)

    # Readiness classification
    readiness: str = "investigate"  # ready, investigate, blocked

    # LLM-generated interpretation (optional, populated by EntropyInterpreter)
    interpretation: EntropyInterpretation | None = None

    def calculate_composite(self, weights: dict[str, float] | None = None) -> float:
        """Calculate composite score from layer scores.

        Default weights loaded from config/entropy/thresholds.yaml.
        """
        if weights is None:
            config = _get_config()
            weights = config.composite_weights
        self.composite_score = (
            self.structural_entropy * weights["structural"]
            + self.semantic_entropy * weights["semantic"]
            + self.value_entropy * weights["value"]
            + self.computational_entropy * weights["computational"]
        )
        return self.composite_score

    def update_high_entropy_dimensions(self, threshold: float | None = None) -> None:
        """Update list of high-entropy dimensions based on threshold.

        Default threshold loaded from config/entropy/thresholds.yaml.
        """
        if threshold is None:
            config = _get_config()
            threshold = config.high_entropy_threshold
        self.high_entropy_dimensions = [
            dim for dim, score in self.dimension_scores.items() if score >= threshold
        ]

    def update_readiness(self) -> None:
        """Update readiness classification based on composite score.

        Thresholds loaded from config/entropy/thresholds.yaml.
        """
        config = _get_config()
        self.readiness = config.get_readiness(self.composite_score)


@dataclass
class TableEntropyProfile:
    """Aggregated entropy profile for a table."""

    table_id: str = ""
    table_name: str = ""

    # Column profiles
    column_profiles: list[ColumnEntropyProfile] = field(default_factory=list)

    # Table-level scores (averages across columns)
    avg_structural_entropy: float = 0.0
    avg_semantic_entropy: float = 0.0
    avg_value_entropy: float = 0.0
    avg_computational_entropy: float = 0.0
    avg_composite_score: float = 0.0

    # Worst scores (max across columns)
    max_structural_entropy: float = 0.0
    max_semantic_entropy: float = 0.0
    max_value_entropy: float = 0.0
    max_computational_entropy: float = 0.0
    max_composite_score: float = 0.0

    # High-entropy columns (composite > 0.5)
    high_entropy_columns: list[str] = field(default_factory=list)

    # Blocked columns (composite > 0.8)
    blocked_columns: list[str] = field(default_factory=list)

    # Table-level compound risks
    compound_risks: list[CompoundRisk] = field(default_factory=list)

    # Readiness classification
    readiness: str = "investigate"  # ready, investigate, blocked
    readiness_blockers: list[str] = field(default_factory=list)

    def calculate_aggregates(self) -> None:
        """Calculate aggregate scores from column profiles.

        Thresholds loaded from config/entropy/thresholds.yaml.
        """
        if not self.column_profiles:
            return

        config = _get_config()
        high_threshold = config.high_entropy_threshold
        critical_threshold = config.critical_entropy_threshold

        n = len(self.column_profiles)

        # Calculate averages
        self.avg_structural_entropy = sum(p.structural_entropy for p in self.column_profiles) / n
        self.avg_semantic_entropy = sum(p.semantic_entropy for p in self.column_profiles) / n
        self.avg_value_entropy = sum(p.value_entropy for p in self.column_profiles) / n
        self.avg_computational_entropy = (
            sum(p.computational_entropy for p in self.column_profiles) / n
        )
        self.avg_composite_score = sum(p.composite_score for p in self.column_profiles) / n

        # Calculate maxes
        self.max_structural_entropy = max(p.structural_entropy for p in self.column_profiles)
        self.max_semantic_entropy = max(p.semantic_entropy for p in self.column_profiles)
        self.max_value_entropy = max(p.value_entropy for p in self.column_profiles)
        self.max_computational_entropy = max(p.computational_entropy for p in self.column_profiles)
        self.max_composite_score = max(p.composite_score for p in self.column_profiles)

        # Identify high-entropy and blocked columns using config thresholds
        self.high_entropy_columns = [
            p.column_name for p in self.column_profiles if p.composite_score >= high_threshold
        ]
        self.blocked_columns = [
            p.column_name for p in self.column_profiles if p.composite_score >= critical_threshold
        ]

        # Update readiness
        if self.blocked_columns:
            self.readiness = "blocked"
            self.readiness_blockers = self.blocked_columns
        elif self.high_entropy_columns:
            self.readiness = "investigate"
        else:
            self.readiness = "ready"


@dataclass
class RelationshipEntropyProfile:
    """Entropy profile for a relationship between tables."""

    from_table: str = ""
    from_column: str = ""
    to_table: str = ""
    to_column: str = ""

    # Relationship-specific entropy
    cardinality_entropy: float = 0.0  # Uncertainty in cardinality
    join_path_entropy: float = 0.0  # Ambiguity in join path
    referential_integrity_entropy: float = 0.0  # Orphan ratio
    semantic_clarity_entropy: float = 0.0  # Is relationship meaning clear?

    # Composite
    composite_score: float = 0.0

    # Is this join deterministic?
    is_deterministic: bool = True

    # Warning for graph agent
    join_warning: str | None = None

    def calculate_composite(self) -> float:
        """Calculate composite score from component scores."""
        self.composite_score = max(
            self.cardinality_entropy,
            self.join_path_entropy,
            self.referential_integrity_entropy,
            self.semantic_clarity_entropy,
        )
        self.is_deterministic = self.composite_score < 0.5
        return self.composite_score

    @property
    def relationship_key(self) -> str:
        """Return a unique key for this relationship."""
        return f"{self.from_table}.{self.from_column}->{self.to_table}.{self.to_column}"


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
    action: str = ""  # e.g., "rename_column", "add_fk_constraint"
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


@dataclass
class EntropyContext:
    """Complete entropy context for graph agent consumption.

    This is the primary interface between the entropy layer
    and the graph execution context.
    """

    # Per-column entropy
    column_profiles: dict[str, ColumnEntropyProfile] = field(default_factory=dict)
    # Key: "{table_name}.{column_name}"

    # Per-table entropy
    table_profiles: dict[str, TableEntropyProfile] = field(default_factory=dict)
    # Key: table_name

    # Per-relationship entropy
    relationship_profiles: dict[str, RelationshipEntropyProfile] = field(default_factory=dict)
    # Key: "{from_table}.{from_col}->{to_table}.{to_col}"

    # Global compound risks
    compound_risks: list[CompoundRisk] = field(default_factory=list)

    # Top resolution hints (across all targets)
    top_resolution_hints: list[ResolutionCascade] = field(default_factory=list)

    # LLM-generated interpretations (optional, keyed by "{table_name}.{column_name}")
    column_interpretations: dict[str, EntropyInterpretation] = field(default_factory=dict)

    # Summary statistics
    total_entropy_objects: int = 0
    high_entropy_count: int = 0  # score > 0.5
    critical_entropy_count: int = 0  # score > 0.8
    compound_risk_count: int = 0

    # Readiness assessment
    overall_readiness: str = "investigate"  # ready, investigate, blocked
    readiness_blockers: list[str] = field(default_factory=list)

    # Contract compliance (if evaluated)
    contract_compliance: dict[str, bool] = field(default_factory=dict)
    # Key: use_case name, Value: whether compliant

    # Computed timestamp
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def get_column_entropy(self, table: str, column: str) -> ColumnEntropyProfile | None:
        """Get entropy profile for a specific column."""
        key = f"{table}.{column}"
        return self.column_profiles.get(key)

    def get_high_entropy_columns(self, threshold: float | None = None) -> list[str]:
        """Get list of columns with entropy above threshold.

        Default threshold loaded from config/entropy/thresholds.yaml.
        """
        if threshold is None:
            config = _get_config()
            threshold = config.high_entropy_threshold
        return [
            key
            for key, profile in self.column_profiles.items()
            if profile.composite_score >= threshold
        ]

    def has_critical_risks(self) -> bool:
        """Check if any critical compound risks exist."""
        return any(r.risk_level == "critical" for r in self.compound_risks)

    def update_summary_stats(self) -> None:
        """Update summary statistics from profiles.

        Thresholds loaded from config/entropy/thresholds.yaml.
        """
        config = _get_config()
        high_threshold = config.high_entropy_threshold
        critical_threshold = config.critical_entropy_threshold

        self.high_entropy_count = len(self.get_high_entropy_columns(high_threshold))
        self.critical_entropy_count = len(self.get_high_entropy_columns(critical_threshold))
        self.compound_risk_count = len(self.compound_risks)

        # Update overall readiness
        if self.critical_entropy_count > 0 or self.has_critical_risks():
            self.overall_readiness = "blocked"
            self.readiness_blockers = self.get_high_entropy_columns(critical_threshold)
        elif self.high_entropy_count > 0:
            self.overall_readiness = "investigate"
        else:
            self.overall_readiness = "ready"
