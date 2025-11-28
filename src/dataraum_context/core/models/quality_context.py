"""Quality context synthesis models (Pillar 5).

Pydantic models for unified quality assessment that aggregates
metrics from all pillars.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class QualityDimensionDetail(BaseModel):
    """Detailed breakdown of how a dimension score was calculated."""

    detail_id: UUID
    dimension: str  # 'completeness', 'consistency', 'accuracy', 'timeliness', 'uniqueness'
    dimension_score: float = Field(ge=0.0, le=1.0)
    component_scores: dict[str, float]  # Which metrics contributed and their values
    calculation_method: str  # Description of how score was calculated
    contributing_metrics: list[str]  # IDs or names of metrics used


class QualityIssue(BaseModel):
    """Aggregated quality issue from any pillar."""

    issue_id: UUID
    issue_type: str
    severity: str  # 'minor', 'moderate', 'severe', 'critical'
    category: str  # 'statistical', 'topological', 'temporal', 'domain'
    description: str
    affected_entities: list[str] = Field(default_factory=list)  # Tables, columns affected
    source_pillar: str  # Which pillar detected this
    source_metric_id: UUID | None = None
    recommendation: str | None = None
    auto_fixable: bool = False


class QualityTrendPoint(BaseModel):
    """Single point in quality trend."""

    timestamp: datetime
    score: float = Field(ge=0.0, le=1.0)
    is_regression: bool = False
    change_magnitude: float | None = None


class QualityContextResult(BaseModel):
    """Complete quality assessment result.

    Synthesizes quality from all pillars into standard DQ dimensions.
    """

    context_id: UUID
    source_id: UUID
    computed_at: datetime

    # Standard data quality dimensions (0-1 scale)
    completeness_score: float = Field(
        ge=0.0, le=1.0, description="Data completeness from statistical + temporal"
    )
    consistency_score: float = Field(
        ge=0.0, le=1.0, description="Data consistency from semantic + topological"
    )
    accuracy_score: float = Field(
        ge=0.0, le=1.0, description="Data accuracy from domain-specific rules"
    )
    timeliness_score: float = Field(
        ge=0.0, le=1.0, description="Data timeliness from temporal metrics"
    )
    uniqueness_score: float = Field(
        ge=0.0, le=1.0, description="Data uniqueness from statistical deduplication"
    )

    # Overall quality score (weighted average of dimensions)
    overall_score: float = Field(ge=0.0, le=1.0)

    # Dimension details
    dimension_details: list[QualityDimensionDetail] = Field(default_factory=list)

    # Aggregated issues
    critical_issues: list[QualityIssue] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)

    # Summary flags
    has_critical_issues: bool
    has_warnings: bool

    # Quality grade (A-F based on overall score)
    quality_grade: str = Field(pattern="^[A-F][+-]?$")

    def calculate_grade(self) -> str:
        """Calculate letter grade from overall score."""
        score = self.overall_score
        if score >= 0.97:
            return "A+"
        elif score >= 0.93:
            return "A"
        elif score >= 0.90:
            return "A-"
        elif score >= 0.87:
            return "B+"
        elif score >= 0.83:
            return "B"
        elif score >= 0.80:
            return "B-"
        elif score >= 0.77:
            return "C+"
        elif score >= 0.73:
            return "C"
        elif score >= 0.70:
            return "C-"
        elif score >= 0.67:
            return "D+"
        elif score >= 0.63:
            return "D"
        elif score >= 0.60:
            return "D-"
        else:
            return "F"


class QualityWeights(BaseModel):
    """Configurable weights for quality dimensions.

    Used to calculate overall_score as weighted average.
    """

    completeness: float = Field(default=0.25, ge=0.0, le=1.0)
    consistency: float = Field(default=0.20, ge=0.0, le=1.0)
    accuracy: float = Field(default=0.25, ge=0.0, le=1.0)
    timeliness: float = Field(default=0.15, ge=0.0, le=1.0)
    uniqueness: float = Field(default=0.15, ge=0.0, le=1.0)

    def validate_sum(self) -> bool:
        """Ensure weights sum to 1.0."""
        total = (
            self.completeness + self.consistency + self.accuracy + self.timeliness + self.uniqueness
        )
        return abs(total - 1.0) < 0.001  # Allow small floating point error


class QualitySummary(BaseModel):
    """Human-readable quality summary."""

    overall_grade: str
    overall_score: float
    strengths: list[str] = Field(default_factory=list)  # High-scoring dimensions
    weaknesses: list[str] = Field(default_factory=list)  # Low-scoring dimensions
    top_issues: list[str] = Field(default_factory=list)  # Most important issues
    top_recommendations: list[str] = Field(default_factory=list)  # Most important actions

    # One-sentence summary
    summary: str
