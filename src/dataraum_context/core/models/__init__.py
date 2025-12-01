"""Pydantic models organized by the 5-pillar architecture.

This module re-exports all models for convenient access. Models are organized by:
- Base types: Common enums and fundamental types
- Pillar-specific: Statistical, Topological, Semantic, Temporal, Quality
- Domain modules: Staging, Profiling, Enrichment, Quality, Context

For new code, prefer importing directly from specific modules:
    from dataraum_context.core.models.base import Result, DataType
    from dataraum_context.core.models.statistical import BenfordTestResult
    from dataraum_context.staging.models import StagingResult

For backwards compatibility, all models are re-exported from this package:
    from dataraum_context.core.models import Result, BenfordTestResult, StagingResult
"""

# =============================================================================
# Base Models (Common Types)
# =============================================================================

# Context models
from dataraum_context.context.models import (
    ColumnContext,
    ContextDocument,
    ContextSummary,
    DomainConcept,
    MetricDefinition,
    QualitySummary,
    SuggestedQuery,
    TableContext,
)
from dataraum_context.core.models.base import (
    Cardinality,
    ColumnRef,
    DataType,
    DecisionSource,
    QualitySeverity,
    RelationshipType,
    Result,
    SemanticRole,
    SourceConfig,
    TableRef,
)

# =============================================================================
# Statistical Context (Pillar 1)
# =============================================================================
from dataraum_context.core.models.correlation import (
    CategoricalAssociation as CategoricalAssociationModel,
)
from dataraum_context.core.models.correlation import (
    CorrelationAnalysisResult,
    CorrelationMatrix,
    NumericCorrelation,
)
from dataraum_context.core.models.correlation import (
    DerivedColumn as DerivedColumnModel,
)
from dataraum_context.core.models.correlation import (
    FunctionalDependency as FunctionalDependencyModel,
)

# =============================================================================
# Domain Quality (Pillar 5)
# =============================================================================
from dataraum_context.core.models.domain_quality import (
    DomainQualityResult,
    DoubleEntryResult,
    FinancialQualityConfig,
    FinancialQualityIssue,
    FinancialQualityResult,
    FiscalPeriodIntegrityCheck,
    IntercompanyTransactionMatch,
    SignConventionConfig,
    TrialBalanceResult,
)
from dataraum_context.core.models.domain_quality import (
    SignConventionViolation as SignConventionViolationModel,
)

# =============================================================================
# Quality Synthesis (Pillar 5 - Aggregation)
# =============================================================================
from dataraum_context.core.models.quality_synthesis import (
    ColumnQualityAssessment,
    DatasetQualityOverview,
    DimensionScore,
    QualityDimension,
    QualitySynthesisResult,
    TableQualityAssessment,
)
from dataraum_context.core.models.quality_synthesis import (
    QualityIssue as QualitySynthesisIssue,
)
from dataraum_context.core.models.quality_synthesis import (
    QualitySeverity as QualitySynthesisSeverity,
)
from dataraum_context.core.models.statistical import (
    BenfordTestResult,
    DistributionStabilityResult,
    EntropyStats,
    HistogramBucket,
    NumericStats,
    OrderStats,
    OutlierDetectionResult,
    QualityIssue,
    StatisticalProfile,
    StatisticalProfilingResult,
    StatisticalQualityMetrics,
    StatisticalQualityResult,
    StringStats,
    UniquenessStats,
    ValueCount,
    VIFResult,
)

# =============================================================================
# Temporal Context (Pillar 4)
# =============================================================================
from dataraum_context.core.models.temporal import (
    ChangePointResult,
    DistributionShiftResult,
    DistributionStabilityAnalysis,
    FiscalCalendarAnalysis,
    SeasonalDecompositionResult,
    SeasonalityAnalysis,
    TemporalCompletenessAnalysis,
    TemporalGapInfo,
    TemporalQualityIssue,
    TemporalQualityResult,
    TemporalQualitySummary,
    TrendAnalysis,
    UpdateFrequencyAnalysis,
)

# =============================================================================
# Topological Context (Pillar 2)
# =============================================================================
from dataraum_context.core.models.topological import (
    BettiNumbers,
    HomologicalStability,
    PersistenceDiagram,
    PersistencePoint,
    PersistentCycleResult,
    StructuralComplexity,
    TopologicalAnomaly,
    TopologicalQualityResult,
    TopologicalSummary,
)

# Enrichment models
from dataraum_context.enrichment.models import (
    EntityDetection,
    JoinPath,
    JoinStep,
    Relationship,
    SemanticAnnotation,
    SemanticEnrichmentResult,
    TemporalEnrichmentResult,
    TemporalGap,
    TemporalProfile,
    TopologyEnrichmentResult,
)

# Profiling models
from dataraum_context.profiling.models import (
    ColumnCastResult,
    ColumnProfile,
    DetectedPattern,
    ProfileResult,
    TypeCandidate,
    TypeDecision,
    TypeResolutionResult,
)

# Quality models
from dataraum_context.quality.models import (
    Anomaly,
    QualityRule,
    QualityScore,
    RuleResult,
)

# =============================================================================
# Domain Module Models
# =============================================================================
# Staging models
from dataraum_context.staging.models import (
    StagedColumn,
    StagedTable,
    StagingResult,
)

# =============================================================================
# Public API
# =============================================================================

__all__ = [
    # Base types
    "Result",
    "DataType",
    "SemanticRole",
    "RelationshipType",
    "Cardinality",
    "QualitySeverity",
    "DecisionSource",
    "ColumnRef",
    "TableRef",
    "SourceConfig",
    # Statistical (Pillar 1)
    "StatisticalProfile",
    "NumericStats",
    "StringStats",
    "HistogramBucket",
    "ValueCount",
    "EntropyStats",
    "UniquenessStats",
    "OrderStats",
    "StatisticalQualityMetrics",
    "BenfordTestResult",
    "DistributionStabilityResult",
    "OutlierDetectionResult",
    "VIFResult",
    "QualityIssue",
    "StatisticalProfilingResult",
    "StatisticalQualityResult",
    "NumericCorrelation",
    "CategoricalAssociationModel",
    "FunctionalDependencyModel",
    "DerivedColumnModel",
    "CorrelationMatrix",
    "CorrelationAnalysisResult",
    # Topological (Pillar 2)
    "BettiNumbers",
    "PersistencePoint",
    "PersistenceDiagram",
    "PersistentCycleResult",
    "HomologicalStability",
    "StructuralComplexity",
    "TopologicalAnomaly",
    "TopologicalQualityResult",
    "TopologicalSummary",
    # Temporal (Pillar 4)
    "SeasonalityAnalysis",
    "SeasonalDecompositionResult",
    "TrendAnalysis",
    "ChangePointResult",
    "UpdateFrequencyAnalysis",
    "FiscalCalendarAnalysis",
    "DistributionShiftResult",
    "DistributionStabilityAnalysis",
    "TemporalGapInfo",
    "TemporalCompletenessAnalysis",
    "TemporalQualityIssue",
    "TemporalQualityResult",
    "TemporalQualitySummary",
    # Domain Quality (Pillar 5)
    "DomainQualityResult",
    "FinancialQualityResult",
    "DoubleEntryResult",
    "TrialBalanceResult",
    "SignConventionViolationModel",
    "IntercompanyTransactionMatch",
    "FiscalPeriodIntegrityCheck",
    "FinancialQualityIssue",
    "FinancialQualityConfig",
    "SignConventionConfig",
    # Quality Synthesis (Pillar 5)
    "QualitySynthesisResult",
    "TableQualityAssessment",
    "ColumnQualityAssessment",
    "DimensionScore",
    "QualityDimension",
    "QualitySynthesisIssue",
    "QualitySynthesisSeverity",
    "DatasetQualityOverview",
    # Staging
    "StagedColumn",
    "StagedTable",
    "StagingResult",
    # Profiling
    "DetectedPattern",
    "TypeCandidate",
    "ColumnProfile",
    "ProfileResult",
    "TypeDecision",
    "ColumnCastResult",
    "TypeResolutionResult",
    # Enrichment
    "SemanticAnnotation",
    "EntityDetection",
    "Relationship",
    "JoinStep",
    "JoinPath",
    "TemporalGap",
    "TemporalProfile",
    "SemanticEnrichmentResult",
    "TopologyEnrichmentResult",
    "TemporalEnrichmentResult",
    # Quality
    "QualityRule",
    "RuleResult",
    "QualityScore",
    "Anomaly",
    # Context
    "ColumnContext",
    "TableContext",
    "MetricDefinition",
    "DomainConcept",
    "QualitySummary",
    "SuggestedQuery",
    "ContextSummary",
    "ContextDocument",
]
