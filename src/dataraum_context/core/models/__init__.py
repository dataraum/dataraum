"""Pydantic models - transitioning to 5-pillar architecture.

BACKWARDS COMPATIBILITY:
This package re-exports all models from the old core/models.py file to maintain
backwards compatibility. Existing code can continue to use:
    from dataraum_context.core.models import Result, DataType, etc.

NEW PILLAR-BASED MODELS:
New models are organized by context pillar:
- statistical.py: Pillar 1 (Statistical Context)
- topological.py: Pillar 2 (Topological Context) - to be created
- semantic.py: Pillar 3 (Semantic Context) - to be created
- temporal.py: Pillar 4 (Temporal Context) - to be created
- quality.py: Pillar 5 (Quality Context) - to be created

For new code, import pillar-specific models:
    from dataraum_context.core.models.statistical import BenfordTestResult
"""

# The simplest solution: manually import and re-export from the legacy models.py
# This avoids complex importlib magic and circular import issues

# We'll import the parent models.py by using a relative import trick
# Since we're in core/models/__init__.py, we can access ../models.py

from pathlib import Path

# Load the legacy models.py file directly
_models_py_path = Path(__file__).parent.parent / "models.py"

# Read and execute it in a clean namespace
_legacy_namespace = {"__name__": "dataraum_context.core.models_legacy"}
with open(_models_py_path) as f:
    exec(f.read(), _legacy_namespace)

# Export the models we need from the legacy file
Cardinality = _legacy_namespace["Cardinality"]
ColumnCastResult = _legacy_namespace["ColumnCastResult"]
ColumnContext = _legacy_namespace["ColumnContext"]
ColumnRef = _legacy_namespace["ColumnRef"]
ContextDocument = _legacy_namespace["ContextDocument"]
DataType = _legacy_namespace["DataType"]
DecisionSource = _legacy_namespace["DecisionSource"]
DetectedPattern = _legacy_namespace["DetectedPattern"]
EntityDetection = _legacy_namespace["EntityDetection"]
JoinPath = _legacy_namespace["JoinPath"]
JoinStep = _legacy_namespace["JoinStep"]
MetricDefinition = _legacy_namespace["MetricDefinition"]
ProfileResult = _legacy_namespace["ProfileResult"]
QualitySeverity = _legacy_namespace["QualitySeverity"]
Relationship = _legacy_namespace["Relationship"]
RelationshipType = _legacy_namespace["RelationshipType"]
Result = _legacy_namespace["Result"]
SemanticAnnotation = _legacy_namespace["SemanticAnnotation"]
SemanticEnrichmentResult = _legacy_namespace["SemanticEnrichmentResult"]
SemanticRole = _legacy_namespace["SemanticRole"]
StagedColumn = _legacy_namespace["StagedColumn"]
StagedTable = _legacy_namespace["StagedTable"]
StagingResult = _legacy_namespace["StagingResult"]
TableContext = _legacy_namespace["TableContext"]
TableRef = _legacy_namespace["TableRef"]
TemporalEnrichmentResult = _legacy_namespace["TemporalEnrichmentResult"]
TemporalGap = _legacy_namespace["TemporalGap"]
TemporalProfile = _legacy_namespace["TemporalProfile"]
TopologyEnrichmentResult = _legacy_namespace["TopologyEnrichmentResult"]
TypeCandidate = _legacy_namespace["TypeCandidate"]
TypeDecision = _legacy_namespace["TypeDecision"]
TypeResolutionResult = _legacy_namespace["TypeResolutionResult"]
SourceConfig = _legacy_namespace["SourceConfig"]
ColumnProfile = _legacy_namespace["ColumnProfile"]
QualityRule = _legacy_namespace["QualityRule"]
RuleResult = _legacy_namespace["RuleResult"]
QualityScore = _legacy_namespace["QualityScore"]
Anomaly = _legacy_namespace["Anomaly"]
SuggestedQuery = _legacy_namespace["SuggestedQuery"]
ContextSummary = _legacy_namespace["ContextSummary"]
QualitySummary = _legacy_namespace["QualitySummary"]
DomainConcept = _legacy_namespace["DomainConcept"]

# Legacy models with naming conflicts (we'll use "Legacy" prefix)
LegacyNumericStats = _legacy_namespace["NumericStats"]
LegacyStringStats = _legacy_namespace["StringStats"]
LegacyHistogramBucket = _legacy_namespace["HistogramBucket"]
LegacyValueCount = _legacy_namespace["ValueCount"]
LegacyQualityIssue = _legacy_namespace.get("QualityIssue")  # May not exist

# Statistical context (Pillar 1) - new models
# Correlation analysis (Part of Pillar 1) - new models
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

# Domain quality (Pillar 5) - new models
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

# Temporal context (Pillar 4) - new models
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

# Topological context (Pillar 2) - new models
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

__all__ = [
    # Legacy models (for backwards compatibility)
    "Cardinality",
    "ColumnCastResult",
    "ColumnContext",
    "ColumnProfile",
    "ColumnRef",
    "ContextDocument",
    "DataType",
    "DecisionSource",
    "DetectedPattern",
    "EntityDetection",
    "JoinPath",
    "JoinStep",
    "MetricDefinition",
    "ProfileResult",
    "QualitySeverity",
    "Relationship",
    "RelationshipType",
    "Result",
    "SemanticAnnotation",
    "SemanticEnrichmentResult",
    "SemanticRole",
    "StagedColumn",
    "StagedTable",
    "StagingResult",
    "TableContext",
    "TableRef",
    "TemporalEnrichmentResult",
    "TemporalGap",
    "TemporalProfile",
    "TopologyEnrichmentResult",
    "TypeCandidate",
    "TypeDecision",
    "TypeResolutionResult",
    "SourceConfig",
    "QualityRule",
    "RuleResult",
    "QualityScore",
    "Anomaly",
    "SuggestedQuery",
    "ContextSummary",
    "QualitySummary",
    "DomainConcept",
    # Legacy with prefix
    "LegacyNumericStats",
    "LegacyStringStats",
    "LegacyHistogramBucket",
    "LegacyValueCount",
    # New Statistical Profile models (Pillar 1)
    "StatisticalProfile",
    "NumericStats",
    "StringStats",
    "HistogramBucket",
    "ValueCount",
    "EntropyStats",
    "UniquenessStats",
    "OrderStats",
    # New Statistical Quality models (Pillar 1)
    "StatisticalQualityMetrics",
    "BenfordTestResult",
    "DistributionStabilityResult",
    "OutlierDetectionResult",
    "VIFResult",
    "QualityIssue",
    # New Results (Pillar 1)
    "StatisticalProfilingResult",
    "StatisticalQualityResult",
    # Correlation Analysis (Pillar 1)
    "NumericCorrelation",
    "CategoricalAssociationModel",
    "FunctionalDependencyModel",
    "DerivedColumnModel",
    "CorrelationMatrix",
    "CorrelationAnalysisResult",
    # Topological Quality (Pillar 2)
    "BettiNumbers",
    "PersistencePoint",
    "PersistenceDiagram",
    "PersistentCycleResult",
    "HomologicalStability",
    "StructuralComplexity",
    "TopologicalAnomaly",
    "TopologicalQualityResult",
    "TopologicalSummary",
    # Temporal Quality (Pillar 4)
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
]
