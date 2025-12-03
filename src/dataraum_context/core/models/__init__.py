"""Core models: ONLY truly shared base types and cross-domain models.

ARCHITECTURAL PRINCIPLE: NO DOMAIN RE-EXPORTS
----------------------------------------------
This package contains ONLY:
1. Truly shared base types (Result, DataType, ColumnRef, etc.)
2. Cross-domain models (correlation, domain_quality, quality_synthesis)

Domain models live in their respective packages:
- profiling/models.py    → Statistical profiling + quality models
- enrichment/models.py   → Enrichment models (topological, temporal, semantic)
- quality/models.py      → Quality rules, scores, anomalies
- staging/models.py      → Staging models

Import domain models directly from their packages:
    from dataraum_context.profiling.models import BenfordAnalysis
    from dataraum_context.enrichment.models import TopologicalQualityResult
    from dataraum_context.quality.models import QualityRule
"""

# =============================================================================
# Base Types (Truly Shared Across All Domains)
# =============================================================================

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
# Cross-Domain Models (core/models/*)
# =============================================================================
# Correlation (cross-domain analysis)
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

# Domain Quality (financial reporting, etc.)
from dataraum_context.core.models.domain_quality import (
    DomainQualityResult,
    DoubleEntryResult,
    FinancialQualityConfig,
    FinancialQualityIssue,
    FinancialQualityResult,
    FiscalPeriodIntegrityCheck,
    IntercompanyTransactionMatch,
    SignConventionConfig,
)
from dataraum_context.core.models.domain_quality import (
    SignConventionViolation as SignConventionViolationModel,
)

# Quality Synthesis (aggregation across pillars)
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
    # Cross-domain (Correlation)
    "NumericCorrelation",
    "CategoricalAssociationModel",
    "FunctionalDependencyModel",
    "DerivedColumnModel",
    "CorrelationMatrix",
    "CorrelationAnalysisResult",
    # Cross-domain (Domain Quality)
    "DomainQualityResult",
    "FinancialQualityResult",
    "DoubleEntryResult",
    "SignConventionViolationModel",
    "IntercompanyTransactionMatch",
    "FiscalPeriodIntegrityCheck",
    "FinancialQualityIssue",
    "FinancialQualityConfig",
    "SignConventionConfig",
    # Cross-domain (Quality Synthesis)
    "QualitySynthesisResult",
    "TableQualityAssessment",
    "ColumnQualityAssessment",
    "DimensionScore",
    "QualityDimension",
    "QualitySynthesisIssue",
    "QualitySynthesisSeverity",
    "DatasetQualityOverview",
]
