"""Core models: ONLY truly shared base types.

ARCHITECTURAL PRINCIPLE: NO DOMAIN RE-EXPORTS
----------------------------------------------
This package contains ONLY truly shared base types (Result, DataType, ColumnRef, etc.)

Domain models live in their respective packages:
- profiling/models.py           → Statistical profiling + correlation models
- enrichment/models.py          → Enrichment models (topological, temporal, semantic)
- quality/models.py             → Quality synthesis models
- quality/domains/models.py     → Domain-specific quality models (financial, etc.)
- staging/models.py             → Staging models

Import domain models directly from their packages:
    from dataraum_context.profiling.models import BenfordAnalysis, NumericCorrelation
    from dataraum_context.enrichment.models import TopologicalQualityResult
    from dataraum_context.quality.models import QualityRule, QualitySynthesisResult
    from dataraum_context.quality.domains.models import FinancialQualityConfig
    from dataraum_context.staging.models import StagedTable
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
# Public API
# =============================================================================

__all__ = [
    # Base types - truly shared across all domains
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
]
