"""Core models: ONLY truly shared base types.

ARCHITECTURAL PRINCIPLE: NO DOMAIN RE-EXPORTS
----------------------------------------------
This package contains ONLY truly shared base types (Result, DataType, ColumnRef, etc.)

Domain models live in their respective packages:
- analysis/typing/models.py     → Type inference models
- analysis/statistics/models.py → Statistical profiling models + quality (Benford, outliers)
- analysis/correlation/models.py → Correlation analysis models
- enrichment/models.py          → Enrichment models (topological, temporal, semantic)
- quality/models.py             → Quality synthesis models (issues, context output)
- quality/domains/models.py     → Domain-specific quality models (financial, etc.)
- sources/csv/models.py         → CSV staging models

Import domain models directly from their packages:
    from dataraum_context.analysis.statistics.models import ColumnProfile, NumericStats
    from dataraum_context.analysis.statistics import BenfordAnalysis, StatisticalQualityResult
    from dataraum_context.analysis.correlation.models import NumericCorrelation
    from dataraum_context.quality.domains.models import FinancialQualityConfig
    from dataraum_context.sources.csv.models import StagedTable
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
