"""Core models: ONLY truly shared base types.

ARCHITECTURAL PRINCIPLE: NO DOMAIN RE-EXPORTS
----------------------------------------------
This package contains ONLY truly shared base types (Result, DataType, ColumnRef, etc.)

Domain models live in their respective packages:
- analysis/typing/models.py     → Type inference models
- analysis/statistics/models.py → Statistical profiling models + quality (Benford, outliers)
- analysis/correlation/models.py → Correlation analysis models
- analysis/validation/models.py → Generic validation models
- enrichment/models.py          → Enrichment models (topological, temporal, semantic)
- quality/models.py             → Quality synthesis models (issues, context output)
- sources/csv/models.py         → CSV staging models

Import domain models directly from their packages:
    from dataraum.analysis.statistics.models import ColumnProfile, NumericStats
    from dataraum.analysis.statistics import BenfordAnalysis, StatisticalQualityResult
    from dataraum.analysis.correlation.models import NumericCorrelation
    from dataraum.analysis.validation import ValidationAgent
    from dataraum.sources.csv.models import StagedTable
"""

# =============================================================================
# Base Types (Truly Shared Across All Domains)
# =============================================================================

from dataraum.core.models.base import (
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
