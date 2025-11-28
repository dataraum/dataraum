"""
New 5-pillar context architecture models.

This module contains the revised schema aligned with the 5-pillar context architecture:
- Pillar 1: Statistical Context (statistical_context.py, correlation.py)
- Pillar 2: Topological Context (topological_context.py)
- Pillar 3: Semantic Context (semantic_context.py)
- Pillar 4: Temporal Context (temporal_context.py)
- Pillar 5: Quality Context (quality_context.py)

Core entities (sources, tables, columns) are in core.py.

Created: 2025-11-28
Replaces: storage/models.py (deprecated)
"""

# Import all models to ensure relationships are resolved
from dataraum_context.storage.models_v2 import core, correlation, statistical_context
from dataraum_context.storage.models_v2.base import Base, metadata_obj
from dataraum_context.storage.models_v2.core import Column, Source, Table

# Correlation and dependencies (Part of Pillar 1)
from dataraum_context.storage.models_v2.correlation import (
    CategoricalAssociation,
    ColumnCorrelation,
    DerivedColumn,
    FunctionalDependency,
)

# Statistical context (Pillar 1)
from dataraum_context.storage.models_v2.statistical_context import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)

__all__ = [
    # Base
    "Base",
    "metadata_obj",
    # Core entities
    "Source",
    "Table",
    "Column",
    # Pillar 1: Statistical
    "StatisticalProfile",
    "StatisticalQualityMetrics",
    # Pillar 1: Correlations and Dependencies
    "ColumnCorrelation",
    "CategoricalAssociation",
    "FunctionalDependency",
    "DerivedColumn",
]
