"""
New 5-pillar context architecture models.

This module contains the revised schema aligned with the 5-pillar context architecture:
- Pillar 1: Statistical Context (statistical_context.py, correlation.py)
- Pillar 2: Topological Context (topological_context.py, relationship.py)
- Pillar 3: Semantic Context (semantic_context.py)
- Pillar 4: Temporal Context (temporal_context.py)
- Pillar 5: Quality Context (domain_quality.py, quality_rules.py)

Additional supporting modules:
- core.py: Core entities (sources, tables, columns)
- type_inference.py: Type candidate and decision models
- ontology.py: Ontology definitions and applications
- workflow.py: Checkpoint and review queue
- llm.py: LLM response caching

Created: 2025-11-28
Replaces: storage/models.py (deprecated)
"""

# Import all model modules to ensure SQLAlchemy relationships are resolved
# ruff: noqa: F401
from dataraum_context.storage.models_v2 import (
    core,
    correlation,
    domain_quality,
    llm,
    ontology,
    quality_rules,
    relationship,
    schema,
    semantic_context,
    statistical_context,
    temporal_context,
    topological_context,
    type_inference,
    workflow,
)
from dataraum_context.storage.models_v2.base import Base, metadata_obj

# Core entities
from dataraum_context.storage.models_v2.core import Column, Source, Table

# Pillar 1: Correlation and dependencies
from dataraum_context.storage.models_v2.correlation import (
    CategoricalAssociation,
    ColumnCorrelation,
    DerivedColumn,
    FunctionalDependency,
)

# Pillar 5: Quality Context (domain-specific)
from dataraum_context.storage.models_v2.domain_quality import (
    DomainQualityMetrics,
    DoubleEntryCheck,
    FinancialQualityMetrics,
    FiscalPeriodIntegrity,
    IntercompanyTransaction,
    SignConventionViolation,
    TrialBalanceCheck,
)

# LLM integration
from dataraum_context.storage.models_v2.llm import LLMCache

# Ontology system
from dataraum_context.storage.models_v2.ontology import Ontology, OntologyApplication

# Pillar 5: Quality Context (rules-based)
from dataraum_context.storage.models_v2.quality_rules import (
    QualityResult,
    QualityRule,
    QualityScore,
)
from dataraum_context.storage.models_v2.relationship import JoinPath, Relationship

# Schema version tracking
from dataraum_context.storage.models_v2.schema import DBSchemaVersion

# Pillar 3: Semantic Context
from dataraum_context.storage.models_v2.semantic_context import (
    SemanticAnnotation,
    TableEntity,
)

# Pillar 1: Statistical Context
from dataraum_context.storage.models_v2.statistical_context import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)

# Pillar 4: Temporal Context
from dataraum_context.storage.models_v2.temporal_context import (
    ChangePoint,
    DistributionShift,
    SeasonalDecomposition,
    TemporalQualityMetrics,
    UpdateFrequencyHistory,
)

# Pillar 2: Topological Context
from dataraum_context.storage.models_v2.topological_context import (
    PersistentCycle,
    StructuralComplexityHistory,
    TopologicalQualityMetrics,
)

# Type inference
from dataraum_context.storage.models_v2.type_inference import TypeCandidate, TypeDecision

# Workflow management
from dataraum_context.storage.models_v2.workflow import Checkpoint, ReviewQueue

__all__ = [
    # Base
    "Base",
    "metadata_obj",
    # Core entities
    "Source",
    "Table",
    "Column",
    # Pillar 1: Statistical Context
    "StatisticalProfile",
    "StatisticalQualityMetrics",
    # Pillar 1: Correlations and Dependencies
    "ColumnCorrelation",
    "CategoricalAssociation",
    "FunctionalDependency",
    "DerivedColumn",
    # Pillar 2: Topological Context
    "TopologicalQualityMetrics",
    "PersistentCycle",
    "StructuralComplexityHistory",
    "Relationship",
    "JoinPath",
    # Pillar 3: Semantic Context
    "SemanticAnnotation",
    "TableEntity",
    # Pillar 4: Temporal Context
    "TemporalQualityMetrics",
    "SeasonalDecomposition",
    "ChangePoint",
    "DistributionShift",
    "UpdateFrequencyHistory",
    # Pillar 5: Domain Quality
    "DomainQualityMetrics",
    "FinancialQualityMetrics",
    "DoubleEntryCheck",
    "TrialBalanceCheck",
    "SignConventionViolation",
    "IntercompanyTransaction",
    "FiscalPeriodIntegrity",
    # Pillar 5: Quality Rules
    "QualityRule",
    "QualityResult",
    "QualityScore",
    # Type Inference
    "TypeCandidate",
    "TypeDecision",
    # Ontology System
    "Ontology",
    "OntologyApplication",
    # Workflow Management
    "Checkpoint",
    "ReviewQueue",
    # LLM Integration
    "LLMCache",
    # Schema Version
    "DBSchemaVersion",
]
