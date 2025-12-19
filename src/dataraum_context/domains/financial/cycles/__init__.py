"""Financial cycle analysis module.

This module provides:
- Relationship structure analysis for LLM context
- Fiscal stability rules (deterministic, auditable)
- Anomaly detection for financial data
- LLM cycle classification
- LLM quality interpretation

Usage:
    from dataraum_context.domains.financial.cycles import (
        # Rich structure analysis (preferred)
        gather_relationships,
        describe_relationship_structure,
        RelationshipStructure,
        RelationshipInfo,
        TableRole,
        CyclePath,
        # Legacy (backward compat)
        analyze_relationship_graph,
        EnrichedRelationship,
        # Fiscal rules
        assess_fiscal_stability,
        detect_financial_anomalies,
        # LLM classification
        classify_financial_cycle_with_llm,
        classify_cross_table_cycle_with_llm,
        interpret_financial_quality_with_llm,
    )
"""

from dataraum_context.domains.financial.cycles.classifier import (
    classify_cross_table_cycle_with_llm,
    classify_financial_cycle_with_llm,
)
from dataraum_context.domains.financial.cycles.detector import detect_financial_anomalies
from dataraum_context.domains.financial.cycles.interpreter import (
    interpret_financial_quality_with_llm,
)
from dataraum_context.domains.financial.cycles.relationships import (
    # New rich models
    CyclePath,
    # Legacy (backward compat)
    EnrichedRelationship,
    GraphAnalysisResult,
    RelationshipInfo,
    RelationshipStructure,
    TableRole,
    analyze_relationship_graph,
    analyze_relationship_graph_detailed,
    build_relationship_graph,
    # Main functions
    describe_relationship_structure,
    gather_relationships,
)
from dataraum_context.domains.financial.cycles.rules import assess_fiscal_stability

__all__ = [
    # Rich structure analysis (preferred)
    "gather_relationships",
    "describe_relationship_structure",
    "RelationshipStructure",
    "RelationshipInfo",
    "TableRole",
    "CyclePath",
    # Legacy (backward compat)
    "analyze_relationship_graph",
    "analyze_relationship_graph_detailed",
    "build_relationship_graph",
    "EnrichedRelationship",
    "GraphAnalysisResult",
    # Fiscal rules
    "assess_fiscal_stability",
    "detect_financial_anomalies",
    # LLM classification
    "classify_financial_cycle_with_llm",
    "classify_cross_table_cycle_with_llm",
    "interpret_financial_quality_with_llm",
]
