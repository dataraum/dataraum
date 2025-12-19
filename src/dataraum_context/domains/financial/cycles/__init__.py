"""Financial cycle analysis module.

This module provides:
- Relationship structure analysis for LLM context
- Fiscal stability rules (deterministic, auditable)
- Anomaly detection for financial data
- LLM cycle classification
- LLM structure interpretation

Usage:
    from dataraum_context.domains.financial.cycles import (
        # Relationship gathering and structure
        gather_relationships,
        describe_relationship_structure,
        RelationshipStructure,
        RelationshipInfo,
        TableRole,
        CyclePath,
        EnrichedRelationship,
        # Fiscal rules
        assess_fiscal_stability,
        detect_financial_anomalies,
        # LLM classification
        classify_business_cycle_with_llm,
        classify_financial_cycle_with_llm,
        interpret_relationship_structure_with_llm,
        interpret_financial_quality_with_llm,
    )
"""

from dataraum_context.domains.financial.cycles.classifier import (
    classify_business_cycle_with_llm,
    classify_financial_cycle_with_llm,
    interpret_relationship_structure_with_llm,
)
from dataraum_context.domains.financial.cycles.detector import detect_financial_anomalies
from dataraum_context.domains.financial.cycles.interpreter import (
    interpret_financial_quality_with_llm,
)
from dataraum_context.domains.financial.cycles.relationships import (
    CyclePath,
    EnrichedRelationship,
    RelationshipInfo,
    RelationshipStructure,
    TableRole,
    describe_relationship_structure,
    gather_relationships,
)
from dataraum_context.domains.financial.cycles.rules import assess_fiscal_stability

__all__ = [
    # Relationship gathering and structure
    "gather_relationships",
    "describe_relationship_structure",
    "RelationshipStructure",
    "RelationshipInfo",
    "TableRole",
    "CyclePath",
    "EnrichedRelationship",
    # Fiscal rules
    "assess_fiscal_stability",
    "detect_financial_anomalies",
    # LLM classification
    "classify_business_cycle_with_llm",
    "classify_financial_cycle_with_llm",
    "interpret_relationship_structure_with_llm",
    "interpret_financial_quality_with_llm",
]
