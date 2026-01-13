"""Entropy layer for quantifying uncertainty in data.

This module provides tools for measuring and managing data entropy across
multiple dimensions (structural, semantic, value, computational) to enable
LLM-driven analytics to make deterministic, reliable decisions.

Key concepts:
- EntropyObject: Core measurement for a specific dimension/target
- ResolutionOption: Actionable fix that can reduce entropy
- CompoundRisk: Dangerous combination of high-entropy dimensions
- EntropyContext: Aggregated entropy for graph agent consumption

Usage:
    from dataraum_context.entropy import (
        EntropyObject,
        ResolutionOption,
        ColumnEntropyProfile,
        TableEntropyProfile,
        EntropyContext,
        CompoundRisk,
    )

See docs/ENTROPY_IMPLEMENTATION_PLAN.md for architecture details.
"""

from dataraum_context.entropy.config import (
    EntropyConfig,
    clear_config_cache,
    get_entropy_config,
    load_entropy_config,
)
from dataraum_context.entropy.context import (
    build_entropy_context,
    get_column_entropy_summary,
    get_table_entropy_summary,
)
from dataraum_context.entropy.interpretation import (
    Assumption,
    EntropyInterpretation,
    EntropyInterpreter,
    InterpretationInput,
    ResolutionAction,
    create_fallback_interpretation,
)
from dataraum_context.entropy.models import (
    ColumnEntropyProfile,
    CompoundRisk,
    CompoundRiskDefinition,
    EntropyContext,
    EntropyObject,
    HumanContext,
    LLMContext,
    RelationshipEntropyProfile,
    ResolutionCascade,
    ResolutionOption,
    TableEntropyProfile,
)

__all__ = [
    # Configuration
    "EntropyConfig",
    "get_entropy_config",
    "load_entropy_config",
    "clear_config_cache",
    # Core models
    "EntropyObject",
    "ResolutionOption",
    "LLMContext",
    "HumanContext",
    # Aggregation models
    "ColumnEntropyProfile",
    "TableEntropyProfile",
    "RelationshipEntropyProfile",
    # Compound risk
    "CompoundRisk",
    "CompoundRiskDefinition",
    # Resolution
    "ResolutionCascade",
    # Context
    "EntropyContext",
    # Context builder
    "build_entropy_context",
    "get_column_entropy_summary",
    "get_table_entropy_summary",
    # Interpretation (LLM-powered)
    "EntropyInterpreter",
    "EntropyInterpretation",
    "InterpretationInput",
    "Assumption",
    "ResolutionAction",
    "create_fallback_interpretation",
]
