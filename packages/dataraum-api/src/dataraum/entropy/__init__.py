"""Entropy layer for quantifying uncertainty in data.

This module provides tools for measuring and managing data entropy across
multiple dimensions (structural, semantic, value, computational) to enable
LLM-driven analytics to make deterministic, reliable decisions.

## Layer Architecture

The entropy framework follows a 4-layer architecture:

### Layer 1: Core (entropy/core/)
- EntropyObject: Core measurement unit
- EntropyRepository: Data access with typed table enforcement

### Layer 2: Analysis (entropy/analysis/)
- ColumnSummary, TableSummary, RelationshipSummary: Computed aggregates
- EntropyAggregator: Dynamic summary computation

### Layer 3: Views (entropy/views/)
- build_for_graph(): For graph execution context
- build_for_query(): For query agent with contract evaluation
- build_for_dashboard(): For API endpoints

### Layer 4: Workflow (future)

## Usage

New API (recommended):
    from dataraum.entropy.views import (
        build_for_graph,
        build_for_query,
        build_for_dashboard,
    )
    from dataraum.entropy.analysis import ColumnSummary, TableSummary

Legacy API (for backward compatibility):
    from dataraum.entropy import (
        EntropyObject,
        ResolutionOption,
        ColumnEntropyProfile,  # Legacy - use ColumnSummary
        TableEntropyProfile,   # Legacy - use TableSummary
        EntropyContext,        # Legacy - use views
    )

See docs/ENTROPY_IMPLEMENTATION_PLAN.md for architecture details.
"""

# Layer 1: Core - fundamental types
from dataraum.entropy.core import (
    CompoundRisk,
    CompoundRiskDefinition,
    EntropyObject,
    EntropyRepository,
    HumanContext,
    LLMContext,
    ResolutionCascade,
    ResolutionOption,
)

# Layer 2: Analysis - aggregation
from dataraum.entropy.analysis import (
    ColumnSummary,
    EntropyAggregator,
    RelationshipSummary,
    TableSummary,
)

# Layer 3: Views - caller-specific builders
from dataraum.entropy.views import (
    EntropyForDashboard,
    EntropyForGraph,
    EntropyForQuery,
    build_for_dashboard,
    build_for_graph,
    build_for_query,
    format_entropy_for_prompt,
)

# Configuration
from dataraum.entropy.config import (
    EntropyConfig,
    clear_config_cache,
    get_entropy_config,
    load_entropy_config,
)

# Legacy API (for backward compatibility)
# These are still needed by compound_risk.py, contracts.py, interpretation.py
from dataraum.entropy.models import (
    ColumnEntropyProfile,
    EntropyContext,
    RelationshipEntropyProfile,
    TableEntropyProfile,
)

# Legacy context builder (for backward compatibility)
from dataraum.entropy.context import (
    build_entropy_context,
    get_column_entropy_summary,
    get_table_entropy_summary,
)

# Interpretation (LLM-powered)
from dataraum.entropy.interpretation import (
    Assumption,
    EntropyInterpretation,
    EntropyInterpretationOutput,
    EntropyInterpreter,
    InterpretationInput,
    ResolutionAction,
)

# Query-time refinement
from dataraum.entropy.query_refinement import (
    QueryRefinementResult,
    find_columns_in_query,
    refine_interpretations_for_query,
)

__all__ = [
    # Layer 1: Core
    "EntropyObject",
    "ResolutionOption",
    "LLMContext",
    "HumanContext",
    "CompoundRisk",
    "CompoundRiskDefinition",
    "ResolutionCascade",
    "EntropyRepository",
    # Layer 2: Analysis
    "ColumnSummary",
    "TableSummary",
    "RelationshipSummary",
    "EntropyAggregator",
    # Layer 3: Views
    "EntropyForGraph",
    "EntropyForQuery",
    "EntropyForDashboard",
    "build_for_graph",
    "build_for_query",
    "build_for_dashboard",
    "format_entropy_for_prompt",
    # Configuration
    "EntropyConfig",
    "get_entropy_config",
    "load_entropy_config",
    "clear_config_cache",
    # Legacy (backward compatibility)
    "ColumnEntropyProfile",
    "TableEntropyProfile",
    "RelationshipEntropyProfile",
    "EntropyContext",
    "build_entropy_context",
    "get_column_entropy_summary",
    "get_table_entropy_summary",
    # Interpretation (LLM-powered)
    "EntropyInterpreter",
    "EntropyInterpretation",
    "EntropyInterpretationOutput",
    "InterpretationInput",
    "Assumption",
    "ResolutionAction",
    # Query-time refinement
    "QueryRefinementResult",
    "find_columns_in_query",
    "refine_interpretations_for_query",
]
