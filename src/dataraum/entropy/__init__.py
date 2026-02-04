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

    from dataraum.entropy.views import (
        build_for_graph,
        build_for_query,
        build_for_dashboard,
    )
    from dataraum.entropy.analysis import ColumnSummary, TableSummary
    from dataraum.entropy.core import EntropyObject, EntropyRepository

See docs/ENTROPY_IMPLEMENTATION_PLAN.md for architecture details.
"""

# Layer 1: Core - fundamental types
# Layer 2: Analysis - aggregation
from dataraum.entropy.analysis import (
    ColumnSummary,
    EntropyAggregator,
    RelationshipSummary,
    TableSummary,
)

# Configuration
from dataraum.entropy.config import (
    EntropyConfig,
    get_entropy_config,
)
from dataraum.entropy.core import (
    CompoundRisk,
    CompoundRiskDefinition,
    EntropyObject,
    EntropyRepository,
    ResolutionCascade,
    ResolutionOption,
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

# Dimensional summary (LLM-powered)
from dataraum.entropy.summary_agent import DimensionalSummaryAgent, DimensionalSummaryOutput

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

__all__ = [
    # Layer 1: Core
    "EntropyObject",
    "ResolutionOption",
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
    # Interpretation (LLM-powered)
    "EntropyInterpreter",
    "EntropyInterpretation",
    "EntropyInterpretationOutput",
    "InterpretationInput",
    "Assumption",
    "ResolutionAction",
    # Dimensional summary (LLM-powered)
    "DimensionalSummaryAgent",
    "DimensionalSummaryOutput",
]
