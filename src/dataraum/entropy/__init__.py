"""Entropy layer for quantifying uncertainty in data.

Measures and manages data entropy across multiple dimensions
(structural, semantic, value, computational) to enable LLM-driven
analytics to make deterministic, reliable decisions.
"""

from dataraum.entropy.analysis import ColumnSummary
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.core import (
    EntropyObject,
    EntropyRepository,
    ResolutionOption,
)
from dataraum.entropy.interpretation import (
    EntropyInterpretation,
    EntropyInterpreter,
    InterpretationInput,
    TableInterpretationInput,
)
from dataraum.entropy.views import (
    EntropyForNetwork,
    EntropyForQuery,
    build_for_network,
    build_for_query,
)

__all__ = [
    # Core
    "EntropyObject",
    "ResolutionOption",
    "EntropyRepository",
    # Analysis
    "ColumnSummary",
    # Views
    "EntropyForNetwork",
    "EntropyForQuery",
    "build_for_network",
    "build_for_query",
    # Config
    "get_entropy_config",
    # Interpretation
    "EntropyInterpretation",
    "EntropyInterpreter",
    "InterpretationInput",
    "TableInterpretationInput",
]
