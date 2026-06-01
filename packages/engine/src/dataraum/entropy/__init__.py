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
)
from dataraum.entropy.views import (
    EntropyForQuery,
    EntropyForReadiness,
    build_for_query,
    build_for_readiness,
)

__all__ = [
    # Core
    "EntropyObject",
    "EntropyRepository",
    # Analysis
    "ColumnSummary",
    # Views
    "EntropyForReadiness",
    "EntropyForQuery",
    "build_for_readiness",
    "build_for_query",
    # Config
    "get_entropy_config",
]
