"""Profiling module for statistical analysis and type inference.

This module profiles staged data in two stages:

1. Schema profiling (raw tables):
   - Pattern detection (sample-based, stable)
   - Type inference → TypeCandidates with confidence scores

2. Statistics profiling (typed tables):
   - All row-based statistics on clean data
   - Correlations, multicollinearity, derived columns
"""

from dataraum_context.profiling.profiler import (
    profile_and_resolve_types,
    profile_schema,
    profile_statistics,
)

__all__ = [
    "profile_schema",
    "profile_statistics",
    "profile_and_resolve_types",
]
