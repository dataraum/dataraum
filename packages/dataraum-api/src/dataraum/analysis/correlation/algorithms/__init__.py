"""Pure correlation algorithms.

These functions operate on numpy arrays and return plain dataclasses.
No database, no async, no Pydantic models - just math.
"""

from dataraum.analysis.correlation.algorithms.categorical import (
    AssociationResult,
    compute_cramers_v,
)
from dataraum.analysis.correlation.algorithms.multicollinearity import (
    DependencyGroupResult,
    MulticollinearityResult,
    compute_multicollinearity,
)
from dataraum.analysis.correlation.algorithms.numeric import (
    CorrelationResult,
    compute_pairwise_correlations,
)

__all__ = [
    # Numeric
    "CorrelationResult",
    "compute_pairwise_correlations",
    # Categorical
    "AssociationResult",
    "compute_cramers_v",
    # Multicollinearity
    "DependencyGroupResult",
    "MulticollinearityResult",
    "compute_multicollinearity",
]
