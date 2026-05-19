"""Computational layer entropy detectors.

Detectors for computational uncertainty:
- Derived value correctness
- Aggregation determinism
- Cross-table consistency
"""

from dataraum.entropy.detectors.computational.cross_table_consistency import (
    CrossTableConsistencyDetector,
)
from dataraum.entropy.detectors.computational.derived_values import (
    DerivedValueDetector,
)

__all__ = [
    "CrossTableConsistencyDetector",
    "DerivedValueDetector",
]
