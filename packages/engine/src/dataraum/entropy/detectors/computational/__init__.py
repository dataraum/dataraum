"""Computational layer entropy detectors.

Detectors for computational uncertainty:
- Derived value correctness
- Aggregation determinism
- Cross-table consistency
- Stock/flow temporal behavior (pooled witnesses)
"""

from dataraum.entropy.detectors.computational.cross_table_consistency import (
    CrossTableConsistencyDetector,
)
from dataraum.entropy.detectors.computational.derived_values import (
    DerivedValueDetector,
)
from dataraum.entropy.detectors.computational.temporal_behavior import (
    TemporalBehaviorDetector,
)

__all__ = [
    "CrossTableConsistencyDetector",
    "DerivedValueDetector",
    "TemporalBehaviorDetector",
]
