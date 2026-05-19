"""Structural layer entropy detectors.

Detectors for structural uncertainty:
- Schema-level ambiguity
- Type fidelity
- Relationship determinism
- Relationship quality (referential integrity, cardinality)

TODO: Add TypeDecisionEntropyDetector
    Measures uncertainty in HOW the type was decided (complements TypeFidelityDetector
    which measures parse success). Uses TypeDecision.decision_source:
    - manual: 0.0 (human verified)
    - automatic: 1.0 - confidence
    - fallback: 0.7 (no good candidate found)
    - override: 0.1 (config-based)
    Requires manual type override functionality to be useful.
"""

from dataraum.entropy.detectors.structural.relations import (
    JoinPathDeterminismDetector,
)
from dataraum.entropy.detectors.structural.relationship_entropy import (
    RelationshipEntropyDetector,
)
from dataraum.entropy.detectors.structural.types import TypeFidelityDetector

__all__ = [
    "TypeFidelityDetector",
    "JoinPathDeterminismDetector",
    "RelationshipEntropyDetector",
]
