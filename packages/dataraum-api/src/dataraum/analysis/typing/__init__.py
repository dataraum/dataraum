"""Type inference and resolution module.

This module provides:
- Pattern-based type inference from VARCHAR values
- Pint unit detection for numeric columns
- Type resolution (VARCHAR → typed tables with quarantine)

Key principle: Type inference is based ONLY on value patterns and TRY_CAST success,
NOT on column names. Column names are semantically meaningful but fragile for type
inference (e.g., "balance" could be numeric or text).

Persisted metadata (interface to other modules):
- TypeCandidate: Detected type candidates with confidence scores
- TypeDecision: Final type decision (automatic or human override)

Usage:
    from dataraum.analysis.typing import (
        infer_type_candidates,
        resolve_types,
        TypeCandidate,
        TypeDecision,
    )
"""

from dataraum.analysis.typing.db_models import (
    TypeCandidate as DBTypeCandidate,
)
from dataraum.analysis.typing.db_models import (
    TypeDecision as DBTypeDecision,
)
from dataraum.analysis.typing.inference import infer_type_candidates
from dataraum.analysis.typing.models import (
    ColumnCastResult,
    TypeCandidate,
    TypeDecision,
    TypeResolutionResult,
)
from dataraum.analysis.typing.resolution import resolve_types

__all__ = [
    # Functions
    "infer_type_candidates",
    "resolve_types",
    # Pydantic models (computation)
    "TypeCandidate",
    "TypeDecision",
    "TypeResolutionResult",
    "ColumnCastResult",
    # DB models (persistence)
    "DBTypeCandidate",
    "DBTypeDecision",
]
