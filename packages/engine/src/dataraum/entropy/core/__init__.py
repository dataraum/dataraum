"""Entropy core module: foundation types and storage.

Layer 1 of the entropy framework - provides:
- EntropyObject (the core measurement unit)
- EntropyRepository for loading/saving with typed table enforcement
- Re-exports from models.py for backward compatibility
"""

from dataraum.entropy.core.storage import EntropyRepository
from dataraum.entropy.models import EntropyObject

__all__ = [
    # Core types (re-exported from models)
    "EntropyObject",
    # Repository
    "EntropyRepository",
]
