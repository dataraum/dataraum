"""DataRaum Context Engine.

A rich metadata context engine for AI-driven data analytics.
"""

__version__ = "0.1.0"

from dataraum_context.context.models import ContextDocument
from dataraum_context.core.models.base import Result

__all__ = [
    "ContextDocument",
    "Result",
    "__version__",
]
