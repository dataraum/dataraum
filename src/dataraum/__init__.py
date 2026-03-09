"""DataRaum Context Engine.

A rich metadata context engine for AI-driven data analytics.

Example:
    from dataraum import Context

    ctx = Context("./pipeline_output")
    ctx.tables                                   # List tables
    ctx.entropy.summary()                        # Entropy scores
    ctx.contracts.evaluate("aggregation_safe")   # Contract compliance
    ctx.actions()                                # Resolution actions
    ctx.sources.list()                           # Registered sources
    result = ctx.query("What is the total revenue?")
"""

__version__ = "0.1.0"

from dataraum.context import Context
from dataraum.core.models.base import Result

__all__ = [
    "Context",
    "Result",
    "__version__",
]
