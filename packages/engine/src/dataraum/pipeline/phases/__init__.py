"""Pipeline phase implementations.

Phase classes are auto-discovered via the @analysis_phase decorator
and the registry module. Import BasePhase from here for convenience.
"""

from dataraum.pipeline.phases.base import BasePhase

__all__ = ["BasePhase"]
