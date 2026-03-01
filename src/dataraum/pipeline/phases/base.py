"""Base phase implementation.

Provides common functionality for all pipeline phases.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from types import ModuleType

from dataraum.pipeline.base import PhaseContext, PhaseResult


class BasePhase(ABC):
    """Base class for pipeline phases.

    Provides common functionality and enforces the Phase protocol.
    Subclasses must implement:
    - name property
    - description property
    - dependencies property
    - outputs property
    - _run method
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this phase."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description."""
        ...

    @property
    @abstractmethod
    def dependencies(self) -> list[str]:
        """List of phase names that must complete before this phase."""
        ...

    @property
    @abstractmethod
    def outputs(self) -> list[str]:
        """List of output keys this phase produces."""
        ...

    @property
    def db_models(self) -> list[ModuleType]:
        """Modules containing SQLAlchemy models owned by this phase.

        Default: empty. Override to declare ownership.
        Lazy imports inside the property avoid circular imports at decoration time.
        """
        return []

    @property
    def entropy_preconditions(self) -> dict[str, float]:
        """Hard entropy dimensions that must be below thresholds before this phase runs.

        Returns a dict mapping sub_dimension names to maximum allowed scores.
        E.g., {"type_fidelity": 0.5} means type_fidelity entropy must be <= 0.5.
        Default: empty (no preconditions).
        """
        return {}

    @property
    def post_verification(self) -> list[str]:
        """Hard detector sub_dimensions to re-measure after this phase completes.

        Used to verify that this phase improved (or at least didn't worsen)
        the specified entropy dimensions.
        Default: empty (no post-verification).
        """
        return []

    def run(self, ctx: PhaseContext) -> PhaseResult:
        """Execute the phase.

        Wraps _run with common error handling.
        """
        try:
            return self._run(ctx)
        except Exception as e:
            return PhaseResult.failed(str(e))

    @abstractmethod
    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Execute the phase logic.

        Subclasses implement this method.
        """
        ...

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Check if this phase should be skipped.

        Default implementation: never skip.
        Override in subclasses to implement skip logic.

        Returns:
            None if phase should run, or a reason string if it should be skipped.
        """
        return None
