"""Base phase implementation.

Provides common functionality for all pipeline phases.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from dataraum_context.pipeline.base import PhaseContext, PhaseResult


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
