"""Base phase implementation.

Provides common functionality for all pipeline phases.
"""

from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from collections.abc import Callable
from types import ModuleType
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult

if TYPE_CHECKING:
    from dataraum.pipeline.fixes import FixInput, FixResult

logger = get_logger(__name__)


class BasePhase(ABC):
    """Base class for pipeline phases.

    Provides common functionality and enforces the Phase protocol.
    Subclasses must implement:
    - name property
    - description property
    - dependencies property
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
    def db_models(self) -> list[ModuleType]:
        """Modules containing SQLAlchemy models owned by this phase.

        Default: empty. Override to declare ownership.
        Lazy imports inside the property avoid circular imports at decoration time.
        """
        return []

    @property
    def post_verification(self) -> list[str]:
        """Detector sub_dimensions to re-measure after this phase completes.

        Used to verify that this phase improved (or at least didn't worsen)
        the specified entropy dimensions.
        Default: empty (no post-verification).
        """
        return []

    @property
    def fix_handlers(self) -> dict[str, Callable[[FixInput, dict[str, Any]], FixResult]]:
        """Map action_name to a handler function that applies the fix.

        Each handler receives a FixInput (structured user decision) and the
        current phase config dict, then returns a FixResult with config patches.
        The handler is a config writer — it applies the user's decision to YAML,
        it does not make decisions itself.

        Default: empty (no fix handlers). Override in subclasses to declare handlers.
        """
        return {}

    def run(self, ctx: PhaseContext) -> PhaseResult:
        """Execute the phase.

        Wraps _run with wall-clock timing and error handling.
        """
        start = time.monotonic()
        try:
            result = self._run(ctx)
        except Exception as e:
            elapsed = time.monotonic() - start
            tb = traceback.format_exc()
            logger.error(
                "phase_failed",
                phase=self.name,
                error=str(e),
                traceback=tb,
            )
            error_msg = f"{type(e).__name__}: {e}"
            return PhaseResult.failed(error_msg, duration=elapsed)
        result.duration_seconds = time.monotonic() - start
        return result

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
