"""Base phase implementation.

Provides common functionality for all pipeline phases.
Structural declarations (description, dependencies, produces_analyses)
are sourced from pipeline.yaml via YAMLAwarePhase.
"""

from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from types import ModuleType
from typing import TYPE_CHECKING

from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


class BasePhase(ABC):
    """Base class for pipeline phases.

    Subclasses must implement:
    - name property (for registry matching)
    - _run method (the actual phase logic)

    Structural metadata (description, dependencies, produces_analyses)
    comes from pipeline.yaml via YAMLAwarePhase wrapper.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this phase."""
        ...

    @property
    def duckdb_layers(self) -> list[str]:
        """DuckDB layers this phase creates (for cleanup).

        Phases that create DuckDB tables/views should override this
        to declare the layers they own, so cleanup can drop them.
        """
        return []

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        """Delete this phase's output records for the given source.

        Override in subclasses to define phase-specific cleanup logic.
        Returns the number of records deleted.
        """
        return 0

    @property
    def db_models(self) -> list[ModuleType]:
        """Modules containing SQLAlchemy models owned by this phase.

        Default: empty. Override to declare ownership.
        Lazy imports inside the property avoid circular imports at decoration time.
        """
        return []

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
