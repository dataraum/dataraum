"""Base phase implementation.

Provides common functionality for all pipeline phases. Per-phase metadata
(description, detectors) is sourced from pipeline.yaml via
``pipeline_config.load_phase_declarations``, not from the phase class.
"""

from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from types import ModuleType

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.llm.providers.base import ProviderError
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.storage import Table

logger = get_logger(__name__)


class BasePhase(ABC):
    """Base class for pipeline phases.

    Subclasses must implement:
    - name property (for registry matching)
    - _run method (the actual phase logic)

    Per-phase metadata (description, detectors) comes from pipeline.yaml
    via ``pipeline_config.load_phase_declarations``.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this phase."""
        ...

    @property
    def db_models(self) -> list[ModuleType]:
        """Modules containing SQLAlchemy models owned by this phase.

        Default: empty. Override to declare ownership.
        Lazy imports inside the property avoid circular imports at decoration time.
        """
        return []

    def _typed_tables(self, ctx: PhaseContext) -> list[Table]:
        """The typed tables this phase should process — the per-table fan-out unit.

        The table-local phases run per-table under the DAT-370 fan-out:
        ``ctx.table_ids`` carries the single typed table the child workflow is
        processing, so the phase analyzes exactly that table.

        Source-free (DAT-422): past ``import`` a run spans 1–N per-object sources,
        so resolution keys on the ``table_ids`` the fan-out hands in — never
        ``source_id`` (which the source-free children leave ``None``). The
        table-local phases always run under the fan-out, so an empty ``table_ids``
        (no scoped unit) resolves to nothing; the run-wide reduce
        (``semantic_per_column``) overrides this to scope by the session instead.
        """
        if not ctx.table_ids:
            return []
        stmt = select(Table).where(Table.layer == "typed", Table.table_id.in_(ctx.table_ids))
        return list(ctx.session.execute(stmt).scalars().all())

    def run(self, ctx: PhaseContext) -> PhaseResult:
        """Execute the phase.

        Wraps _run with wall-clock timing and error handling.

        A :class:`~dataraum.llm.providers.base.ProviderError` is NOT flattened
        into a FAILED ``PhaseResult`` (DAT-503): retryability rides the
        exception *type* to the worker's durable boundary, so it must propagate
        unchanged. Flattening it would lose the transient/permanent distinction
        and make every LLM 429 a non-retryable phase failure. The enclosing
        ``session_scope`` rolls the phase's partial writes back on the raise.
        """
        start = time.monotonic()
        try:
            result = self._run(ctx)
        except ProviderError:
            # Let the typed provider failure propagate to _outcome_or_raise,
            # which classifies it for Temporal retry. session_scope rolls back.
            raise
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
