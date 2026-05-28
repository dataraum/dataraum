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

    def replay_cleanup(self, ctx: PhaseContext, table_ids: list[str]) -> None:
        """Drop this phase's outputs so a replay from here starts fresh (DAT-343).

        Invoked by the worker activity wrapper **before** ``run`` when the
        workflow's ``replay.from_phase`` equals this phase's name. The
        purpose is to clear whatever would make ``should_skip`` return a
        "already done" reason — typically the phase's own DB rows plus any
        DuckDB artifacts it owns.

        Default: no-op. Phases that ARE replay entry points (today:
        ``import``, ``typing``, ``semantic_per_column``) override; everything
        downstream of a from_phase rides on cascade-delete through
        ``Column.cascade='all, delete-orphan'`` from the cleaned-up rows.

        Args:
            ctx: phase context (session + DuckDB cursor + source_id).
            table_ids: replay scope. Empty list = source-wide cleanup
                (matches the source-level reduce shape); a single-element
                list scopes to that raw table id (table-local replays).
        """
        return None

    @property
    def db_models(self) -> list[ModuleType]:
        """Modules containing SQLAlchemy models owned by this phase.

        Default: empty. Override to declare ownership.
        Lazy imports inside the property avoid circular imports at decoration time.
        """
        return []

    def _typed_tables(self, ctx: PhaseContext) -> list[Table]:
        """The typed tables this phase should process, scoped to ``ctx.table_ids``.

        The table-local phases run per-table under the DAT-370 fan-out:
        ``ctx.table_ids`` carries the single typed table the child workflow is
        processing, so the phase analyzes exactly that table. An empty
        ``ctx.table_ids`` means source-wide (direct/test invocation).
        """
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        if ctx.table_ids:
            stmt = stmt.where(Table.table_id.in_(ctx.table_ids))
        return list(ctx.session.execute(stmt).scalars().all())

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
