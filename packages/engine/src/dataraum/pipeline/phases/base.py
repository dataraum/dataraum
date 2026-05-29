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
        """Clear this phase's OWN outputs so a replay re-runs it cleanly (DAT-343/373).

        Invoked by the worker (``replay_cleanup_for_phase`` activity) **before**
        ``run`` for EVERY phase that re-executes under a replay — not just the
        ``from_phase``. The purpose is to clear whatever would make this phase's
        ``should_skip`` return an "already done" reason, so the re-run rebuilds
        its outputs against the (possibly re-typed) data.

        Ownership contract (DAT-373 — read before adding/changing an override):

        - **Delete ONLY your own per-Column / per-Table rows**, scoped to
          ``table_ids`` (or source-wide on the empty-list shape). The
          per-phase pattern is "delete-own-rows-by-column_id for the columns of
          the typed tables in scope".
        - **NEVER delete a parent ``Table`` you do not exclusively own.** Typing
          reuses the typed ``Table`` + ``Column`` rows in place (stable identity)
          precisely so other stages' per-Column findings stay attached. The
          Table-delete cascade (``Table`` → ``Column`` → every per-Column row)
          is reserved for ``import`` / source teardown, where dropping the whole
          source IS the intent — never for a phase-local re-run.
        - Cross-stage data (e.g. ``begin_session`` / frame-ground findings on a
          typed column) belongs to OTHER stages and MUST survive your cleanup.

        Default: no-op (a phase with no replay-relevant outputs, e.g. a pure
        DuckDB-view builder whose ``_run`` is ``CREATE OR REPLACE``-idempotent).

        Args:
            ctx: phase context (session + DuckDB cursor + source_id).
            table_ids: replay scope. Empty list = source-wide cleanup
                (matches the source-level reduce shape); a single-element list
                scopes to one table id — a raw table id for ``typing`` /
                ``import``, a typed table id for the table-local analytics phases.
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
