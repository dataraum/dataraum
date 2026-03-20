"""Data fixes phase — replays stored fix documents on pipeline re-run.

Runs after semantic, before quality_review. Loads all DataFix records
for the source, ordered by ordinal, and applies metadata fixes via
MetadataInterpreter. Config fixes are skipped (already on disk).

Metadata fixes are re-applied because column IDs and typed tables
are regenerated on --force re-runs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import ModuleType
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.fixes.interpreters import apply_fix_document
from dataraum.pipeline.fixes.models import DataFix
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


@analysis_phase
class DataFixesPhase(BasePhase):
    """Replay stored fixes on pipeline re-run."""

    @property
    def name(self) -> str:
        return "data_fixes"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.pipeline.fixes import models as fix_models_mod

        return [fix_models_mod]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Load and replay all fixes for this source."""
        if ctx.session is None:
            return PhaseResult.success(summary="No session — skipped")

        # Load all fixes ordered by ordinal
        fixes = list(
            ctx.session.execute(
                select(DataFix).where(DataFix.source_id == ctx.source_id).order_by(DataFix.ordinal)
            )
            .scalars()
            .all()
        )

        if not fixes:
            return PhaseResult.success(summary="No stored fixes")

        applied = 0
        skipped = 0
        failed = 0

        for fix in fixes:
            # Skip config fixes on replay — they're already on disk
            if fix.target == "config":
                skipped += 1
                continue

            doc = fix.to_document()
            try:
                apply_fix_document(
                    doc,
                    session=ctx.session,
                )
                fix.status = "applied"
                fix.applied_at = datetime.now(UTC)
                fix.error_message = None
                applied += 1
            except Exception as e:
                fix.status = "failed"
                fix.error_message = str(e)
                failed += 1
                logger.error(
                    "fix_replay_failed",
                    fix_id=fix.fix_id,
                    action=fix.action,
                    error=str(e),
                )

        ctx.session.flush()

        summary_parts = []
        if applied:
            summary_parts.append(f"{applied} applied")
        if skipped:
            summary_parts.append(f"{skipped} config (on disk)")
        if failed:
            summary_parts.append(f"{failed} failed")

        summary = f"Fixes: {', '.join(summary_parts)}" if summary_parts else "No fixes"
        if failed:
            return PhaseResult.failed(summary)
        return PhaseResult.success(summary=summary)

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        """Reset fix statuses to pending so they replay on next run.

        Does NOT delete fixes — they are durable records.
        Marker fixes (target=metadata, no model) replay as no-ops — the
        DataFix record itself is the fix. Their status is still reset so
        the replay loop processes them and sets applied_at freshly.
        """
        from sqlalchemy import update

        result = session.execute(
            update(DataFix)
            .where(
                DataFix.source_id == source_id,
                DataFix.target != "config",
            )
            .values(status="pending", applied_at=None, error_message=None)
        )
        count: int = result.rowcount  # type: ignore[attr-defined]
        return count
