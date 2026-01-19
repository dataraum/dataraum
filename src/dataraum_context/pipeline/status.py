"""Pipeline status utilities.

Query and display pipeline execution status.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.pipeline.base import PIPELINE_DAG, PhaseStatus
from dataraum_context.pipeline.db_models import PhaseCheckpoint, PipelineRun


@dataclass
class PhaseStatusInfo:
    """Status information for a single phase."""

    name: str
    description: str
    status: PhaseStatus
    duration_seconds: float | None = None
    completed_at: datetime | None = None
    error: str | None = None
    records_processed: int = 0
    records_created: int = 0


@dataclass
class PipelineStatus:
    """Overall pipeline status."""

    source_id: str
    last_run_id: str | None
    last_run_status: str | None
    last_run_at: datetime | None
    phases: list[PhaseStatusInfo]

    @property
    def completed_count(self) -> int:
        """Number of completed phases."""
        return sum(1 for p in self.phases if p.status == PhaseStatus.COMPLETED)

    @property
    def total_count(self) -> int:
        """Total number of phases."""
        return len(self.phases)

    @property
    def progress_percent(self) -> float:
        """Completion percentage."""
        if self.total_count == 0:
            return 0.0
        return (self.completed_count / self.total_count) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "source_id": self.source_id,
            "last_run_id": self.last_run_id,
            "last_run_status": self.last_run_status,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "completed": self.completed_count,
            "total": self.total_count,
            "progress_percent": round(self.progress_percent, 1),
            "phases": [
                {
                    "name": p.name,
                    "description": p.description,
                    "status": p.status.value,
                    "duration_seconds": p.duration_seconds,
                    "completed_at": p.completed_at.isoformat() if p.completed_at else None,
                    "error": p.error,
                    "records_processed": p.records_processed,
                    "records_created": p.records_created,
                }
                for p in self.phases
            ],
        }


async def get_pipeline_status(session: AsyncSession, source_id: str) -> PipelineStatus:
    """Get the current pipeline status for a source.

    Args:
        session: SQLAlchemy async session
        source_id: Source identifier

    Returns:
        PipelineStatus with phase-by-phase breakdown
    """
    # Get latest run
    latest_run_stmt = (
        select(PipelineRun)
        .where(PipelineRun.source_id == source_id)
        .order_by(PipelineRun.started_at.desc())
        .limit(1)
    )
    run_result = await session.execute(latest_run_stmt)
    latest_run = run_result.scalar_one_or_none()

    # Get all checkpoints for this source
    checkpoints_stmt = select(PhaseCheckpoint).where(PhaseCheckpoint.source_id == source_id)
    checkpoint_result = await session.execute(checkpoints_stmt)
    checkpoints: list[PhaseCheckpoint] = list(checkpoint_result.scalars().all())

    # Build checkpoint lookup (latest per phase)
    checkpoint_by_phase: dict[str, PhaseCheckpoint] = {}
    for checkpoint in checkpoints:
        existing = checkpoint_by_phase.get(checkpoint.phase_name)
        if existing is None or (
            existing.completed_at is not None
            and checkpoint.completed_at is not None
            and checkpoint.completed_at > existing.completed_at
        ):
            checkpoint_by_phase[checkpoint.phase_name] = checkpoint

    # Build phase status list
    phases: list[PhaseStatusInfo] = []
    for phase_def in PIPELINE_DAG:
        phase_checkpoint = checkpoint_by_phase.get(phase_def.name)
        if phase_checkpoint is not None:
            status = PhaseStatus(phase_checkpoint.status)
            phases.append(
                PhaseStatusInfo(
                    name=phase_def.name,
                    description=phase_def.description,
                    status=status,
                    duration_seconds=phase_checkpoint.duration_seconds,
                    completed_at=phase_checkpoint.completed_at,
                    error=phase_checkpoint.error,
                    records_processed=phase_checkpoint.records_processed,
                    records_created=phase_checkpoint.records_created,
                )
            )
        else:
            phases.append(
                PhaseStatusInfo(
                    name=phase_def.name,
                    description=phase_def.description,
                    status=PhaseStatus.PENDING,
                )
            )

    return PipelineStatus(
        source_id=source_id,
        last_run_id=latest_run.run_id if latest_run else None,
        last_run_status=latest_run.status if latest_run else None,
        last_run_at=latest_run.started_at if latest_run else None,
        phases=phases,
    )


async def reset_pipeline(session: AsyncSession, source_id: str) -> int:
    """Reset all checkpoints for a source.

    Args:
        session: SQLAlchemy async session
        source_id: Source identifier

    Returns:
        Number of checkpoints deleted
    """
    # Count checkpoints
    count_stmt = select(func.count()).where(PhaseCheckpoint.source_id == source_id)
    result = await session.execute(count_stmt)
    count = result.scalar() or 0

    # Delete all runs and checkpoints (cascade)
    runs_stmt = select(PipelineRun).where(PipelineRun.source_id == source_id)
    result = await session.execute(runs_stmt)
    runs = result.scalars().all()

    for run in runs:
        await session.delete(run)

    return count
