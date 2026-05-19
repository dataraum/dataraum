"""Pipeline status utilities.

Query and display pipeline execution status.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseStatus
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.pipeline.registry import get_phase_class
from dataraum.storage.base import Base


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


def get_pipeline_status(session: Session, source_id: str) -> PipelineStatus:
    """Get the current pipeline status for a source.

    Args:
        session: SQLAlchemy session
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
    run_result = session.execute(latest_run_stmt)
    latest_run = run_result.scalar_one_or_none()

    # Get all phase logs for this source (latest per phase)
    logs_stmt = (
        select(PhaseLog)
        .where(PhaseLog.source_id == source_id)
        .order_by(PhaseLog.completed_at.desc())
    )
    log_result = session.execute(logs_stmt)
    logs: list[PhaseLog] = list(log_result.scalars().all())

    # Build log lookup (latest per phase)
    log_by_phase: dict[str, PhaseLog] = {}
    for log in logs:
        if log.phase_name not in log_by_phase:
            log_by_phase[log.phase_name] = log

    # Build phase status list from YAML declarations
    declarations = load_phase_declarations()
    phases: list[PhaseStatusInfo] = []
    for name, decl in declarations.items():
        phase_log = log_by_phase.get(name)
        if phase_log is not None:
            status = PhaseStatus(phase_log.status)
            phases.append(
                PhaseStatusInfo(
                    name=name,
                    description=decl.description,
                    status=status,
                    duration_seconds=phase_log.duration_seconds,
                    completed_at=phase_log.completed_at,
                    error=phase_log.error,
                )
            )
        else:
            phases.append(
                PhaseStatusInfo(
                    name=name,
                    description=decl.description,
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


def get_phase_tables(phase_name: str) -> list[type[Base]]:
    """Get all SQLAlchemy model classes owned by a phase.

    Introspects the phase's db_models modules for Base subclasses.

    Args:
        phase_name: Name of the phase.

    Returns:
        List of SQLAlchemy model classes.
    """
    cls = get_phase_class(phase_name)
    if not cls:
        return []

    instance = cls()
    tables: list[type[Base]] = []
    for module in instance.db_models:
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, Base) and attr is not Base:
                tables.append(attr)
    return tables


def _sort_by_fk_depth(models: list[type[Base]]) -> list[type[Base]]:
    """Sort models so child tables (with FKs to others in the list) come first.

    This ensures we delete from leaf tables before parent tables to
    avoid FK constraint violations.
    """
    table_names = {m.__tablename__ for m in models}

    def fk_depth(model: type[Base]) -> int:
        depth = 0
        for col in model.__table__.columns:
            for fk in col.foreign_keys:
                if fk.column.table.name in table_names:
                    depth += 1
        return -depth  # Negative so more FKs = sorted first

    return sorted(models, key=fk_depth)


def reset_phase(session: Session, source_id: str, phase_name: str) -> int:
    """Reset a specific phase for a source.

    Deletes phase data (bulk DELETE filtered by source_id) and
    removes the phase checkpoint so the pipeline re-runs it.

    Args:
        session: SQLAlchemy session
        source_id: Source identifier
        phase_name: Phase to reset

    Returns:
        Number of rows deleted.
    """
    deleted = 0
    tables = get_phase_tables(phase_name)
    sorted_tables = _sort_by_fk_depth(tables)

    for model in sorted_tables:
        if hasattr(model, "source_id"):
            stmt = delete(model).where(model.source_id == source_id)
        elif hasattr(model, "column_id"):
            from dataraum.storage.models import Column, Table

            subq = (
                select(Column.column_id)
                .join(Table, Column.table_id == Table.table_id)
                .where(Table.source_id == source_id)
            )
            stmt = delete(model).where(model.column_id.in_(subq))
        elif hasattr(model, "table_id"):
            from dataraum.storage.models import Table

            subq = select(Table.table_id).where(Table.source_id == source_id)
            stmt = delete(model).where(model.table_id.in_(subq))
        else:
            continue

        result = session.execute(stmt)
        deleted += result.rowcount  # type: ignore[attr-defined]

    # Delete phase logs
    log_stmt = delete(PhaseLog).where(
        PhaseLog.source_id == source_id,
        PhaseLog.phase_name == phase_name,
    )
    log_result = session.execute(log_stmt)
    deleted += log_result.rowcount  # type: ignore[attr-defined]

    return int(deleted)
