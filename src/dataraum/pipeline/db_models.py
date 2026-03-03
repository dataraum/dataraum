"""Pipeline database models.

SQLAlchemy models for tracking pipeline runs and phase checkpoints.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage.base import Base


class PipelineRun(Base):
    """A single execution of the pipeline.

    Tracks the overall pipeline run with its configuration and status.
    """

    __tablename__ = "pipeline_runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # Run configuration
    target_phase: Mapped[str | None] = mapped_column(String)  # None = run all
    config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    # Status
    status: Mapped[str] = mapped_column(String, nullable=False, default="running")
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Phase counts
    phases_completed: Mapped[int] = mapped_column(Integer, default=0)
    phases_failed: Mapped[int] = mapped_column(Integer, default=0)
    phases_skipped: Mapped[int] = mapped_column(Integer, default=0)

    # Timing metrics
    total_duration_seconds: Mapped[float] = mapped_column(Float, default=0.0)

    # Aggregate data metrics
    total_tables_processed: Mapped[int] = mapped_column(Integer, default=0)
    total_rows_processed: Mapped[int] = mapped_column(Integer, default=0)

    # Error info
    error: Mapped[str | None] = mapped_column(String)

    # Gate configuration
    contract_name: Mapped[str | None] = mapped_column(String)
    gate_mode: Mapped[str | None] = mapped_column(String)  # skip, pause, fail
    final_entropy_state: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Relationships
    checkpoints: Mapped[list[PhaseCheckpoint]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class PhaseCheckpoint(Base):
    """Checkpoint for a completed pipeline phase.

    Stores the result of each phase execution, enabling resume.
    """

    __tablename__ = "phase_checkpoints"

    checkpoint_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    run_id: Mapped[str] = mapped_column(
        ForeignKey("pipeline_runs.run_id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    phase_name: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # Execution status
    status: Mapped[str] = mapped_column(String, nullable=False)  # completed, failed, skipped
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)

    # Outputs (for passing to dependent phases)
    outputs: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    # Input hash (for invalidation detection)
    input_hash: Mapped[str | None] = mapped_column(String)

    # Basic metrics
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    records_created: Mapped[int] = mapped_column(Integer, default=0)

    # Detailed metrics (from PhaseMetrics)
    tables_processed: Mapped[int] = mapped_column(Integer, default=0)
    columns_processed: Mapped[int] = mapped_column(Integer, default=0)
    rows_processed: Mapped[int] = mapped_column(Integer, default=0)
    db_queries: Mapped[int] = mapped_column(Integer, default=0)
    db_writes: Mapped[int] = mapped_column(Integer, default=0)

    # Sub-operation timings (JSON dict of operation_name -> seconds)
    timings: Mapped[dict[str, float]] = mapped_column(JSON, default=dict)

    # Error/warning info
    error: Mapped[str | None] = mapped_column(String)
    warnings: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Gate state
    entropy_hard_scores: Mapped[dict[str, float] | None] = mapped_column(JSON)
    gate_status: Mapped[str | None] = mapped_column(String)  # passed, blocked, skipped
    gate_reason: Mapped[str | None] = mapped_column(String)

    # Relationship
    run: Mapped[PipelineRun] = relationship(back_populates="checkpoints")


# Composite index for efficient checkpoint lookups during pipeline resume
Index("idx_checkpoint_run_phase", PhaseCheckpoint.run_id, PhaseCheckpoint.phase_name)
# Composite index for source-based checkpoint queries
Index("idx_checkpoint_source_phase", PhaseCheckpoint.source_id, PhaseCheckpoint.phase_name)


class PhaseLog(Base):
    """Append-only observability log for phase executions.

    Unlike PhaseCheckpoint (which tracks resume state), PhaseLog provides
    a historical record of every phase execution for observability.
    """

    __tablename__ = "phase_logs"

    log_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    run_id: Mapped[str] = mapped_column(
        ForeignKey("pipeline_runs.run_id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    phase_name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)  # completed | failed | skipped
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    error: Mapped[str | None] = mapped_column(String)
    entropy_scores: Mapped[dict[str, float] | None] = mapped_column(JSON)


class Fix(Base):
    """Persistent, replayable fix record.

    Fixes are applied after specific phases complete. They are replayed
    on subsequent pipeline runs to maintain data corrections.
    """

    __tablename__ = "fixes"

    fix_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    source_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    action_type: Mapped[str] = mapped_column(String, nullable=False)
    target: Mapped[str] = mapped_column(String, nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    after_phase: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(UTC)
    )
    last_applied_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_applied_run_id: Mapped[str | None] = mapped_column(String)
