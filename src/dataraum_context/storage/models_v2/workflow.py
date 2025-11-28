"""Workflow Models.

SQLAlchemy models for dataflow execution management:
- Checkpoints for resume capability
- Review queue for human-in-loop approval
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    pass  # Import from core if needed later


class Checkpoint(Base):
    """Dataflow execution checkpoints for resume capability.

    Supports pausing dataflow execution (e.g., waiting for human review)
    and resuming from the saved state.
    """

    __tablename__ = "checkpoints"

    checkpoint_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    dataflow_name: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)

    # Status
    status: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'pending', 'in_review', 'approved', 'rejected', 'completed'
    checkpoint_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'type_resolution', 'quarantine_review', 'quality_review'

    # Timing
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    resumed_at: Mapped[datetime | None] = mapped_column(DateTime)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)

    # State for resume (Hamilton inputs/outputs)
    checkpoint_state: Mapped[dict | None] = mapped_column(
        JSON
    )  # Serialized Hamilton execution state

    # Results
    result_summary: Mapped[dict | None] = mapped_column(JSON)
    error_message: Mapped[str | None] = mapped_column(Text)

    # Relationships
    review_items: Mapped[list["ReviewQueue"]] = relationship(
        back_populates="checkpoint", cascade="all, delete-orphan"
    )


Index("idx_checkpoints_status", Checkpoint.status)


class ReviewQueue(Base):
    """Human review queue.

    Items that require human review before dataflow can proceed.
    """

    __tablename__ = "review_queue"

    review_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    checkpoint_id: Mapped[str] = mapped_column(
        ForeignKey("checkpoints.checkpoint_id"), nullable=False
    )
    review_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'type_decision', 'quarantine_value', 'quality_rule'
    item_id: Mapped[str] = mapped_column(String, nullable=False)  # ID of the item being reviewed

    # Context
    context_data: Mapped[dict | None] = mapped_column(
        JSON
    )  # Data for review (e.g., failed values, type candidates)
    suggested_action: Mapped[dict | None] = mapped_column(JSON)  # AI-suggested action for reviewer

    # Status
    status: Mapped[str] = mapped_column(
        String, default="pending"
    )  # 'pending', 'approved', 'rejected', 'modified'
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime)
    reviewed_by: Mapped[str | None] = mapped_column(String)
    review_notes: Mapped[str | None] = mapped_column(Text)

    # Relationships
    checkpoint: Mapped["Checkpoint"] = relationship(back_populates="review_items")


Index("idx_review_queue_status", ReviewQueue.status)
