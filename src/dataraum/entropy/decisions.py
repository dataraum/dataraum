"""Decision ledger for entropy-gated pipeline.

Records what actions were taken at gates, with before/after entropy scores
for auditability and reproducibility.

Contains:
- Decision: Immutable record of a gate action (dataclass)
- DecisionRecord: SQLAlchemy model for persistence
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


@dataclass(frozen=True)
class Decision:
    """Immutable record of an action taken at a pipeline gate.

    Captures the full context: what was wrong, what was done,
    and the measured effect on entropy scores.
    """

    decision_id: str = field(default_factory=lambda: str(uuid4()))

    # What gate triggered this
    gate_type: str = ""  # "structural", "semantic", "value", "contract"
    blocked_phase: str = ""  # Phase that was blocked

    # What action was taken
    action_type: str = ""  # e.g., "override_type", "declare_unit"
    target: str = ""  # e.g., "column:orders.amount"
    parameters: dict[str, Any] = field(default_factory=dict)

    # Who/what decided
    actor: str = ""  # "user", "auto_fix", "mcp_agent"

    # Before/after entropy scores for the affected dimension
    before_scores: dict[str, float] = field(default_factory=dict)
    after_scores: dict[str, float] = field(default_factory=dict)

    # Content hash of the source data at decision time
    source_hash: str = ""

    # Outcome
    improved: bool = False
    evidence_summary: str = ""

    # Timing
    decided_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class DecisionRecord(Base):
    """Persisted decision record.

    Stores gate decisions in the database for audit trail
    and reproducibility.
    """

    __tablename__ = "decisions"

    decision_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Context
    run_id: Mapped[str | None] = mapped_column(String, index=True)
    source_id: Mapped[str | None] = mapped_column(String, index=True)
    gate_type: Mapped[str] = mapped_column(String, nullable=False)
    blocked_phase: Mapped[str] = mapped_column(String, nullable=False)

    # Action
    action_type: Mapped[str] = mapped_column(String, nullable=False)
    target: Mapped[str] = mapped_column(String, nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    # Actor
    actor: Mapped[str] = mapped_column(String, nullable=False, default="user")

    # Entropy delta
    before_scores: Mapped[dict[str, float]] = mapped_column(JSON, default=dict)
    after_scores: Mapped[dict[str, float]] = mapped_column(JSON, default=dict)
    improved: Mapped[bool] = mapped_column(Integer, nullable=False, default=False)

    # Evidence
    source_hash: Mapped[str] = mapped_column(String, default="")
    evidence_summary: Mapped[str] = mapped_column(String, default="")

    # Timing
    decided_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Sequence within a gate (for ordering multiple fixes)
    sequence: Mapped[int] = mapped_column(Integer, default=0)


# Indexes
Index("idx_decision_run", DecisionRecord.run_id)
Index("idx_decision_source", DecisionRecord.source_id)
Index("idx_decision_gate_type", DecisionRecord.gate_type)
Index("idx_decision_target", DecisionRecord.target)
Index("idx_decision_time", DecisionRecord.decided_at)
