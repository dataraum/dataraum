"""Fix document models.

FixDocument is the in-memory representation of a teach/fix operation.
DataFix is the ORM model that persists these across pipeline re-runs.

Each document targets exactly one interpreter (config or metadata).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base

# ---------------------------------------------------------------------------
# In-memory models (API surface)
# ---------------------------------------------------------------------------


@dataclass
class FixDocument:
    """A single atomic teach/fix operation targeting one interpreter.

    Used by the teach tool to represent config writes and metadata patches.
    Each document targets exactly one interpreter (config or metadata).

    Args:
        target: Which interpreter handles this: "config" or "metadata".
        action: Teach type name, e.g. "concept", "relationship".
        table_name: Scoping — which table this applies to.
        column_name: Scoping — which column (None for table-scoped).
        dimension: Which entropy dimension this addresses.
        payload: Target-specific data for the interpreter.
        description: Human-readable summary.
        fix_id: Unique identifier (auto-generated).
        ordinal: Execution order (0-indexed).
    """

    target: str  # "config" | "metadata"
    action: str
    table_name: str
    column_name: str | None
    dimension: str
    payload: dict[str, Any]
    description: str = ""
    fix_id: str = field(default_factory=lambda: str(uuid4()))
    ordinal: int = 0

    def __post_init__(self) -> None:
        if self.target not in ("config", "metadata"):
            msg = f"target must be 'config' or 'metadata', got {self.target!r}"
            raise ValueError(msg)


# ---------------------------------------------------------------------------
# ORM model (persistence — survives --force re-runs)
# ---------------------------------------------------------------------------


class DataFix(Base):
    """Persisted fix record.

    Stores fix documents so the data_fixes phase can replay them on
    pipeline re-runs. Scoped by table_name/column_name (not column_id)
    to survive --force re-runs that regenerate typed-layer IDs.
    """

    __tablename__ = "data_fixes"

    fix_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)

    # What this fix does
    action: Mapped[str] = mapped_column(String, nullable=False)
    target: Mapped[str] = mapped_column(String, nullable=False)  # config|metadata
    dimension: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False, default="")

    # Scope (denormalized for --force resilience)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    column_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # The actual fix content — interpreter reads this based on target
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Lifecycle
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    applied_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    @classmethod
    def from_document(cls, source_id: str, doc: FixDocument, *, session_id: str) -> DataFix:
        """Create a DataFix record from an in-memory FixDocument."""
        return cls(
            fix_id=doc.fix_id,
            session_id=session_id,
            source_id=source_id,
            action=doc.action,
            target=doc.target,
            dimension=doc.dimension,
            description=doc.description,
            table_name=doc.table_name,
            column_name=doc.column_name,
            payload=doc.payload,
            ordinal=doc.ordinal,
        )

    def to_document(self) -> FixDocument:
        """Convert back to an in-memory FixDocument."""
        return FixDocument(
            fix_id=self.fix_id,
            target=self.target,
            action=self.action,
            table_name=self.table_name,
            column_name=self.column_name,
            dimension=self.dimension,
            payload=self.payload,
            description=self.description,
            ordinal=self.ordinal,
        )
