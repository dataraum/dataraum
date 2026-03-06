"""Fixes Ledger Database Models.

SQLAlchemy model for user-confirmed domain knowledge that survives --force re-runs.
The fix_ledger table is NOT phase-owned and NOT in _CLEANUP_MAP.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class FixLedgerEntry(Base):
    """Persisted user-confirmed domain knowledge.

    Each entry records a user's answer to a document_* action,
    interpreted by the document agent into structured domain knowledge.
    Entries are scoped by table_name/column_name (denormalized) so they
    survive --force re-runs that recreate typed-layer column IDs.
    """

    __tablename__ = "fix_ledger"

    fix_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    source_id: Mapped[str] = mapped_column(
        ForeignKey("sources.source_id"), nullable=False
    )

    # What action this resolves
    action_name: Mapped[str] = mapped_column(String, nullable=False)

    # Scope (denormalized — survives table/column ID changes on --force)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    column_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Domain knowledge
    user_input: Mapped[str] = mapped_column(String, nullable=False)
    interpretation: Mapped[str] = mapped_column(String, nullable=False)

    # Lifecycle
    status: Mapped[str] = mapped_column(String, nullable=False, default="confirmed")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    superseded_by: Mapped[str | None] = mapped_column(
        ForeignKey("fix_ledger.fix_id"), nullable=True
    )


Index("idx_fix_ledger_source", FixLedgerEntry.source_id)
Index(
    "idx_fix_ledger_scope",
    FixLedgerEntry.source_id,
    FixLedgerEntry.action_name,
    FixLedgerEntry.table_name,
    FixLedgerEntry.column_name,
)
