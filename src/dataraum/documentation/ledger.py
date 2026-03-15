"""Fixes ledger — durable store of user-confirmed domain knowledge.

Provides functions to log fixes, query active fixes, and format them
for injection into LLM prompts (e.g., semantic analysis re-runs).
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.documentation.db_models import FixLedgerEntry


def log_fix(
    session: Session,
    source_id: str,
    action_name: str,
    table_name: str,
    column_name: str | None,
    user_input: str,
    interpretation: str,
    status: str = "confirmed",
) -> FixLedgerEntry:
    """Create a fix entry, superseding any prior fix with the same status for same action+scope.

    Args:
        session: Database session
        source_id: Source ID the fix belongs to
        action_name: Action name being resolved (e.g. "document_unit")
        table_name: Table name (denormalized)
        column_name: Column name, or None for table-level actions
        user_input: Raw user text
        interpretation: Agent's structured interpretation
        status: Fix status — "confirmed" or "rejected"

    Returns:
        The newly created FixLedgerEntry
    """
    # Find existing fix with same status for same action+scope
    stmt = select(FixLedgerEntry).where(
        FixLedgerEntry.source_id == source_id,
        FixLedgerEntry.action_name == action_name,
        FixLedgerEntry.table_name == table_name,
        FixLedgerEntry.status == status,
    )
    if column_name is not None:
        stmt = stmt.where(FixLedgerEntry.column_name == column_name)
    else:
        stmt = stmt.where(FixLedgerEntry.column_name.is_(None))

    existing = session.execute(stmt).scalars().all()

    # Create new entry
    new_entry = FixLedgerEntry(
        source_id=source_id,
        action_name=action_name,
        table_name=table_name,
        column_name=column_name,
        user_input=user_input,
        interpretation=interpretation,
        status=status,
    )
    session.add(new_entry)
    session.flush()  # Get fix_id assigned

    # Supersede old entries
    now = datetime.now(UTC)
    for old in existing:
        old.status = "superseded"
        old.superseded_at = now
        old.superseded_by = new_entry.fix_id

    return new_entry


def get_active_fixes(session: Session, source_id: str) -> list[FixLedgerEntry]:
    """Get all active (confirmed or rejected, non-superseded) fixes for a source.

    Args:
        session: Database session
        source_id: Source ID to query

    Returns:
        List of active FixLedgerEntry records
    """
    stmt = (
        select(FixLedgerEntry)
        .where(
            FixLedgerEntry.source_id == source_id,
            FixLedgerEntry.status.in_(["confirmed", "rejected"]),
        )
        .order_by(FixLedgerEntry.created_at)
    )
    return list(session.execute(stmt).scalars().all())
