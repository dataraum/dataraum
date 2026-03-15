"""Tests for the fixes ledger."""

from sqlalchemy.orm import Session

from dataraum.documentation.ledger import (
    get_active_fixes,
    log_fix,
)
from dataraum.storage.models import Source


def _create_source(session: Session, source_id: str = "src-1") -> str:
    source = Source(
        source_id=source_id,
        name="test",
        source_type="csv",
    )
    session.add(source)
    session.flush()
    return source_id


class TestLogFix:
    def test_creates_entry(self, session: Session) -> None:
        source_id = _create_source(session)

        entry = log_fix(
            session=session,
            source_id=source_id,
            action_name="document_unit",
            table_name="transactions",
            column_name="amount",
            user_input="The amount is always in EUR",
            interpretation="Column transactions.amount uses EUR as fixed currency unit.",
        )

        assert entry.fix_id is not None
        assert entry.source_id == source_id
        assert entry.action_name == "document_unit"
        assert entry.table_name == "transactions"
        assert entry.column_name == "amount"
        assert entry.status == "confirmed"
        assert entry.superseded_by is None

    def test_supersedes_previous(self, session: Session) -> None:
        source_id = _create_source(session)

        old = log_fix(
            session=session,
            source_id=source_id,
            action_name="document_unit",
            table_name="transactions",
            column_name="amount",
            user_input="Amount is in USD",
            interpretation="USD currency.",
        )
        old_id = old.fix_id

        new = log_fix(
            session=session,
            source_id=source_id,
            action_name="document_unit",
            table_name="transactions",
            column_name="amount",
            user_input="Actually it's EUR",
            interpretation="EUR currency.",
        )

        session.flush()
        # Refresh old entry
        session.refresh(old)
        assert old.status == "superseded"
        assert old.superseded_by == new.fix_id
        assert old.superseded_at is not None
        assert new.status == "confirmed"
        assert new.fix_id != old_id

    def test_different_columns_not_superseded(self, session: Session) -> None:
        source_id = _create_source(session)

        fix_a = log_fix(
            session=session,
            source_id=source_id,
            action_name="document_unit",
            table_name="transactions",
            column_name="amount",
            user_input="EUR",
            interpretation="EUR",
        )

        fix_b = log_fix(
            session=session,
            source_id=source_id,
            action_name="document_unit",
            table_name="transactions",
            column_name="price",
            user_input="USD",
            interpretation="USD",
        )

        session.refresh(fix_a)
        assert fix_a.status == "confirmed"
        assert fix_b.status == "confirmed"

    def test_table_level_fix(self, session: Session) -> None:
        source_id = _create_source(session)

        entry = log_fix(
            session=session,
            source_id=source_id,
            action_name="document_grain",
            table_name="transactions",
            column_name=None,
            user_input="One row per transaction",
            interpretation="Table grain is one row per transaction.",
        )

        assert entry.column_name is None
        assert entry.status == "confirmed"


class TestGetActiveFixes:
    def test_excludes_superseded(self, session: Session) -> None:
        source_id = _create_source(session)

        log_fix(
            session=session,
            source_id=source_id,
            action_name="document_unit",
            table_name="t",
            column_name="c",
            user_input="old",
            interpretation="old",
        )
        log_fix(
            session=session,
            source_id=source_id,
            action_name="document_unit",
            table_name="t",
            column_name="c",
            user_input="new",
            interpretation="new",
        )

        active = get_active_fixes(session, source_id)
        assert len(active) == 1
        assert active[0].user_input == "new"

    def test_empty_source(self, session: Session) -> None:
        source_id = _create_source(session)
        active = get_active_fixes(session, source_id)
        assert active == []
