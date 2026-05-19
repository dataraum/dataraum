"""Tests for the entropy engine library."""

from __future__ import annotations

from unittest.mock import MagicMock

from dataraum.entropy.engine import (
    _extract_column_id,
    _resolve_table_id_from_target,
    persist_records,
)
from dataraum.entropy.models import EntropyObject

# ---------------------------------------------------------------------------
# _resolve_table_id_from_target
# ---------------------------------------------------------------------------


class TestResolveTableId:
    def test_column_target(self) -> None:
        lookup = {"orders": "tbl_001", "customers": "tbl_002"}
        assert (
            _resolve_table_id_from_target("column:orders.amount", lookup, "fallback") == "tbl_001"
        )

    def test_table_target(self) -> None:
        lookup = {"orders": "tbl_001"}
        assert _resolve_table_id_from_target("table:orders", lookup, "fallback") == "tbl_001"

    def test_unknown_table_uses_fallback(self) -> None:
        lookup = {"orders": "tbl_001"}
        assert _resolve_table_id_from_target("column:unknown.col", lookup, "fallback") == "fallback"

    def test_no_colon_uses_fallback(self) -> None:
        assert _resolve_table_id_from_target("bare_target", {}, "fallback") == "fallback"


# ---------------------------------------------------------------------------
# _extract_column_id
# ---------------------------------------------------------------------------


class TestExtractColumnId:
    def test_extracts_from_evidence(self) -> None:
        obj = EntropyObject(
            target="column:orders.amount",
            evidence=[{"column_id": "col_001", "table_id": "tbl_001"}],
        )
        assert _extract_column_id(obj) == "col_001"

    def test_returns_none_without_evidence(self) -> None:
        obj = EntropyObject(target="table:orders", evidence=[{"some_key": "val"}])
        assert _extract_column_id(obj) is None

    def test_returns_none_for_empty_evidence(self) -> None:
        obj = EntropyObject(target="table:orders", evidence=[])
        assert _extract_column_id(obj) is None


# ---------------------------------------------------------------------------
# persist_records
# ---------------------------------------------------------------------------


class TestPersistRecords:
    def test_adds_to_session(self) -> None:
        session = MagicMock()
        records = [MagicMock(), MagicMock()]
        persist_records(session, records)
        session.add_all.assert_called_once_with(records)

    def test_empty_records_no_op(self) -> None:
        session = MagicMock()
        persist_records(session, [])
        session.add_all.assert_not_called()
