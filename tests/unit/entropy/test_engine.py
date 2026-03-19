"""Tests for the entropy engine library."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataraum.entropy.engine import (
    _extract_column_id,
    _resolve_table_id_from_target,
    build_network_context,
    compute_dimension_scores,
    create_snapshot,
    persist_records,
    run_detectors,
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
        assert _extract_column_id(obj, {}) == "col_001"

    def test_returns_none_without_evidence(self) -> None:
        obj = EntropyObject(target="table:orders", evidence=[{"some_key": "val"}])
        assert _extract_column_id(obj, {}) is None

    def test_returns_none_for_empty_evidence(self) -> None:
        obj = EntropyObject(target="table:orders", evidence=[])
        assert _extract_column_id(obj, {}) is None


# ---------------------------------------------------------------------------
# compute_dimension_scores
# ---------------------------------------------------------------------------


class TestComputeDimensionScores:
    def test_averages_by_dimension_path(self) -> None:
        objects = [
            EntropyObject(layer="value", dimension="nulls", sub_dimension="null_ratio", score=0.1),
            EntropyObject(layer="value", dimension="nulls", sub_dimension="null_ratio", score=0.3),
            EntropyObject(
                layer="structural", dimension="types", sub_dimension="type_fidelity", score=0.5
            ),
        ]
        scores = compute_dimension_scores(objects)
        assert scores["value.nulls.null_ratio"] == pytest.approx(0.2)
        assert scores["structural.types.type_fidelity"] == pytest.approx(0.5)

    def test_empty_objects(self) -> None:
        assert compute_dimension_scores([]) == {}


# ---------------------------------------------------------------------------
# build_network_context
# ---------------------------------------------------------------------------


class TestBuildNetworkContext:
    def test_empty_objects_returns_empty_context(self) -> None:
        ctx = build_network_context([])
        assert ctx.total_columns == 0
        assert ctx.overall_readiness == "ready"

    def test_with_column_objects(self) -> None:
        objects = [
            EntropyObject(
                layer="value",
                dimension="nulls",
                sub_dimension="null_ratio",
                target="column:orders.amount",
                score=0.8,
                detector_id="null_ratio",
            ),
        ]
        ctx = build_network_context(objects)
        assert ctx.total_columns == 1
        assert "column:orders.amount" in ctx.columns


# ---------------------------------------------------------------------------
# create_snapshot
# ---------------------------------------------------------------------------


class TestCreateSnapshot:
    @patch("dataraum.entropy.engine.EntropySnapshotRecord")
    def test_creates_valid_snapshot(self, mock_record_cls: MagicMock) -> None:
        objects = [
            EntropyObject(target="column:orders.amount", score=0.5),
            EntropyObject(target="column:orders.qty", score=0.3),
        ]
        network_ctx = MagicMock()
        network_ctx.columns_blocked = 1
        network_ctx.columns_investigate = 0
        network_ctx.overall_readiness = "blocked"
        network_ctx.intents = []

        create_snapshot("src_001", objects, network_ctx)

        mock_record_cls.assert_called_once()
        kwargs = mock_record_cls.call_args.kwargs
        assert kwargs["source_id"] == "src_001"
        assert kwargs["total_entropy_objects"] == 2
        assert kwargs["high_entropy_count"] == 1
        assert kwargs["critical_entropy_count"] == 1
        assert kwargs["overall_readiness"] == "blocked"
        assert kwargs["avg_entropy_score"] == pytest.approx(0.4)

    @patch("dataraum.entropy.engine.EntropySnapshotRecord")
    def test_avg_uses_per_target_max(self, mock_record_cls: MagicMock) -> None:
        """Multiple objects for same target: avg uses max score per target."""
        objects = [
            EntropyObject(target="column:orders.amount", score=0.2),
            EntropyObject(target="column:orders.amount", score=0.8),
            EntropyObject(target="column:orders.qty", score=0.4),
        ]
        network_ctx = MagicMock()
        network_ctx.columns_blocked = 0
        network_ctx.columns_investigate = 0
        network_ctx.overall_readiness = "ready"
        network_ctx.intents = []

        create_snapshot("src_001", objects, network_ctx)

        kwargs = mock_record_cls.call_args.kwargs
        # Per-target max: amount=0.8, qty=0.4. Mean = 0.6
        assert kwargs["avg_entropy_score"] == pytest.approx(0.6)


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


# ---------------------------------------------------------------------------
# run_detectors
# ---------------------------------------------------------------------------


class TestRunDetectors:
    @patch("dataraum.entropy.engine._make_record")
    @patch("dataraum.entropy.engine.take_snapshot")
    def test_runs_column_and_table_scoped(self, mock_snap: MagicMock, mock_make: MagicMock) -> None:
        """Verify run_detectors calls take_snapshot for columns and tables."""
        session = MagicMock()

        table = MagicMock()
        table.table_id = "tbl_001"
        table.table_name = "orders"

        col = MagicMock()
        col.table_id = "tbl_001"
        col.column_id = "col_001"
        col.column_name = "amount"

        col_obj = EntropyObject(
            layer="value",
            dimension="nulls",
            sub_dimension="null_ratio",
            target="column:orders.amount",
            score=0.1,
            detector_id="null_ratio",
        )
        table_obj = EntropyObject(
            layer="semantic",
            dimension="dimensional",
            sub_dimension="cross_column_patterns",
            target="table:orders",
            score=0.3,
            detector_id="dimensional_entropy",
        )

        col_snapshot = MagicMock()
        col_snapshot.objects = (col_obj,)

        table_snapshot = MagicMock()
        table_snapshot.objects = (table_obj,)

        mock_snap.side_effect = [col_snapshot, table_snapshot]
        mock_make.side_effect = lambda **kw: MagicMock(target=kw["entropy_obj"].target)

        results = run_detectors(
            session=session,
            source_id="src_001",
            typed_tables=[table],
            columns=[col],
        )

        assert results.tables_processed == 1
        assert len(results.records) == 2
        assert len(results.domain_objects) == 2

        # Verify take_snapshot was called for column and table
        assert mock_snap.call_count == 2
        calls = mock_snap.call_args_list
        assert calls[0].kwargs["target"] == "column:orders.amount"
        assert calls[1].kwargs["target"] == "table:orders"

    @patch("dataraum.entropy.engine.take_snapshot")
    def test_skips_tables_with_no_columns(self, mock_snap: MagicMock) -> None:
        session = MagicMock()

        table = MagicMock()
        table.table_id = "tbl_001"
        table.table_name = "empty_table"

        results = run_detectors(
            session=session,
            source_id="src_001",
            typed_tables=[table],
            columns=[],  # No columns for this table
        )

        assert results.tables_processed == 0
        assert results.records == []
        mock_snap.assert_not_called()
