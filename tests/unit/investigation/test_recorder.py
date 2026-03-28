"""Tests for investigation session recorder."""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import InvestigationStep
from dataraum.investigation.recorder import (
    begin_session,
    end_session,
    get_session_trace,
    record_step,
    summarize_result,
)
from dataraum.storage import Source


def _ensure_source(session: Session, source_id: str = "src-1") -> None:
    existing = session.get(Source, source_id)
    if not existing:
        session.add(Source(source_id=source_id, name=source_id, source_type="csv"))
        session.flush()


class TestBeginSession:
    def test_creates_active_session(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="investigate outliers")

        assert inv.session_id is not None
        assert inv.source_id == "src-1"
        assert inv.status == "active"
        assert inv.intent == "investigate outliers"
        assert inv.step_count == 0
        assert inv.started_at is not None
        assert inv.ended_at is None

    def test_with_contract(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(
            session, "src-1", intent="check compliance", contract="aggregation_safe"
        )
        assert inv.contract == "aggregation_safe"


class TestRecordStep:
    def test_records_step_with_ordinal(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        step = record_step(
            session,
            inv.session_id,
            "measure",
            {"include": ["entropy"]},
            result={"entropy_score": 0.42},
            duration_seconds=1.5,
        )

        assert step.ordinal == 0
        assert step.tool_name == "measure"
        assert step.arguments == {"include": ["entropy"]}
        assert step.status == "success"
        assert step.duration_seconds == 1.5
        assert "0.42" in (step.result_summary or "")

        # Session step_count incremented
        assert inv.step_count == 1

    def test_sequential_ordinals(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        s0 = record_step(session, inv.session_id, "look", {})
        s1 = record_step(session, inv.session_id, "measure", {})
        s2 = record_step(session, inv.session_id, "why", {"dimension": "outliers"})

        assert s0.ordinal == 0
        assert s1.ordinal == 1
        assert s2.ordinal == 2
        assert inv.step_count == 3

    def test_error_step(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        step = record_step(
            session,
            inv.session_id,
            "run_sql",
            {"sql": "SELECT * FROM nonexistent"},
            status="error",
            error="Table not found",
        )

        assert step.status == "error"
        assert step.error == "Table not found"

    def test_extracts_target(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        step = record_step(
            session,
            inv.session_id,
            "why",
            {"target": "column:orders.amount", "dimension": "value.outliers"},
        )

        assert step.target == "column:orders.amount"
        assert step.dimension == "value.outliers"

    def test_extracts_table_column_combined(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        step = record_step(session, inv.session_id, "fix", {"table": "orders", "column": "amount"})
        assert step.target == "orders.amount"

    def test_extracts_table_only(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        step = record_step(session, inv.session_id, "look", {"table_name": "orders"})
        assert step.target == "orders"

    def test_no_target_returns_none(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        step = record_step(session, inv.session_id, "measure", {"include": ["entropy"]})
        assert step.target is None

    def test_invalid_session_raises(self, session: Session) -> None:
        with pytest.raises(ValueError, match="No session"):
            record_step(session, "nonexistent-id", "look", {})

    def test_cannot_record_on_ended_session(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")
        end_session(session, inv.session_id, "delivered")

        with pytest.raises(ValueError, match="cannot record steps"):
            record_step(session, inv.session_id, "look", {})


class TestEndSession:
    def test_delivers(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")
        record_step(session, inv.session_id, "measure", {})

        ended = end_session(
            session,
            inv.session_id,
            "delivered",
            summary="Data is aggregation-safe after documenting null semantics.",
        )

        assert ended.status == "delivered"
        assert ended.ended_at is not None
        assert ended.duration_seconds is not None
        assert ended.duration_seconds >= 0
        assert "aggregation-safe" in (ended.outcome_summary or "")

    def test_refuses(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        ended = end_session(
            session,
            inv.session_id,
            "refused",
            summary="Cannot resolve cross-table inconsistency without source data fix.",
            payload={"blocking_dimensions": ["computational.reconciliation"]},
        )

        assert ended.status == "refused"
        assert ended.outcome_payload == {"blocking_dimensions": ["computational.reconciliation"]}

    def test_invalid_session_raises(self, session: Session) -> None:
        with pytest.raises(ValueError, match="No session"):
            end_session(session, "nonexistent-id", "delivered")

    def test_cannot_end_twice(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")
        end_session(session, inv.session_id, "delivered")

        with pytest.raises(ValueError, match="already"):
            end_session(session, inv.session_id, "refused")

    def test_invalid_outcome_raises(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")

        with pytest.raises(ValueError, match="Invalid outcome"):
            end_session(session, inv.session_id, "banana")


class TestGetSessionTrace:
    def test_returns_ordered_steps(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="investigate outliers")
        record_step(session, inv.session_id, "look", {})
        record_step(session, inv.session_id, "measure", {"include": ["entropy"]})
        record_step(session, inv.session_id, "why", {"dimension": "outliers"})
        end_session(session, inv.session_id, "delivered", summary="All clear.")

        trace = get_session_trace(session, inv.session_id)

        assert trace["status"] == "delivered"
        assert trace["intent"] == "investigate outliers"
        assert trace["step_count"] == 3
        assert len(trace["steps"]) == 3
        assert [s["tool_name"] for s in trace["steps"]] == ["look", "measure", "why"]
        assert trace["outcome_summary"] == "All clear."

    def test_invalid_session_raises(self, session: Session) -> None:
        with pytest.raises(ValueError, match="No session"):
            get_session_trace(session, "nonexistent-id")


class TestSummarizeResult:
    def test_small_result_kept_as_is(self) -> None:
        result = {"score": 0.42, "status": "ok"}
        summary = summarize_result("measure", result)
        assert "0.42" in summary

    def test_large_result_truncated(self) -> None:
        result = {"data": "x" * 5000}
        summary = summarize_result("run_sql", result)
        assert len(summary) <= 2001  # 2000 + ellipsis
        assert summary.endswith("…")

    def test_none_result(self) -> None:
        assert summarize_result("look", None) == ""

    def test_string_result(self) -> None:
        summary = summarize_result("why", "Because outliers are 10x above mean")
        assert summary == "Because outliers are 10x above mean"


class TestCascadeDelete:
    def test_deleting_session_deletes_steps(self, session: Session) -> None:
        _ensure_source(session)
        inv = begin_session(session, "src-1", intent="test")
        record_step(session, inv.session_id, "look", {})
        record_step(session, inv.session_id, "measure", {})
        session.commit()

        session.delete(inv)
        session.commit()

        # Steps should be cascade-deleted
        from sqlalchemy import select

        remaining = session.execute(select(InvestigationStep)).scalars().all()
        assert len(remaining) == 0
