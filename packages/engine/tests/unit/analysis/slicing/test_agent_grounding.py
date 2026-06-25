"""The slicing agent grounds every recommendation against the real column universe.

A recommendation the LLM emits for a column that is not in THIS run's context — a
hallucination, or a cross-run enriched-view shape change (a fact's dimension join
drops to a passthrough view on a re-run, so its ``fk__dim`` columns vanish) — has no
resolvable ``column_id``. Stored, that empty id is a guaranteed FK violation on
``slice_definitions`` that crashes the whole begin_session (observed on a DAT-473 teach
re-run: ``account_id__account_type`` recommended for a journal_lines passthrough view).
The agent must drop it, not pass an empty FK downstream.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from structlog.testing import capture_logs

from dataraum.analysis.slicing.agent import SlicingAgent
from dataraum.analysis.slicing.models import SliceRecommendationOutput, SlicingAnalysisOutput


def _agent() -> SlicingAgent:
    # _convert_output_to_result is a pure conversion — config/provider/renderer unused.
    return SlicingAgent(MagicMock(), MagicMock(), MagicMock())


def _context() -> dict:
    """One table whose ONLY real column is cost_center (no enriched dim this run)."""
    return {
        "tables": [
            {
                "table_name": "journal_lines",
                "table_id": "tbl_jl",
                "duckdb_path": "journal_lines",
                "enriched_duckdb_path": None,
                "columns": [
                    {
                        "column_name": "cost_center",
                        "column_id": "col_cc",
                        "top_values": [{"value": "CC100"}, {"value": "CC200"}],
                    }
                ],
            }
        ]
    }


def _rec(column: str) -> SliceRecommendationOutput:
    return SliceRecommendationOutput(
        table_name="journal_lines",
        column_name=column,
        priority=1,
        distinct_values=["CC100", "CC200"],
        reasoning="r",
        business_context="b",
        confidence=0.9,
    )


def test_ungrounded_recommendation_is_dropped() -> None:
    # account_id__account_type is NOT in this run's columns (passthrough view) →
    # it has no resolvable column_id and must be dropped, not stored with an empty FK.
    output = SlicingAnalysisOutput(
        recommendations=[_rec("cost_center"), _rec("account_id__account_type")]
    )
    with capture_logs() as logs:
        result = _agent()._convert_output_to_result(output, _context())

    recs = result.value.recommendations
    assert [r.column_name for r in recs] == ["cost_center"]  # only the grounded one survives
    assert all(r.column_id for r in recs)  # no empty column_id ever leaves the agent
    assert any(e.get("event") == "slice_recommendation_ungrounded" for e in logs)


def test_grounded_recommendations_pass_through() -> None:
    output = SlicingAnalysisOutput(recommendations=[_rec("cost_center")])
    result = _agent()._convert_output_to_result(output, _context())
    recs = result.value.recommendations
    assert len(recs) == 1
    assert recs[0].column_id == "col_cc"
    assert recs[0].table_id == "tbl_jl"


def test_unknown_table_recommendation_is_dropped() -> None:
    # A table the run does not have (table_id unresolvable) is dropped too — an empty
    # table_id is as invalid as an empty column_id.
    output = SlicingAnalysisOutput(
        recommendations=[
            SliceRecommendationOutput(
                table_name="ghost_table",
                column_name="cost_center",
                priority=1,
                distinct_values=["A", "B"],
                reasoning="r",
                business_context="b",
                confidence=0.9,
            )
        ]
    )
    result = _agent()._convert_output_to_result(output, _context())
    assert result.value.recommendations == []
