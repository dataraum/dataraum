"""Membership floor for detected cycles (DAT-630).

``verify_cycles`` rejects cycles whose cited categorical references aren't in the
served context — a guardrail on the agent, not a re-detector. These pin the
reject/keep boundary: a fabricated column or value is dropped; a value on a
column with no served value-set is kept (we can't prove improvisation).
"""

from __future__ import annotations

from dataraum.analysis.cycles.models import CycleStage, DetectedCycle, EntityFlow
from dataraum.analysis.cycles.verify import verify_cycles


def _cycle(**kwargs) -> DetectedCycle:
    base = {
        "cycle_id": "c1",
        "cycle_name": "Test Cycle",
        "cycle_type": "journal_entry_cycle",
        "description": "",
    }
    base.update(kwargs)
    return DetectedCycle(**base)


def _context() -> dict:
    return {
        "tables": [
            {
                "table_name": "journal",
                "columns": [{"name": "status"}, {"name": "debit"}, {"name": "credit"}],
            },
            {"table_name": "accounts", "columns": [{"name": "account_id"}]},
        ],
        "slice_definitions": [
            {
                "table_name": "journal",
                "column_name": "status",
                "value_counts": [{"value": "posted"}, {"value": "draft"}],
                "values": [],
            }
        ],
    }


def test_keeps_cycle_with_served_references() -> None:
    """A cycle whose every reference is in context survives."""
    cycle = _cycle(
        status_table="journal",
        status_column="status",
        completion_value="posted",
        stages=[
            CycleStage(
                stage_name="Posted",
                stage_order=1,
                indicator_column="status",
                indicator_values=["posted"],
            )
        ],
        entity_flows=[
            EntityFlow(entity_type="account", entity_column="account_id", entity_table="accounts")
        ],
    )
    kept, rejections = verify_cycles([cycle], _context())
    assert len(kept) == 1
    assert rejections == []


def test_rejects_improvised_column() -> None:
    """A status column not in the workspace is a hallucination — dropped."""
    cycle = _cycle(
        status_table="journal", status_column="posting_status", completion_value="posted"
    )
    kept, rejections = verify_cycles([cycle], _context())
    assert kept == []
    assert "posting_status" in rejections[0]


def test_rejects_improvised_completion_value() -> None:
    """A completion value absent from a served value-set is dropped."""
    cycle = _cycle(status_table="journal", status_column="status", completion_value="finalized")
    kept, rejections = verify_cycles([cycle], _context())
    assert kept == []
    assert "finalized" in rejections[0]


def test_value_unprovable_when_no_value_set_is_kept() -> None:
    """A value on a column with no served value-set can't be proven made up — kept."""
    # 'debit' is a real column but not a slice → no value-set to check against.
    cycle = _cycle(status_table="journal", status_column="debit", completion_value="anything")
    kept, _ = verify_cycles([cycle], _context())
    assert len(kept) == 1


def test_numeric_completion_cycle_has_no_status_refs() -> None:
    """A numeric-completion cycle (no status column) passes the floor untouched."""
    cycle = _cycle(status_column=None, completion_rate=0.99)
    kept, rejections = verify_cycles([cycle], _context())
    assert len(kept) == 1
    assert rejections == []


def test_layer_prefixed_table_resolves() -> None:
    """A cycle citing the layer-prefixed table name (typed_journal) still resolves."""
    cycle = _cycle(status_table="typed_journal", status_column="status", completion_value="posted")
    kept, rejections = verify_cycles([cycle], _context())
    assert len(kept) == 1
    assert rejections == []


def test_rejects_improvised_stage_indicator_column() -> None:
    """A stage indicator column not in the workspace is dropped."""
    cycle = _cycle(
        status_table="journal",
        status_column="status",
        stages=[CycleStage(stage_name="Posted", stage_order=1, indicator_column="ghost_col")],
    )
    kept, rejections = verify_cycles([cycle], _context())
    assert kept == []
    assert "ghost_col" in rejections[0]


def test_rejects_improvised_stage_indicator_value() -> None:
    """A stage indicator value absent from a served value-set is dropped."""
    cycle = _cycle(
        status_table="journal",
        status_column="status",
        stages=[
            CycleStage(
                stage_name="Posted",
                stage_order=1,
                indicator_column="status",
                indicator_values=["nonexistent"],
            )
        ],
    )
    kept, rejections = verify_cycles([cycle], _context())
    assert kept == []
    assert "nonexistent" in rejections[0]


def test_rejects_improvised_entity_flow_column() -> None:
    """An entity-flow column not in the workspace is dropped."""
    cycle = _cycle(
        entity_flows=[
            EntityFlow(entity_type="account", entity_column="ghost_id", entity_table="accounts")
        ],
    )
    kept, rejections = verify_cycles([cycle], _context())
    assert kept == []
    assert "ghost_id" in rejections[0]
