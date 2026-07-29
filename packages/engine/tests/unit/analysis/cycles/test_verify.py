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


def test_membership_comes_only_from_the_measured_profile() -> None:
    """DAT-671: the floor rejects what the DATA never contained.

    ``slice_definitions[*]['values']`` used to be the slicing agent's ECHO of a
    value list rather than a measurement of one, so a completion value the model
    had invented in the slicing turn was already sitting in the served set by
    the time the cycles turn cited it — the anti-hallucination check validating
    one LLM claim against another. With the values read from the statistical
    profile, an unmeasured value has nowhere to enter from.
    """
    context = _context()
    context["slice_definitions"][0]["values"] = ["posted", "draft"]

    invented = _cycle(status_table="journal", status_column="status", completion_value="settled")
    kept, rejections = verify_cycles([invented], context)
    assert kept == []
    assert "settled" in rejections[0]

    measured = _cycle(status_table="journal", status_column="status", completion_value="posted")
    assert len(verify_cycles([measured], context)[0]) == 1


def test_an_enriched_axis_floor_is_its_own_values_not_the_fk_column_s() -> None:
    """Each axis gets its OWN membership set, keyed by its OWN name.

    An enriched axis (``{fk}__{attr}``) is catalogued against the fact's FK
    ``column_id``, and both the served heading and the value counts used to be
    resolved through that id. Every ``invoice_id__*`` axis therefore collapsed
    onto the single key ``invoice_id`` and their value-sets unioned — so a cycle
    completing on the *status* value 'paid' passed while pointed at the id
    column, and an invoice id passed as a status. Two distinct keys, two
    distinct sets, no union.
    """
    context = {
        "tables": [
            {
                "table_name": "invoices",
                "columns": [{"name": "invoice_id"}, {"name": "invoice_id__status"}],
            }
        ],
        "slice_definitions": [
            {
                "table_name": "invoices",
                "column_name": "invoice_id",
                "value_counts": [{"value": "INV-1"}, {"value": "INV-2"}],
                "values": ["INV-1", "INV-2"],
            },
            {
                "table_name": "invoices",
                "column_name": "invoice_id__status",
                "value_counts": [{"value": "open"}, {"value": "paid"}],
                "values": ["open", "paid"],
            },
        ],
    }

    honest = _cycle(
        status_table="invoices", status_column="invoice_id__status", completion_value="paid"
    )
    assert len(verify_cycles([honest], context)[0]) == 1

    # 'paid' is not a member of the id column — the union used to let it pass.
    crossed = _cycle(status_table="invoices", status_column="invoice_id", completion_value="paid")
    kept, rejections = verify_cycles([crossed], context)
    assert kept == []
    assert "paid" in rejections[0]
