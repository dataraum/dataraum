"""Column eligibility phase against a real substrate.

Pins the deletion of the key-name hard abort (German-dates user report,
2026-06-05): the phase used to ``PhaseResult.failed`` the ENTIRE run when an
INELIGIBLE (100%-null) column's NAME matched ``_id$``/``^id$``/``_key$`` —
name-only key inference with no structural evidence and no teach escape
hatch. An all-null key-named column is now dropped + recorded INELIGIBLE
like any other, and the run continues; key-ness claims live downstream where
relationship evidence exists.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord
from dataraum.pipeline.base import PhaseStatus
from dataraum.storage import Column, Table


@pytest.fixture
def csv_with_all_null_id_column(tmp_path):
    """An optional FK column (``logikal30_id``) that is 100% empty in this extract."""
    csv_file = tmp_path / "positions.csv"
    csv_file.write_text(
        "id,logikal30_id,amount\n1,,10.5\n2,,20.0\n3,,30.25\n4,,40.0\n",
    )
    return csv_file


def test_all_null_key_named_column_drops_without_failing_the_run(
    harness, csv_with_all_null_id_column
) -> None:
    result = harness.run_import(source_path=csv_with_all_null_id_column, source_name="positions")
    assert result.status == PhaseStatus.COMPLETED, result.error
    result = harness.run_phase("typing")
    assert result.status == PhaseStatus.COMPLETED, result.error
    result = harness.run_phase("statistics")
    assert result.status == PhaseStatus.COMPLETED, result.error

    # The old behavior: PhaseResult.failed("Critical column 'logikal30_id' …
    # Cannot proceed with unusable key column.") — the whole run died.
    result = harness.run_phase("column_eligibility")
    assert result.status == PhaseStatus.COMPLETED, result.error
    assert result.outputs is not None
    assert result.outputs["dropped"] == 1

    with harness.session_factory() as session:
        # Recorded INELIGIBLE like any other all-null column…
        record = session.execute(
            select(ColumnEligibilityRecord).where(
                ColumnEligibilityRecord.column_name == "logikal30_id"
            )
        ).scalar_one()
        assert record.status == "INELIGIBLE"
        assert record.triggered_rule == "all_null"

        # …dropped from the typed table; the usable columns survive.
        typed_table = session.execute(select(Table).where(Table.layer == "typed")).scalar_one()
        typed_names = set(
            session.execute(
                select(Column.column_name).where(Column.table_id == typed_table.table_id)
            ).scalars()
        )
        assert "logikal30_id" not in typed_names
        assert {"id", "amount"} <= typed_names
