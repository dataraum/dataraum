"""Table-role derivation (DAT-728) — the fact / periodic-snapshot / dimension cut.

The LLM answers one bit (fact vs dimension); the PeriodicSnapshot subtype is
structural (a time column in the grain), derived here and persisted so the
additivity COUNT rule reads a subtype instead of re-deriving grain∩time.
"""

from __future__ import annotations

from dataraum.analysis.semantic.db_models import TableRole, derive_table_role


def test_dimension_when_not_fact() -> None:
    assert derive_table_role(False, ["account_id"], []) == TableRole.DIMENSION


def test_fact_when_time_column_is_not_in_grain() -> None:
    # journal_lines: grain is the transaction line id; the posting date is an
    # event-time axis but not part of the grain → an additive event fact.
    assert derive_table_role(True, ["line_id"], ["posting_date"]) == TableRole.FACT


def test_periodic_snapshot_when_time_column_is_in_grain() -> None:
    # trial_balance: keyed by (account, period) — the period IS in the grain, so a
    # COUNT re-states the same population each period (non-additive across time).
    assert (
        derive_table_role(True, ["account_id", "period"], ["period"]) == TableRole.PERIODIC_SNAPSHOT
    )


def test_fact_with_empty_grain_is_not_a_snapshot() -> None:
    assert derive_table_role(True, [], ["posting_date"]) == TableRole.FACT
