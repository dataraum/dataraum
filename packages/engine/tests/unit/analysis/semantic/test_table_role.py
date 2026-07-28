"""Table-role derivation (DAT-728) — the fact / periodic-snapshot / dimension cut.

The LLM answers one bit (fact vs dimension); the PeriodicSnapshot subtype is
structural (the reporting period in the grain), derived here and persisted so the
additivity COUNT rule reads a subtype instead of re-deriving it.

The period is usually not a date column. The standard warehouse snapshot keys it
as an FK into a SURROGATE-keyed calendar — ``balances(account_id, period_id)`` →
``dim_period(period_id PK, period_date DATE)`` — where no date appears in either
table's grain (DAT-847). That shape is recognized by a cardinality witness on the
dimension: one row per period means its event date is unique across its rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Set as AbstractSet

from dataraum.analysis.semantic.db_models import TableRole, derive_table_role
from dataraum.analysis.semantic.models import (
    RelationshipOutput,
    TableEntityOutput,
    TableSynthesisOutput,
    TimeColumn,
)


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


# --- period axis resolution (DAT-847) ------------------------------------------


def _time(column: str, role: str = "event", *, anchor: bool = True) -> TimeColumn:
    return TimeColumn(
        column=column,
        aspect=column.split("_")[0],
        role=role,  # type: ignore[arg-type]
        is_anchor=anchor,
        note=f"{column} note",
    )


def _table(
    name: str,
    *,
    is_fact: bool,
    grain: list[str],
    time_columns: list[TimeColumn] | None = None,
) -> TableEntityOutput:
    return TableEntityOutput(
        table_name=name,
        is_fact_table=is_fact,
        grain=grain,
        time_columns=time_columns or [],
        identity_columns=[],
    )


def _fk(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    *,
    kind: str = "foreign_key",
) -> RelationshipOutput:
    return RelationshipOutput(
        from_table=from_table,
        from_column=from_column,
        to_table=to_table,
        to_column=to_column,
        key_columns=[],
        relationship_type=kind,  # type: ignore[arg-type]
        confidence=0.9,
        reasoning="test fixture",
    )


def _calendar() -> TableEntityOutput:
    """The standard warehouse calendar: surrogate int key, the date an attribute.

    Note what is NOT true of it — its date is not in its own grain. That is the
    whole reason the role has to be decided by cardinality rather than by shape.
    """
    return _table(
        "dim_period",
        is_fact=False,
        grain=["period_id"],
        time_columns=[_time("period_date")],
    )


# One row per period: the calendar's key AND its date are both unique.
_CALENDAR_WITNESS = {"dim_period": {"period_id", "period_date"}}


def _role_of(
    synthesis: TableSynthesisOutput,
    table_name: str,
    unique_columns: Mapping[str, AbstractSet[str]],
) -> TableRole:
    table = next(t for t in synthesis.tables if t.table_name == table_name)
    dimensions = synthesis.period_columns_by_dimension(unique_columns)
    return derive_table_role(
        table.is_fact_table, table.grain, synthesis.period_axis_columns(table, dimensions)
    )


def test_period_fk_in_grain_is_a_periodic_snapshot() -> None:
    # The defect: ``balances`` holds NO date at all — its period is the integer
    # key ``period_id``, so the date-column test read it as a plain FACT and the
    # additivity COUNT rule then allowed counting across time.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("balances", is_fact=True, grain=["account_id", "period_id"]),
            _calendar(),
        ],
        relationships=[_fk("balances", "period_id", "dim_period", "period_id")],
    )
    assert _role_of(synthesis, "balances", _CALENDAR_WITNESS) == TableRole.PERIODIC_SNAPSHOT


def test_period_fk_outside_the_grain_stays_a_fact() -> None:
    # An event fact that merely REFERENCES a period (grain is the line) does not
    # re-state a population — the grain test is what makes it a snapshot.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("journal_lines", is_fact=True, grain=["line_id"]),
            _calendar(),
        ],
        relationships=[_fk("journal_lines", "period_id", "dim_period", "period_id")],
    )
    assert _role_of(synthesis, "journal_lines", _CALENDAR_WITNESS) == TableRole.FACT


def test_a_dimension_whose_date_repeats_is_not_a_period() -> None:
    # The soundness case, now decided by DATA rather than by the model's grain
    # choice. ``dim_customer`` is shaped exactly like the calendar — surrogate
    # key, one event date — and is separated from it ONLY by the witness: many
    # customers share a signup date, so the date does not identify a row.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("subscriptions", is_fact=True, grain=["customer_id"]),
            _table(
                "dim_customer",
                is_fact=False,
                grain=["customer_id"],
                time_columns=[_time("signup_date")],
            ),
        ],
        relationships=[_fk("subscriptions", "customer_id", "dim_customer", "customer_id")],
    )
    witness = {"dim_customer": {"customer_id"}}  # signup_date NOT unique
    assert _role_of(synthesis, "subscriptions", witness) == TableRole.FACT


def test_missing_witness_fails_safe_to_fact() -> None:
    # The converse of test_period_fk_in_grain_is_a_periodic_snapshot, isolating
    # the witness as the ONLY difference: the SAME calendar synthesis, witness
    # withheld (unprofiled dimension), opposite role. The role only ever tightens
    # a verdict, so an absent witness must never mint a snapshot.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("balances", is_fact=True, grain=["account_id", "period_id"]),
            _calendar(),
        ],
        relationships=[_fk("balances", "period_id", "dim_period", "period_id")],
    )
    assert _role_of(synthesis, "balances", {}) == TableRole.FACT


def test_a_fact_keyed_on_the_entity_leg_of_a_bridge_is_not_a_snapshot() -> None:
    # Only the grain columns that are THEMSELVES unique count as period keys.
    # dim_entity_period is one row per (entity, period) and happens to carry a
    # unique observed_date; handing back its WHOLE grain would let a fact that
    # FKs only the ENTITY leg read as a snapshot and lose its COUNT time axis.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("readings", is_fact=True, grain=["entity_id", "meter_id"]),
            _table(
                "dim_entity_period",
                is_fact=False,
                grain=["entity_id", "period_id"],
                time_columns=[_time("observed_date")],
            ),
        ],
        relationships=[_fk("readings", "entity_id", "dim_entity_period", "entity_id")],
    )
    # Neither grain leg identifies a row on its own; only the date does.
    witness = {"dim_entity_period": {"observed_date"}}
    assert _role_of(synthesis, "readings", witness) == TableRole.FACT


def test_a_composite_keyed_calendar_under_claims_to_fact() -> None:
    # A genuine fiscal calendar keyed (fiscal_year, fiscal_period) with a unique
    # period_end_date. NEITHER key column is individually unique — that is what
    # makes the key composite — and a per-column profile carries no JOINT
    # cardinality, so the dimension never registers and the fact stays a FACT.
    # A documented under-claim, not a wrong answer: it errs toward leaving COUNT
    # its time axis rather than denying it on an unproven period.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("balances", is_fact=True, grain=["account_id", "fy", "fp"]),
            _table(
                "dim_fiscal",
                is_fact=False,
                grain=["fiscal_year", "fiscal_period"],
                time_columns=[_time("period_end_date")],
            ),
        ],
        relationships=[_fk("balances", "fy", "dim_fiscal", "fiscal_year")],
    )
    witness = {"dim_fiscal": {"period_end_date"}}
    assert _role_of(synthesis, "balances", witness) == TableRole.FACT


def test_attribute_dated_dimension_is_not_a_period_axis() -> None:
    # DAT-780 holds on the dimension side too: a unique date the row merely
    # REFERS to is not a reporting period, however well it identifies a row.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("accruals", is_fact=True, grain=["account_id", "due_date_id"]),
            _table(
                "dim_due_date",
                is_fact=False,
                grain=["due_date_id"],
                time_columns=[_time("due_date", role="attribute", anchor=False)],
            ),
        ],
        relationships=[_fk("accruals", "due_date_id", "dim_due_date", "due_date_id")],
    )
    witness = {"dim_due_date": {"due_date_id", "due_date"}}
    assert _role_of(synthesis, "accruals", witness) == TableRole.FACT


def test_fk_to_a_fact_is_not_a_period_axis() -> None:
    # A period dimension is a DIMENSION. A fact grained on its own period is a
    # snapshot in its own right, not a calendar other facts key against.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("allocations", is_fact=True, grain=["alloc_id", "period_id"]),
            _table(
                "period_totals",
                is_fact=True,
                grain=["period_id"],
                time_columns=[_time("period_date")],
            ),
        ],
        relationships=[_fk("allocations", "period_id", "period_totals", "period_id")],
    )
    witness = {"period_totals": {"period_id", "period_date"}}
    assert _role_of(synthesis, "allocations", witness) == TableRole.FACT


def test_hierarchy_edge_is_not_followed() -> None:
    # A hierarchy edge is a parent/child link inside one entity, never a bridge
    # to a calendar.
    synthesis = TableSynthesisOutput(
        tables=[
            _table("balances", is_fact=True, grain=["account_id", "period_id"]),
            _calendar(),
        ],
        relationships=[_fk("balances", "period_id", "dim_period", "period_id", kind="hierarchy")],
    )
    assert _role_of(synthesis, "balances", _CALENDAR_WITNESS) == TableRole.FACT


def test_fk_to_a_non_key_column_of_a_calendar_is_not_the_period() -> None:
    # The reference has to land on the calendar's KEY. A join onto some other
    # column of it does not say "this row's period".
    synthesis = TableSynthesisOutput(
        tables=[
            _table("balances", is_fact=True, grain=["account_id", "fiscal_year"]),
            _calendar(),
        ],
        relationships=[_fk("balances", "fiscal_year", "dim_period", "fiscal_year")],
    )
    assert _role_of(synthesis, "balances", _CALENDAR_WITNESS) == TableRole.FACT


def test_event_date_in_grain_still_wins_without_any_relationship() -> None:
    # The DAT-780 path is untouched: the fact's own event date in the grain is a
    # snapshot with no dimension in sight.
    synthesis = TableSynthesisOutput(
        tables=[
            _table(
                "trial_balance",
                is_fact=True,
                grain=["account_id", "period"],
                time_columns=[_time("period")],
            )
        ],
        relationships=[],
    )
    assert _role_of(synthesis, "trial_balance", {}) == TableRole.PERIODIC_SNAPSHOT


def test_attribute_date_in_grain_does_not_flip_a_fact() -> None:
    # DAT-780's original case, re-pinned through the new derivation.
    synthesis = TableSynthesisOutput(
        tables=[
            _table(
                "invoices",
                is_fact=True,
                grain=["invoice_id", "due_date"],
                time_columns=[_time("due_date", role="attribute", anchor=False)],
            )
        ],
        relationships=[],
    )
    assert _role_of(synthesis, "invoices", {}) == TableRole.FACT
