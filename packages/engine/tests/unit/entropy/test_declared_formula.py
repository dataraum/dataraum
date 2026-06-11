"""Declared-expected-formula teach loader — the derived_value teach substrate.

DAT-447 Option B: a declared expected formula rides the EXISTING ``validation``
teach — a spec-shaped ``ConfigOverlay(type='validation')`` row with
``check_type: "expected_formula"`` and ``parameters: {table, column, formula}``
(documented on ``core.overlay._apply_validation``). The validation phase
executes it as a check every run; ``load_declared_formula`` reads the same rows
directly so the derived_value measurement pools the declaration as the
``human_declaration`` witness. Mirrors ``load_documented_dependencies``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from dataraum.entropy.detectors.loaders import load_declared_formula
from dataraum.storage import ConfigOverlay


def _declaration_payload(formula: str, table: str = "orders", column: str = "total") -> dict:
    """A spec-shaped validation payload carrying an expected-formula declaration."""
    return {
        "vertical": "finance",
        "validation_id": f"expected_formula:{table}.{column}",
        "name": f"Expected formula for {table}.{column}",
        "description": f"{column} should equal {formula}",
        "category": "business_rule",
        "check_type": "expected_formula",
        "parameters": {"table": table, "column": column, "formula": formula},
    }


def test_declared_formula_is_loaded_for_the_column(session: Session) -> None:
    session.add(ConfigOverlay(type="validation", payload=_declaration_payload("subtotal + tax")))
    session.flush()
    assert load_declared_formula(session, "orders", "total") == {"formula": "subtotal + tax"}


def test_identity_match_is_case_insensitive(session: Session) -> None:
    session.add(ConfigOverlay(type="validation", payload=_declaration_payload("subtotal + tax")))
    session.flush()
    assert load_declared_formula(session, "Orders", " TOTAL ") == {"formula": "subtotal + tax"}


def test_other_columns_and_tables_load_nothing(session: Session) -> None:
    session.add(ConfigOverlay(type="validation", payload=_declaration_payload("subtotal + tax")))
    session.flush()
    assert load_declared_formula(session, "orders", "subtotal") is None
    assert load_declared_formula(session, "invoices", "total") is None


def test_superseded_declaration_is_ignored(session: Session) -> None:
    """An undone teach (superseded_at set) no longer declares the formula."""
    session.add(
        ConfigOverlay(
            type="validation",
            payload=_declaration_payload("subtotal + tax"),
            superseded_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
    )
    session.flush()
    assert load_declared_formula(session, "orders", "total") is None


def test_last_declaration_wins(session: Session) -> None:
    """Re-declaring replaces — created_at ASC, last write wins (the applier's rule)."""
    session.add(
        ConfigOverlay(
            type="validation",
            payload=_declaration_payload("subtotal + tax"),
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    session.add(
        ConfigOverlay(
            type="validation",
            payload=_declaration_payload("subtotal * tax_rate"),
            created_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
    )
    session.flush()
    assert load_declared_formula(session, "orders", "total") == {"formula": "subtotal * tax_rate"}


def test_non_formula_validation_rows_are_ignored(session: Session) -> None:
    """Ordinary declared validations (other check_types) are not declarations."""
    session.add(
        ConfigOverlay(
            type="validation",
            payload={
                "vertical": "finance",
                "validation_id": "balance_check",
                "name": "Trial balance",
                "description": "debits equal credits",
                "category": "financial",
                "check_type": "balance",
                "parameters": {"table": "orders", "column": "total"},
            },
        )
    )
    session.flush()
    assert load_declared_formula(session, "orders", "total") is None


def test_other_overlay_types_are_ignored(session: Session) -> None:
    session.add(
        ConfigOverlay(
            type="expected_dependency",
            payload={"column_ids": ["c_a", "c_b"]},
        )
    )
    session.flush()
    assert load_declared_formula(session, "orders", "total") is None
