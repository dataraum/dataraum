"""The typed validation vocabulary — seed + generated + read (DAT-735, config→DB).

Pins the config→DB seam for validations: the shipped vertical YAML seeds typed
``Validation`` rows once (idempotently, ON CONFLICT DO NOTHING on the active-row
index), agentic induction persists ``source='generated'`` rows (re-induction
supersedes), and the loader reads active rows back — the source the validation
phase moved onto, off the raw YAML directory walk. The check LOGIC is typed:
``tolerance`` + ``guidance`` replace the free ``parameters``/``sql_hints``.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.validation.db_models import Validation
from dataraum.analysis.validation.models import ValidationSeverity, ValidationSpec
from dataraum.analysis.validation.validation_store import (
    ensure_validations_seeded,
    load_workspace_validations,
    persist_generated_validations,
)

VERTICAL = "finance"


def _active(session: Session, vertical: str = VERTICAL) -> dict[str, Validation]:
    return {
        r.validation_id: r
        for r in session.execute(
            select(Validation).where(
                Validation.vertical == vertical, Validation.superseded_at.is_(None)
            )
        ).scalars()
    }


def _gen_spec(validation_id: str, **overrides) -> ValidationSpec:
    fields: dict = {
        "validation_id": validation_id,
        "name": validation_id.replace("_", " ").title(),
        "description": "induced check",
        "category": "data_quality",
        "severity": ValidationSeverity.WARNING,
        "check_type": "constraint",
        "tolerance": 0.02,
        "guidance": "Ground this against the served columns.",
    }
    fields.update(overrides)
    return ValidationSpec(**fields)


def test_seed_finance_creates_typed_rows(session: Session) -> None:
    n = ensure_validations_seeded(session, VERTICAL)
    assert n == 9  # the nine shipped finance validations
    rows = _active(session)
    assert "double_entry_balance" in rows
    de = rows["double_entry_balance"]
    assert de.source == "seed"
    # The typed check definition: tolerance is a float column (from YAML parameters),
    # guidance carries the SQL-binding prose (the former sql_hints).
    assert de.tolerance == 0.01
    assert de.guidance and de.guidance.strip()
    assert de.severity == "critical"


def test_seed_folds_nontolerance_params_into_guidance(session: Session) -> None:
    """A spec whose sql_hints referenced keyword-list params keeps them in guidance."""
    ensure_validations_seeded(session, VERTICAL)
    tb = _active(session)["trial_balance"]
    # trial_balance.yaml carries asset_types/liability_types/... in parameters and
    # references them from sql_hints — they must survive into the typed guidance.
    assert "asset_types" in (tb.guidance or "")


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_validations_seeded(session, VERTICAL) == 9
    # A re-run (or a later phase re-entering) inserts nothing — never duplicates.
    assert ensure_validations_seeded(session, VERTICAL) == 0
    assert len(_active(session)) == 9


def test_load_returns_typed_specs(session: Session) -> None:
    ensure_validations_seeded(session, VERTICAL)
    specs = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL)}
    assert len(specs) == 9
    assert specs["double_entry_balance"].tolerance == 0.01
    # sql_hints/parameters are gone; the model exposes tolerance/guidance.
    assert specs["double_entry_balance"].guidance


def test_generated_rows_persist_alongside_seed(session: Session) -> None:
    ensure_validations_seeded(session, VERTICAL)
    inserted = persist_generated_validations(session, VERTICAL, [_gen_spec("induced_a")])
    assert inserted == 1
    rows = _active(session)
    assert rows["induced_a"].source == "generated"
    # The seed rows are untouched.
    assert rows["double_entry_balance"].source == "seed"


def test_reinduction_supersedes_prior_generated(session: Session) -> None:
    """Re-induction supersedes the prior generated set, never duplicates."""
    persist_generated_validations(session, VERTICAL, [_gen_spec("induced_a")])
    first = _active(session)["induced_a"].row_id

    persist_generated_validations(session, VERTICAL, [_gen_spec("induced_b")])
    active = _active(session)
    # induced_a superseded (gone from active), induced_b is the new active generated set.
    assert "induced_a" not in active
    assert "induced_b" in active
    # A fresh row was minted (not a mutate-in-place).
    assert active["induced_b"].row_id != first
    # The superseded history row survives.
    all_a = (
        session.execute(select(Validation).where(Validation.validation_id == "induced_a"))
        .scalars()
        .all()
    )
    assert len(all_a) == 1
    assert all_a[0].superseded_at is not None


def test_generated_collision_with_active_seed_is_skipped(session: Session) -> None:
    """A generated proposal duplicating an active seed id is skipped — the seed wins."""
    ensure_validations_seeded(session, VERTICAL)
    inserted = persist_generated_validations(
        session, VERTICAL, [_gen_spec("double_entry_balance"), _gen_spec("induced_new")]
    )
    assert inserted == 1  # only induced_new; the seed collision skipped
    rows = _active(session)
    assert rows["double_entry_balance"].source == "seed"
    assert rows["induced_new"].source == "generated"


def test_empty_generated_set_supersedes_prior(session: Session) -> None:
    """An empty induction result supersedes the prior generated set (a thin re-run)."""
    persist_generated_validations(session, VERTICAL, [_gen_spec("induced_a")])
    assert persist_generated_validations(session, VERTICAL, []) == 0
    assert "induced_a" not in _active(session)
