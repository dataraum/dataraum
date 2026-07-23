"""The typed validation vocabulary — seed + generated + read (DAT-735, config→DB).

Pins the config→DB seam for validations: a vertical's shipped YAML, when one
ships, seeds typed ``Validation`` rows once (idempotently, ON CONFLICT DO
NOTHING on the active-row index), agentic induction persists
``source='generated'`` rows (re-induction supersedes), and the loader reads
active rows back — the source the validation phase moved onto, off the raw
YAML directory walk. The check LOGIC is typed: ``tolerance`` + ``guidance``
replace the free ``parameters``/``sql_hints``.

DAT-725 band 3 retired finance's nine shipped YAMLs entirely — no vertical
ships a ``validations/`` directory today, so ``ensure_validations_seeded``
against "finance" degrades to a clean no-op (see
``test_seed_now_yields_nothing`` below). The seed/idempotency/load MACHINERY
these tests pin is otherwise unchanged, so they exercise it via direct
``source='seed'`` DB rows — never the real (now-deleted) config tree.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dataraum.analysis.validation.db_models import Validation
from dataraum.analysis.validation.models import ValidationSeverity, ValidationSpec
from dataraum.analysis.validation.validation_store import (
    ensure_validations_seeded,
    load_workspace_validations,
    persist_generated_validations,
)
from dataraum.storage.upsert import insert_if_absent

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


def _seed_row(validation_id: str, **overrides) -> dict:
    """A synthetic ``source='seed'`` row dict — stands in for what a vertical's
    (now-retired, for finance) shipped YAML used to normalize into via
    ``ensure_validations_seeded``. NEVER reads the real config tree."""
    row = {
        "vertical": VERTICAL,
        "validation_id": validation_id,
        "name": validation_id.replace("_", " ").title(),
        "description": "synthetic seed check",
        "category": "financial",
        "severity": "critical",
        "check_type": "balance",
        "tolerance": 0.01,
        "guidance": "Sum debit - credit per account_type.",
        "source": "seed",
    }
    row.update(overrides)
    return row


def test_seed_now_yields_nothing(session: Session) -> None:
    """DAT-725 band 3: finance's shipped validations/ directory is retired —
    ensure_validations_seeded degrades to a clean no-op (FileNotFoundError →
    the family's empty base, no loader code change), never raising. LLM
    induction is the sole validation source now."""
    assert ensure_validations_seeded(session, VERTICAL) == 0
    assert _active(session) == {}


def test_seeded_row_carries_the_typed_check_definition(session: Session) -> None:
    """A source='seed' row (synthetic here — historically finance's shipped
    YAML) carries the typed check definition: tolerance is a float column,
    guidance carries the SQL-binding prose (the former sql_hints)."""
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()
    de = _active(session)["double_entry_balance"]
    assert de.source == "seed"
    assert de.tolerance == 0.01
    assert de.guidance and de.guidance.strip()
    assert de.severity == "critical"


def test_seed_folds_nontolerance_params_into_guidance(session: Session) -> None:
    """A spec built from the legacy parameters/sql_hints shape (the normalizer's
    remaining live producer shape, DAT-447) still folds non-tolerance params
    into guidance before it reaches a seed row — the same fold
    ensure_validations_seeded ran per-doc against the now-retired shipped YAML."""
    spec = ValidationSpec.model_validate(
        {
            "validation_id": "trial_balance",
            "name": "Trial Balance",
            "description": "Assets + expenses equal liabilities + equity + revenue",
            "category": "financial",
            "check_type": "balance",
            "sql_hints": "classify accounts",
            "parameters": {"tolerance": 0.01, "asset_types": ["asset", "assets"]},
        }
    )
    session.add(
        Validation(**_seed_row("trial_balance", tolerance=spec.tolerance, guidance=spec.guidance))
    )
    session.flush()
    tb = _active(session)["trial_balance"]
    assert "asset_types" in (tb.guidance or "")


def test_seed_is_idempotent(session: Session) -> None:
    """A second insert of the same active (vertical, validation_id) is skipped
    via ON CONFLICT DO NOTHING on the active-row index — the primitive
    ensure_validations_seeded relies on for a race-safe re-seed."""
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()
    inserted = insert_if_absent(
        session,
        Validation,
        [_seed_row("double_entry_balance")],
        index_elements=["vertical", "validation_id"],
        index_where=text("superseded_at IS NULL"),
    )
    assert inserted == 0
    assert len(_active(session)) == 1


def test_load_returns_typed_specs(session: Session) -> None:
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()
    specs = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL)}
    assert len(specs) == 1
    assert specs["double_entry_balance"].tolerance == 0.01
    # sql_hints/parameters are gone; the model exposes tolerance/guidance.
    assert specs["double_entry_balance"].guidance


def test_generated_rows_persist_alongside_seed(session: Session) -> None:
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()
    inserted = persist_generated_validations(session, VERTICAL, [_gen_spec("induced_a")])
    assert inserted == 1
    rows = _active(session)
    assert rows["induced_a"].source == "generated"
    # The seed row is untouched.
    assert rows["double_entry_balance"].source == "seed"


def test_relevant_conventions_roundtrip(session: Session) -> None:
    """The declared convention dependency persists and loads back typed (DAT-865).

    The validation→convention edge is what routes a convention to a GENERATED
    check's SQL binder — losing it on either leg of the roundtrip silently
    reverts the binder to an empty conventions block.
    """
    persist_generated_validations(
        session,
        VERTICAL,
        [_gen_spec("induced_dep", relevant_conventions=["sign_natural_balance"])],
    )
    row = _active(session)["induced_dep"]
    assert row.relevant_conventions == ["sign_natural_balance"]
    specs = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL)}
    assert specs["induced_dep"].relevant_conventions == ["sign_natural_balance"]
    # Undeclared ⇒ empty list (NULL in the row), never None on the spec.
    persist_generated_validations(session, VERTICAL, [_gen_spec("induced_plain")])
    plain = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL)}
    assert plain["induced_plain"].relevant_conventions == []


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
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()
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


def test_source_check_rejects_unknown_vocab(session: Session) -> None:
    """The DB enforces ck_validations_source — only live-writer sources are admitted."""
    session.add(Validation(**_seed_row("raw_check", source="teach")))
    with pytest.raises(IntegrityError):
        session.flush()


def test_severity_check_rejects_unknown_vocab(session: Session) -> None:
    """The DB enforces ck_validations_severity (derived from ValidationSeverity)."""
    session.add(Validation(**_seed_row("raw_check", severity="fatal")))
    with pytest.raises(IntegrityError):
        session.flush()


def test_active_partial_unique_blocks_two_active_rows(session: Session) -> None:
    """uq_validation_active permits at most one ACTIVE row per (vertical, validation_id)."""
    session.add(Validation(**_seed_row("raw_check")))
    session.flush()
    session.add(Validation(**_seed_row("raw_check")))  # second ACTIVE row, same id
    with pytest.raises(IntegrityError):
        session.flush()
