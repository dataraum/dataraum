"""The typed validation vocabulary — seed + generated + read (DAT-735, config→DB).

Pins the config→DB seam for validations: a vertical's shipped YAML, when one
ships, seeds typed ``Validation`` rows once (idempotently, ON CONFLICT DO
NOTHING on the active-row index), agentic induction STAGES ``source='generated'``
proposals run-versioned and the operating_model promote materializes them
(re-induction supersedes), and the loader reads active rows back — the source the
validation phase moved onto, off the raw YAML directory walk. The check LOGIC is
typed: ``tolerance`` + ``guidance`` replace the free ``parameters``/``sql_hints``.

The staged/materialized split is DAT-877: a generation must not be readable as the
workspace's live vocabulary until the run that induced it promotes, because its
executed results and detected cycles are head-gated and only appear at that same
flip. These tests pin both legs and the run-scoped in-run read.

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

from dataraum.analysis.validation.db_models import InducedValidation, Validation
from dataraum.analysis.validation.models import ValidationSeverity, ValidationSpec
from dataraum.analysis.validation.validation_store import (
    ensure_validations_seeded,
    load_workspace_validations,
    materialize_induced_validations,
    stage_induced_validations,
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


RUN = "run-1"


def _induce(session: Session, specs: list[ValidationSpec], run_id: str = RUN) -> int:
    """Stage a generation and promote it — the full induction→promote path.

    The two legs are exercised separately where the SPLIT is what's under test;
    this helper is for the tests that only care about the landed outcome.
    """
    stage_induced_validations(session, run_id, VERTICAL, specs)
    return materialize_induced_validations(session, run_id)


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
    inserted = _induce(session, [_gen_spec("induced_a")])
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
    _induce(session, [_gen_spec("induced_dep", relevant_conventions=["sign_natural_balance"])])
    row = _active(session)["induced_dep"]
    assert row.relevant_conventions == ["sign_natural_balance"]
    specs = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL)}
    assert specs["induced_dep"].relevant_conventions == ["sign_natural_balance"]
    # Undeclared ⇒ empty list (NULL in the row), never None on the spec.
    _induce(session, [_gen_spec("induced_plain")], run_id="run-2")
    plain = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL)}
    assert plain["induced_plain"].relevant_conventions == []


def test_reinduction_supersedes_prior_generated(session: Session) -> None:
    """Re-induction supersedes the prior generated set, never duplicates."""
    _induce(session, [_gen_spec("induced_a")])
    first = _active(session)["induced_a"].row_id

    _induce(session, [_gen_spec("induced_b")], run_id="run-2")
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
    inserted = _induce(session, [_gen_spec("double_entry_balance"), _gen_spec("induced_new")])
    assert inserted == 1  # only induced_new; the seed collision skipped
    rows = _active(session)
    assert rows["double_entry_balance"].source == "seed"
    assert rows["induced_new"].source == "generated"


def test_degraded_induction_leaves_the_prior_generation_standing(session: Session) -> None:
    """A DEGRADED induction supersedes nothing (DAT-877).

    A parse/render failure reports generated=0 and never stages — so it writes no
    seal, and the promote keeps the previously promoted generation. (The old
    induction-time writer superseded first and asked questions later, which turned a
    degraded LLM turn into vocabulary loss.)
    """
    _induce(session, [_gen_spec("induced_a")])
    # No stage_induced_validations call at all — that IS the degraded path.
    assert materialize_induced_validations(session, "run-2") == 0
    assert "induced_a" in _active(session)


def test_zero_proposal_induction_retires_the_prior_generation(session: Session) -> None:
    """A SUCCESSFUL induction proposing zero DOES supersede (DAT-877).

    The counterpart to the degraded case, and the reason the seal exists: without it
    the two are indistinguishable at promote time and a generated validation could
    never be retired once induced — a thin-graph or deliberately-empty re-induction
    would re-seal the previous generation forever. Absence stays loud on the
    retirement axis.
    """
    _induce(session, [_gen_spec("induced_a")])
    assert _induce(session, [], run_id="run-2") == 0
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


def test_staging_does_not_touch_the_live_vocabulary(session: Session) -> None:
    """The DAT-877 invariant: staging alone publishes NOTHING.

    This is the whole point of the split. Induction commits its activity minutes
    before the run's executed results and detected cycles become visible (both are
    head-gated on the operating_model promote), so a generation that reached the
    vocabulary home at induction time was readable as current with zero evidence
    behind it — and stayed that way forever if the run never promoted.
    """
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()

    staged = stage_induced_validations(session, RUN, VERTICAL, [_gen_spec("induced_a")])
    assert staged == 1

    # The head-facing vocabulary is untouched — only the seed row is active.
    assert set(_active(session)) == {"double_entry_balance"}
    # ... and a head-resolved read (no run_id) cannot see the staged proposal.
    assert "induced_a" not in {
        s.validation_id for s in load_workspace_validations(session, VERTICAL)
    }


def test_in_run_read_sees_this_runs_staged_set(session: Session) -> None:
    """The run's own phases read their staged generation by run_id (DAT-877).

    The validation phase binds and executes the set induction just proposed, so the
    staging must be invisible to the WORLD without being invisible to the run.
    """
    stage_induced_validations(session, RUN, VERTICAL, [_gen_spec("induced_a")])
    stage_induced_validations(session, "other-run", VERTICAL, [_gen_spec("induced_other")])

    in_run = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL, run_id=RUN)}
    assert "induced_a" in in_run
    assert in_run["induced_a"].source == "generated"
    # Strictly this run's — a sibling run's staging never leaks in.
    assert "induced_other" not in in_run


def test_in_run_read_never_displaces_an_active_seed(session: Session) -> None:
    """Seed precedence holds on the in-run read exactly as it does at materialize."""
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()
    stage_induced_validations(session, RUN, VERTICAL, [_gen_spec("double_entry_balance")])

    in_run = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL, run_id=RUN)}
    assert in_run["double_entry_balance"].source == "seed"


def test_unpromoted_run_leaves_the_prior_generation_live(session: Session) -> None:
    """A run that dies after induction is a NO-OP on the vocabulary (DAT-877).

    The correctness half of the ticket: any post-induction exit — a non-retryable
    PhaseFailed downstream, the ``nothing_declared`` completion, a crash — used to
    leave the workspace serving an unsealed generation with no results, because the
    prior one had already been superseded. Never promoting must now change nothing.
    """
    _induce(session, [_gen_spec("induced_a")])
    before = _active(session)["induced_a"].row_id

    # A second run induces, then never reaches its promote.
    stage_induced_validations(session, "run-2", VERTICAL, [_gen_spec("induced_b")])

    active = _active(session)
    assert set(active) == {"induced_a"}
    assert active["induced_a"].row_id == before


def test_restaging_the_same_run_is_idempotent(session: Session) -> None:
    """An activity retry re-stages in place — ADR-0010 (key, run_id) upsert."""
    stage_induced_validations(session, RUN, VERTICAL, [_gen_spec("induced_a")])
    stage_induced_validations(session, RUN, VERTICAL, [_gen_spec("induced_a", tolerance=0.5)])

    rows = (
        session.execute(select(InducedValidation).where(InducedValidation.run_id == RUN))
        .scalars()
        .all()
    )
    assert len(rows) == 1
    assert rows[0].tolerance == 0.5


def test_materialize_retry_is_quiescent(session: Session) -> None:
    """A promote retry writes NOTHING when the live set already matches (DAT-877).

    Temporal re-runs a committed-but-unacked activity. A blind re-supersede would
    retire the rows this promote just wrote and re-insert them under fresh row_ids —
    phantom superseded generations accumulating in plain sight, since
    ``__READ__.validations`` is a pass-through over the whole table.
    """
    stage_induced_validations(session, RUN, VERTICAL, [_gen_spec("induced_a")])
    assert materialize_induced_validations(session, RUN) == 1
    row_id = _active(session)["induced_a"].row_id

    assert materialize_induced_validations(session, RUN) == 1

    assert set(_active(session)) == {"induced_a"}
    # Same row, not a re-mint — and no superseded phantom behind it.
    assert _active(session)["induced_a"].row_id == row_id
    all_rows = (
        session.execute(select(Validation).where(Validation.validation_id == "induced_a"))
        .scalars()
        .all()
    )
    assert len(all_rows) == 1


def test_materialize_rewrites_when_content_drifted(session: Session) -> None:
    """Quiescence is content-keyed, not run-keyed — a drifted spec still lands."""
    _induce(session, [_gen_spec("induced_a", tolerance=0.02)])
    stage_induced_validations(session, "run-2", VERTICAL, [_gen_spec("induced_a", tolerance=0.5)])

    assert materialize_induced_validations(session, "run-2") == 1
    assert _active(session)["induced_a"].tolerance == 0.5


def test_in_run_read_replaces_the_prior_generation(session: Session) -> None:
    """A staged generation supersedes the prior one wholesale on the in-run read.

    The declared set a run works on must equal the set its promote will publish.
    Merging instead of replacing would have the run declare, bind and EXECUTE a
    check its own induction dropped — evidence for a validation that is about to
    stop existing.
    """
    session.add(Validation(**_seed_row("double_entry_balance")))
    session.flush()
    _induce(session, [_gen_spec("induced_old")])

    stage_induced_validations(session, "run-2", VERTICAL, [_gen_spec("induced_new")])
    in_run = {
        s.validation_id for s in load_workspace_validations(session, VERTICAL, run_id="run-2")
    }
    # The seed survives; the prior generation does not.
    assert in_run == {"double_entry_balance", "induced_new"}

    # And the promote lands exactly that set.
    materialize_induced_validations(session, "run-2")
    assert set(_active(session)) == {"double_entry_balance", "induced_new"}


def test_in_run_read_of_a_degraded_induction_keeps_the_prior_generation(
    session: Session,
) -> None:
    """Staged nothing ⇒ the run declares the previously promoted generation.

    A degraded induction (parse/render failure → generated=0) must not empty the
    run's declared set; it falls back to the live vocabulary, which is exactly what
    the promote will leave standing.
    """
    _induce(session, [_gen_spec("induced_a")])

    in_run = {
        s.validation_id for s in load_workspace_validations(session, VERTICAL, run_id="run-2")
    }
    assert in_run == {"induced_a"}


def test_in_run_read_serves_the_runs_own_drifted_tolerance(session: Session) -> None:
    """The in-run read serves THIS run's spec, not the promoted one (DAT-877).

    The sharp case behind the third-consumer sweep: an id present in BOTH
    generations whose tolerance drifted. A head-free spec read inside the run
    evaluates this run's results against the PREVIOUS generation's tolerance — a
    wrong verdict rather than a missing one, so nothing looks broken.
    """
    _induce(session, [_gen_spec("induced_a", tolerance=0.02)])
    stage_induced_validations(session, "run-2", VERTICAL, [_gen_spec("induced_a", tolerance=0.5)])

    in_run = {
        s.validation_id: s for s in load_workspace_validations(session, VERTICAL, run_id="run-2")
    }
    assert in_run["induced_a"].tolerance == 0.5
    # The head still serves the promoted generation until run-2 promotes.
    head = {s.validation_id: s for s in load_workspace_validations(session, VERTICAL)}
    assert head["induced_a"].tolerance == 0.02
