"""The typed convention vocabulary — seed + read (DAT-789, config→DB).

Pins the config→DB seam for conventions: the shipped vertical YAML seeds typed
``Convention`` rows once (idempotently, ON CONFLICT DO NOTHING on the active-row
index), and all three SQL authors read the active rows back — the source the
extraction / validation / Q&A consumers moved onto, off the raw ``OntologyLoader``
YAML read. Conventions stay PROSE: the ``statement`` is stored + served verbatim.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.convention_store import (
    ensure_conventions_seeded,
    load_workspace_conventions,
)
from dataraum.analysis.semantic.db_models import Convention, WorkspaceSettings
from dataraum.analysis.semantic.ontology import OntologyDefinition, OntologyLoader


def _bind_vertical(session: Session, vertical: str) -> None:
    session.add(WorkspaceSettings(pin=True, active_vertical=vertical))
    session.flush()


def _active(session: Session, vertical: str) -> dict[str, Convention]:
    return {
        r.name: r
        for r in session.execute(
            select(Convention).where(
                Convention.vertical == vertical, Convention.superseded_at.is_(None)
            )
        ).scalars()
    }


def test_seed_finance_creates_typed_rows(session: Session) -> None:
    n = ensure_conventions_seeded(session, "finance")
    assert n == 3
    rows = _active(session, "finance")
    assert set(rows) == {
        "sign_natural_balance",
        "ledger_leg_netting",
        "balance_sheet_composition",
    }
    sign = rows["sign_natural_balance"]
    assert sign.source == "seed"
    # The statement is served verbatim (a non-empty prose blob the engine never parses).
    assert sign.statement.strip()
    # The routing envelope round-trips typed (JSON list), including the per-spec qualifier.
    assert "validation:sign_conventions" in sign.targets
    assert "qa" in sign.targets
    # The concept_groups partition round-trips typed (JSON object).
    assert set(sign.concept_groups) == {"credit_normal", "debit_normal"}
    assert "revenue" in sign.concept_groups["credit_normal"]
    # A statement-only convention (no groups) stores NULL concept_groups (empty ⇒ None).
    assert rows["ledger_leg_netting"].concept_groups is None


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_conventions_seeded(session, "finance") == 3
    # A re-run (or a later phase re-entering) inserts nothing — never duplicates,
    # never clobbers an edited row.
    assert ensure_conventions_seeded(session, "finance") == 0
    assert len(_active(session, "finance")) == 3


def test_seed_does_not_clobber_a_frame_edit(session: Session) -> None:
    """A frame edit (supersede + insert a new active row) survives a re-seed.

    The re-seed's ``ON CONFLICT DO NOTHING`` skips the convention whose active row is
    the frame edit — the seed never overwrites a user's declared convention, and never
    RAISES on the collision (the old read-then-insert would ``IntegrityError`` here
    under concurrency). The race-safety contract shared with ``ensure_concepts_seeded``.
    """
    assert ensure_conventions_seeded(session, "finance") == 3
    # Simulate a frame edit of 'sign_natural_balance': supersede the seed row, insert a
    # new active row (source='frame', an edited statement).
    session.execute(
        update(Convention)
        .where(
            Convention.vertical == "finance",
            Convention.name == "sign_natural_balance",
            Convention.superseded_at.is_(None),
        )
        .values(superseded_at=datetime.now(UTC))
    )
    session.add(
        Convention(
            vertical="finance",
            name="sign_natural_balance",
            statement="user-edited sign rule",
            targets=["qa"],
            source="frame",
        )
    )
    session.flush()
    # Re-seed collides on the active partial-unique index → skipped, no error.
    assert ensure_conventions_seeded(session, "finance") == 0
    active = {c.id: c for c in load_workspace_conventions(session, "finance")}
    assert active["sign_natural_balance"].statement == "user-edited sign rule"
    assert len(active) == 3


def test_load_reads_typed_rows_as_ontology_conventions(session: Session) -> None:
    ensure_conventions_seeded(session, "finance")
    conventions = {c.id: c for c in load_workspace_conventions(session, "finance")}
    assert set(conventions) == {
        "sign_natural_balance",
        "ledger_leg_netting",
        "balance_sheet_composition",
    }
    # NULL concept_groups reads back as {} (the OntologyConvention default), not None.
    assert conventions["ledger_leg_netting"].concept_groups == {}
    assert "extraction" in conventions["sign_natural_balance"].targets


def test_load_excludes_superseded_rows(session: Session) -> None:
    ensure_conventions_seeded(session, "finance")
    session.execute(
        update(Convention)
        .where(Convention.vertical == "finance", Convention.name == "ledger_leg_netting")
        .values(superseded_at=datetime.now(UTC))
    )
    session.flush()
    ids = {c.id for c in load_workspace_conventions(session, "finance")}
    assert "ledger_leg_netting" not in ids
    assert len(ids) == 2


def test_load_scoped_to_active_vertical(session: Session) -> None:
    """The read serves only the workspace's bound active vertical (DAT-848).

    A convention left under a DIFFERENT vertical (a wrong ``--vertical``) is present in
    the base table but NOT served; a reader threaded a mismatched vertical still gets the
    bound vertical's conventions.
    """
    _bind_vertical(session, "finance")
    ensure_conventions_seeded(session, "finance")
    session.add(Convention(vertical="marketing", name="foreign", statement="x", source="seed"))
    session.flush()
    # Threaded a mismatched 'marketing', but the bound vertical (finance) wins.
    ids = {c.id for c in load_workspace_conventions(session, "marketing")}
    assert "foreign" not in ids
    assert "sign_natural_balance" in ids


def test_frame_authored_convention_reaches_validation_prompt(session: Session) -> None:
    """The asymmetry DAT-789 fixes: a frame convention reaches the validation prompt.

    Before, the validation phase read the raw vertical YAML — a ``frame`` convention (a
    DB row that never exists in the YAML) could NEVER reach a validation prompt. Now
    validation reads the typed ``conventions`` home (via ``load_workspace_conventions``),
    so a frame row targeting ``validation:<id>`` is rendered. This test builds the exact
    ``OntologyDefinition`` the validation phase now constructs and asserts the frame rule
    routes to the spec — while a YAML-only reader stays blind to it.
    """
    # A frame-authored convention that exists ONLY as a DB row (never in the YAML).
    session.add(
        Convention(
            vertical="finance",
            name="frame_only_rule",
            statement="FRAME RULE: apply the declared adjustment.",
            targets=["validation:my_check"],
            source="frame",
        )
    )
    session.flush()

    # The DAT-789 path: the phase constructs the definition off the DB conventions
    # (model_construct skips the concept↔group lint), then routes per spec.
    ontology = OntologyDefinition.model_construct(
        name="finance", conventions=load_workspace_conventions(session, "finance")
    )
    rendered = OntologyLoader().format_conventions_for_prompt(
        ontology, "validation", qualifier="my_check"
    )
    assert "FRAME RULE: apply the declared adjustment." in rendered

    # The OLD path (raw YAML) never saw it — the shipped finance YAML declares no
    # `my_check` convention, so a YAML-only reader renders nothing. The asymmetry removed.
    yaml_only = OntologyLoader().format_conventions_for_prompt(
        OntologyLoader().load("finance"), "validation", qualifier="my_check"
    )
    assert "FRAME RULE" not in yaml_only
