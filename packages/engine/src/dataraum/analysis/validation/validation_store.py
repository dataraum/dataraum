"""Seed + read the validation typed home (DAT-735).

The runtime home for a workspace's validation vocabulary. The shipped vertical
YAML is the *seed* (source='seed'), normalized into ``validations`` rows once per
workspace; agentic induction (:mod:`dataraum.analysis.validation.induction`)
proposes more rows over the served graph (source='generated'). The validation
phase reads these typed rows (never the YAML directory walk), ``⊕`` the
``validation`` teach overlay applied at read time — so a *framed* vertical whose
validations exist only as rows is served identically to a builtin.

**Induction is staged, not published (DAT-877).** An induction run writes its
proposals to the run-versioned ``induced_validations`` table
(:func:`stage_induced_validations`); only the terminal operating_model promote
lands them in the vocabulary home (:func:`materialize_induced_validations`), in
the same transaction as the head flip. That is what keeps the vocabulary, the
executed results and the detected cycles becoming current together: writing
straight to the home published a generation minutes before its evidence existed,
and permanently so if the run never promoted. The in-flight run reads its own
staged set via ``load_workspace_validations(run_id=…)``.

The DAT-789 ``convention_store`` pattern applied to validation specs: the check
LOGIC (``check_type`` + ``tolerance``) gets a typed home instead of living as free
``sql_hints`` text. The teach overlay stays a SEPARATE layer (it is NOT a
``source`` here) — the DAT-802 live-writer discipline admits only sources this
module writes: 'seed' and 'generated'.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, text, update

from dataraum.analysis.semantic.db_models import WorkspaceSettings
from dataraum.analysis.validation.db_models import (
    InducedValidation,
    InductionRun,
    Validation,
)
from dataraum.analysis.validation.models import ValidationSpec
from dataraum.core.logging import get_logger
from dataraum.core.vertical_loader import Family, VerticalLoader
from dataraum.storage.upsert import insert_if_absent, upsert

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def _active_vertical(session: Session) -> str | None:
    """The workspace's bound active vertical, or ``None`` if none is bound yet.

    Reads the single ``workspace_settings`` row (the ``pin`` CHECK keeps it at most
    one). Mirrors ``convention_store._active_vertical`` — the same DAT-848 binding.
    """
    return session.execute(select(WorkspaceSettings.active_vertical)).scalar_one_or_none()


def _row_values(vertical: str, spec: ValidationSpec, *, source: str) -> dict[str, Any]:
    """A :class:`Validation` row dict from a spec (row_id/created_at defaulted)."""
    return {
        "vertical": vertical,
        "validation_id": spec.validation_id,
        "name": spec.name,
        "description": spec.description,
        "category": spec.category,
        "severity": spec.severity.value,
        "check_type": spec.check_type,
        "tolerance": spec.tolerance,
        "guidance": spec.guidance,
        "expected_outcome": spec.expected_outcome,
        "relevant_cycles": spec.relevant_cycles or None,
        "relevant_conventions": spec.relevant_conventions or None,
        "tags": spec.tags or None,
        "version": spec.version,
        "source": source,
    }


# The columns an :class:`InducedValidation` staging row carries over to a
# :class:`Validation` row verbatim — the two models are column-identical apart from
# the staging ``run_id`` and the vocabulary's ``source``/``superseded_at`` lifecycle
# axes. Named explicitly (not derived by deleting keys from the vocabulary dict) so
# the day one model gains a column the other lacks is a loud failure, not silent drift.
_SPEC_COLUMNS: tuple[str, ...] = (
    "validation_id",
    "name",
    "description",
    "category",
    "severity",
    "check_type",
    "tolerance",
    "guidance",
    "expected_outcome",
    "relevant_cycles",
    "relevant_conventions",
    "tags",
    "version",
)


def _staged_row_values(run_id: str, vertical: str, spec: ValidationSpec) -> dict[str, Any]:
    """An :class:`InducedValidation` staging-row dict (row_id/created_at defaulted).

    The vocabulary home's ``source`` axis is absent by construction: every staged row
    is a 'generated' proposal, and it only acquires that label when the promote
    materializes it.
    """
    return {
        "run_id": run_id,
        "vertical": vertical,
        "validation_id": spec.validation_id,
        "name": spec.name,
        "description": spec.description,
        "category": spec.category,
        "severity": spec.severity.value,
        "check_type": spec.check_type,
        "tolerance": spec.tolerance,
        "guidance": spec.guidance,
        "expected_outcome": spec.expected_outcome,
        "relevant_cycles": spec.relevant_cycles or None,
        "relevant_conventions": spec.relevant_conventions or None,
        "tags": spec.tags or None,
        "version": spec.version,
    }


def _staged_to_validation_values(row: InducedValidation) -> dict[str, Any]:
    """A :class:`Validation` row dict straight off a staged row's columns.

    Deliberately NOT a ``ValidationSpec`` round-trip: this runs inside the promote's
    transaction, where a Pydantic ValidationError would be a deterministic raise that
    Temporal retries five times and then fails the whole operating_model run — after
    the entire spine has already succeeded. The staged row was validated on the way
    IN, and the DB CHECKs mirror the vocabulary's, so copying columns is both safer
    and honest about where the contract is enforced.
    """
    values: dict[str, Any] = {name: getattr(row, name) for name in _SPEC_COLUMNS}
    values["vertical"] = row.vertical
    values["source"] = "generated"
    return values


def _staged_to_spec(row: InducedValidation) -> ValidationSpec:
    """A :class:`ValidationSpec` from a staged row — always ``source='generated'``."""
    return ValidationSpec(
        validation_id=row.validation_id,
        name=row.name,
        description=row.description,
        category=row.category,
        severity=row.severity,  # type: ignore[arg-type]  # StrEnum coerces the str
        check_type=row.check_type,
        tolerance=row.tolerance,
        guidance=row.guidance,
        expected_outcome=row.expected_outcome,
        tags=list(row.tags or []),
        relevant_cycles=list(row.relevant_cycles or []),
        relevant_conventions=list(row.relevant_conventions or []),
        version=row.version,
        source="generated",
    )


def _row_to_spec(row: Validation) -> ValidationSpec:
    """A :class:`ValidationSpec` from a typed row (no legacy normalization needed)."""
    return ValidationSpec(
        validation_id=row.validation_id,
        name=row.name,
        description=row.description,
        category=row.category,
        severity=row.severity,  # type: ignore[arg-type]  # StrEnum coerces the str
        check_type=row.check_type,
        tolerance=row.tolerance,
        guidance=row.guidance,
        expected_outcome=row.expected_outcome,
        tags=list(row.tags or []),
        relevant_cycles=list(row.relevant_cycles or []),
        relevant_conventions=list(row.relevant_conventions or []),
        version=row.version,
        source=row.source or "config",
    )


def ensure_validations_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's validations as typed rows (DAT-735).

    Reads the vertical's SHIPPED validation YAML (the seed source, WITHOUT the teach
    overlay — that stays a read-time ``⊕`` layer) and inserts a typed
    :class:`Validation` row for every validation with no active row yet, via
    ``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique index — so a
    re-run is a no-op, a generated/frame supersede is never clobbered, and it is
    race-safe against a concurrent seed. Mirrors ``ensure_conventions_seeded``.

    Each YAML doc is re-typed through :class:`ValidationSpec` (the ``mode="before"``
    normalizer maps the legacy ``parameters``/``sql_hints`` shape onto the typed
    ``tolerance``/``guidance`` fields), so the seed rows carry the typed check
    definition. A framed vertical (no on-disk YAML) seeds nothing. Returns the number
    of rows actually inserted (conflicts skipped).

    **Per-doc fault isolation** (the ``ensure_metrics_seeded`` pattern): each doc is
    parsed AND written on its own, inside its OWN ``begin_nested`` savepoint, so one
    malformed doc rolls back only THAT row — never the whole batch, and never the
    concept/convention/edge/metric seeds this phase already wrote to the same
    uncommitted session. One bad validation must not non-retryably fail the add_source
    grounding phase (a permanent failure Temporal would retry forever).
    """
    base = VerticalLoader(vertical).shipped_base(Family.VALIDATIONS)
    seeded = 0
    for doc in base.get("validations") or []:
        try:
            spec = ValidationSpec.model_validate(doc)
        except Exception as exc:  # noqa: BLE001 - one malformed doc must not sink the seed
            logger.warning("validation_seed_parse_skip", error=str(exc))
            continue
        try:
            with session.begin_nested():
                seeded += insert_if_absent(
                    session,
                    Validation,
                    [_row_values(vertical, spec, source="seed")],
                    index_elements=["vertical", "validation_id"],
                    index_where=text("superseded_at IS NULL"),
                )
        except Exception as exc:  # noqa: BLE001 - the savepoint rolled back only this row
            logger.warning("validation_seed_write_skip", error=str(exc))
            continue
    if seeded:
        logger.info("validations_seeded", vertical=vertical, count=seeded)
    return seeded


def stage_induced_validations(
    session: Session, run_id: str, vertical: str, specs: list[ValidationSpec]
) -> int:
    """Stage one induction run's proposed set, run-versioned (DAT-877).

    Writes into :class:`InducedValidation`, NOT the live vocabulary. The workspace's
    ``validations`` home is only flipped later, by
    :func:`materialize_induced_validations` inside the terminal operating_model
    promote — so an operating_model run that dies after induction (a non-retryable
    ``PhaseFailed`` downstream, the ``nothing_declared`` completion, a crash) leaves
    the previously promoted vocabulary intact instead of publishing a generation
    whose results and cycles will never exist.

    Called ONLY on the induction agent's success path, and it also writes the run's
    :class:`InductionRun` seal — that is what lets the promote tell an authoritative
    zero-proposal induction (supersede: the model retired the generation) from a
    degraded one that never staged (keep the prior generation). Without the seal a
    generated validation could never be retired once induced.

    ADR-0010 default writer form: ``(validation_id, run_id)`` UNIQUE + ON CONFLICT
    upsert, so a Temporal activity retry of the induction phase re-stages the same
    run in place rather than colliding. Returns the number of rows written.
    """
    upsert(
        session,
        InductionRun,
        [{"run_id": run_id, "vertical": vertical, "proposed": len(specs)}],
        index_elements=["run_id"],
    )
    rows = [_staged_row_values(run_id, vertical, spec) for spec in specs]
    upsert(session, InducedValidation, rows, index_elements=["validation_id", "run_id"])
    logger.info("induced_validations_staged", vertical=vertical, run_id=run_id, staged=len(rows))
    return len(rows)


def _generated_matches(active: list[Validation], rows: list[dict[str, Any]]) -> bool:
    """Is the live generated set already content-identical to the staged set?

    Compared on the spec columns only — ``row_id``/``created_at`` are per-write
    identity, not content, and comparing them would make every promote look changed.
    """
    if len(active) != len(rows):
        return False
    live = sorted(
        (tuple(getattr(row, name) for name in _SPEC_COLUMNS) for row in active),
        key=lambda item: str(item[0]),
    )
    incoming = sorted(
        (tuple(row[name] for name in _SPEC_COLUMNS) for row in rows),
        key=lambda item: str(item[0]),
    )
    return live == incoming


def materialize_induced_validations(session: Session, run_id: str) -> int:
    """Land a promoted run's staged generation into the live vocabulary (DAT-877).

    Called by ``promote_operating_model_run`` in the SAME transaction as the
    ``(catalog, "operating_model")`` head flip, so the validation vocabulary, the
    executed results and the detected cycles all become current at one instant —
    a head-resolved reader can never observe a generation without its evidence.

    Keyed on the run's :class:`InductionRun` SEAL, not on the presence of staged
    rows. No seal ⇒ the induction degraded or never ran ⇒ keep the prior generation
    untouched. A seal with zero staged rows is an authoritative empty proposal, and
    DOES supersede — otherwise a generated validation could never be retired.

    Supersede-then-insert, the semantics the induction-time writer had: the sealed
    vertical's active generated rows are stamped ``superseded_at``, then the run's
    staged set is inserted via ``INSERT … ON CONFLICT DO NOTHING`` on the active-row
    index — a generated proposal colliding with an active SEED row is skipped (the
    shipped validation wins). Seed rows are untouched.

    **Quiescent when nothing changed.** A committed-but-unacked promote is re-run by
    Temporal, and a blind re-supersede would retire the rows it just wrote and
    re-insert them under fresh ``row_id``s — phantom superseded generations piling up
    in plain sight on the ``__READ__.validations`` pass-through. So when the live
    generated set is already content-identical to the staged set, this is a no-op.

    Returns the number of live generated rows for the sealed vertical.
    """
    seal = session.execute(
        select(InductionRun).where(InductionRun.run_id == run_id)
    ).scalar_one_or_none()
    if seal is None:
        return 0

    staged = list(
        session.execute(
            select(InducedValidation)
            .where(InducedValidation.run_id == run_id)
            .order_by(InducedValidation.validation_id)
        ).scalars()
    )
    rows = [_staged_to_validation_values(row) for row in staged]

    active = list(
        session.execute(
            select(Validation).where(
                Validation.vertical == seal.vertical,
                Validation.source == "generated",
                Validation.superseded_at.is_(None),
            )
        ).scalars()
    )
    if _generated_matches(active, rows):
        logger.info("induced_validations_quiescent", run_id=run_id, live=len(active))
        return len(active)

    session.execute(
        update(Validation)
        .where(
            Validation.vertical == seal.vertical,
            Validation.source == "generated",
            Validation.superseded_at.is_(None),
        )
        .values(superseded_at=datetime.now(UTC))
    )
    inserted = (
        insert_if_absent(
            session,
            Validation,
            rows,
            index_elements=["vertical", "validation_id"],
            index_where=text("superseded_at IS NULL"),
        )
        if rows
        else 0
    )
    logger.info(
        "induced_validations_materialized",
        run_id=run_id,
        vertical=seal.vertical,
        inserted=inserted,
        skipped_collisions=len(rows) - inserted,
    )
    return inserted


def load_workspace_validations(
    session: Session, vertical: str, *, run_id: str | None = None
) -> list[ValidationSpec]:
    """The workspace's active validations as typed :class:`ValidationSpec` objects.

    Reads the active ``validations`` rows (seed ``⊕`` promoted generated) as the
    config→DB home. **Scoped to the workspace's bound active vertical (DAT-848),**
    exactly like ``load_workspace_conventions``: the read filters on
    ``workspace_settings.active_vertical`` (never blindly on the caller's
    ``vertical``), with ``vertical`` the fallback for an UNBOUND workspace. The teach
    overlay is layered on top by the caller (``config.load_all_validation_specs``),
    NOT here.

    ``run_id`` is the IN-RUN read (DAT-877): an operating_model run's own phases must
    see the generation their induction just staged, which is not in the vocabulary
    home until that run promotes. Head-resolved readers pass nothing and see only the
    promoted vocabulary.

    The in-run read REPLACES the generated layer rather than merging into it, so the
    set a run declares is exactly the set its promote will publish: a staged
    generation supersedes the prior one wholesale (a check this run's induction
    dropped must not still be declared), while a run that staged NOTHING — a degraded
    induction — keeps the previously promoted generation, matching
    :func:`materialize_induced_validations`. Seed rows are untouched by either.

    Ordered by ``validation_id`` for a deterministic declared set.
    """
    effective = _active_vertical(session) or vertical
    active = list(
        session.execute(
            select(Validation)
            .where(Validation.vertical == effective, Validation.superseded_at.is_(None))
            .order_by(Validation.validation_id)
        ).scalars()
    )
    staged = (
        list(
            session.execute(
                select(InducedValidation)
                .where(
                    InducedValidation.vertical == effective,
                    InducedValidation.run_id == run_id,
                )
                .order_by(InducedValidation.validation_id)
            ).scalars()
        )
        if run_id is not None
        else []
    )
    if not staged:
        return [_row_to_spec(row) for row in active]

    # Seed first: a staged proposal never displaces an active SEED row — the shipped
    # validation wins, the same precedence the materialization applies at promote.
    specs = {row.validation_id: _row_to_spec(row) for row in active if row.source == "seed"}
    for row in staged:
        specs.setdefault(row.validation_id, _staged_to_spec(row))
    return [specs[key] for key in sorted(specs)]


__all__ = [
    "ensure_validations_seeded",
    "stage_induced_validations",
    "materialize_induced_validations",
    "load_workspace_validations",
]
