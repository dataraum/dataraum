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
from dataraum.analysis.validation.db_models import InducedValidation, Validation
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


def _staged_row_values(run_id: str, vertical: str, spec: ValidationSpec) -> dict[str, Any]:
    """An :class:`InducedValidation` staging-row dict (row_id/created_at defaulted).

    The vocabulary home's ``source`` axis is absent by construction: every staged row
    is a 'generated' proposal, and it only acquires that label when the promote
    materializes it.
    """
    values = _row_values(vertical, spec, source="generated")
    del values["source"]
    return {"run_id": run_id, **values}


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

    ADR-0010 default writer form: ``(validation_id, run_id)`` UNIQUE + ON CONFLICT
    upsert, so a Temporal activity retry of the induction phase re-stages the same
    run in place rather than colliding. Returns the number of rows written.
    """
    rows = [_staged_row_values(run_id, vertical, spec) for spec in specs]
    upsert(session, InducedValidation, rows, index_elements=["validation_id", "run_id"])
    logger.info("induced_validations_staged", vertical=vertical, run_id=run_id, staged=len(rows))
    return len(rows)


def materialize_induced_validations(session: Session, run_id: str) -> int:
    """Land a promoted run's staged generation into the live vocabulary (DAT-877).

    Called by ``promote_operating_model_run`` in the SAME transaction as the
    ``(catalog, "operating_model")`` head flip, so the validation vocabulary, the
    executed results and the detected cycles all become current at one instant —
    a head-resolved reader can never observe a generation without its evidence.

    Supersede-then-insert, exactly the semantics the induction-time writer had:
    the prior active generated rows for the staged verticals are stamped
    ``superseded_at``, then the run's staged set is inserted via ``INSERT … ON
    CONFLICT DO NOTHING`` on the active-row index — a generated proposal colliding
    with an active SEED row is skipped (the shipped validation wins). Seed rows are
    untouched. A run that staged nothing supersedes nothing: an induction that
    degraded to zero proposals must not silently empty the vocabulary.

    Returns the number of generated rows actually inserted (skipped collisions
    excluded).
    """
    staged = list(
        session.execute(
            select(InducedValidation)
            .where(InducedValidation.run_id == run_id)
            .order_by(InducedValidation.validation_id)
        ).scalars()
    )
    if not staged:
        return 0

    verticals = sorted({row.vertical for row in staged})
    session.execute(
        update(Validation)
        .where(
            Validation.vertical.in_(verticals),
            Validation.source == "generated",
            Validation.superseded_at.is_(None),
        )
        .values(superseded_at=datetime.now(UTC))
    )
    rows = [_row_values(row.vertical, _staged_to_spec(row), source="generated") for row in staged]
    inserted = insert_if_absent(
        session,
        Validation,
        rows,
        index_elements=["vertical", "validation_id"],
        index_where=text("superseded_at IS NULL"),
    )
    logger.info(
        "induced_validations_materialized",
        run_id=run_id,
        verticals=verticals,
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
