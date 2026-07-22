"""Seed + read the validation typed home (DAT-735).

The runtime home for a workspace's validation vocabulary. The shipped vertical
YAML is the *seed* (source='seed'), normalized into ``validations`` rows once per
workspace; agentic induction (:mod:`dataraum.analysis.validation.induction`)
proposes more rows over the served graph (source='generated'). The validation
phase reads these typed rows (never the YAML directory walk), ``⊕`` the
``validation`` teach overlay applied at read time — so a *framed* vertical whose
validations exist only as rows is served identically to a builtin.

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
from dataraum.analysis.validation.db_models import Validation
from dataraum.analysis.validation.models import ValidationSpec
from dataraum.core.logging import get_logger
from dataraum.core.vertical_loader import Family, VerticalLoader
from dataraum.storage.upsert import insert_if_absent

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
        "tags": spec.tags or None,
        "version": spec.version,
        "source": source,
    }


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


def persist_generated_validations(
    session: Session, vertical: str, specs: list[ValidationSpec]
) -> int:
    """Persist an induced validation set as ``source='generated'`` rows (DAT-735).

    Re-induction SUPERSEDES, never duplicates: the prior active generated rows for
    this vertical are stamped ``superseded_at`` in one statement, then the fresh set
    is inserted via ``INSERT … ON CONFLICT DO NOTHING`` on the active-row index — so a
    generated proposal that collides with an active SEED row is skipped (the shipped
    validation wins; a generated duplicate is redundant), and re-running the induction
    converges in place rather than piling up history. The seed rows are untouched.

    Returns the number of generated rows actually inserted (skipped collisions
    excluded).
    """
    session.execute(
        update(Validation)
        .where(
            Validation.vertical == vertical,
            Validation.source == "generated",
            Validation.superseded_at.is_(None),
        )
        .values(superseded_at=datetime.now(UTC))
    )
    rows = [_row_values(vertical, spec, source="generated") for spec in specs]
    if not rows:
        return 0
    inserted = insert_if_absent(
        session,
        Validation,
        rows,
        index_elements=["vertical", "validation_id"],
        index_where=text("superseded_at IS NULL"),
    )
    skipped = len(rows) - inserted
    logger.info(
        "generated_validations_persisted",
        vertical=vertical,
        inserted=inserted,
        skipped_collisions=skipped,
    )
    return inserted


def load_workspace_validations(session: Session, vertical: str) -> list[ValidationSpec]:
    """The workspace's active validations as typed :class:`ValidationSpec` objects.

    Reads the active ``validations`` rows (seed ``⊕`` generated — both are rows in this
    table) as the config→DB home. **Scoped to the workspace's bound active vertical
    (DAT-848),** exactly like ``load_workspace_conventions``: the read filters on
    ``workspace_settings.active_vertical`` (never blindly on the caller's ``vertical``),
    with ``vertical`` the fallback for an UNBOUND workspace. The teach overlay is
    layered on top by the caller (``config.load_all_validation_specs``), NOT here.

    Ordered by ``validation_id`` for a deterministic declared set.
    """
    effective = _active_vertical(session) or vertical
    rows = session.execute(
        select(Validation)
        .where(Validation.vertical == effective, Validation.superseded_at.is_(None))
        .order_by(Validation.validation_id)
    ).scalars()
    return [_row_to_spec(row) for row in rows]


__all__ = [
    "ensure_validations_seeded",
    "persist_generated_validations",
    "load_workspace_validations",
]
