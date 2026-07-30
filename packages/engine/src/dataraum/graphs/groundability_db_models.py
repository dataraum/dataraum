"""SQLAlchemy model for the per-column ungroundable-dimension verdict (DAT-620).

The durable form of the groundability verdicts computed at the operating_model
``metrics`` phase (logic in :mod:`dataraum.graphs.groundability`): for each
typed discriminator column a declared metric's grounding path depends on, one
run-versioned row saying whether a filter over its values can be grounded
honestly (``groundable``), cannot until a resolving reference/lookup table is
linked (``ungroundable``), or could not be judged (a typed abstention). Rows
are written for EVERY evaluated dependency column, so on the read surface
absence means "not a dependency of any declared metric this run" — never
"not judged".

**Grain: one row per (column, run).** The column is the cross-run-stable typed
identity (``columns.column_id``); ``column_name`` / ``table_name`` ride along
as of-this-run provenance so the read surface names the column without a join.
Run-versioned like its twin :class:`~dataraum.graphs.additivity_db_models.MetricAxisAdditivity`
— the version axis is the operating_model ``run_id``, current once that run is
promoted under the ``(catalog, "operating_model")`` head
(``current_dimension_groundability``, DAT-506 read-view machinery). Recomputed
every run from the run's grounding set — never frozen, so linking the resolving
reference table clears the verdict on the next run's rows (the ticket's
no-false-abstention criterion). The ``(column_id, run_id)`` UNIQUE is the
run-grain contract's form-(a) upsert key (ADR-0010).

``vertical`` is PROVENANCE, not identity (the additivity precedent): one run
has one vertical, so ``run_id`` already pins it; it records which vertical's
declared metrics defined the dependency set this row was evaluated under.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.graphs.groundability import (
    GROUNDABLE_REASONS,
    GroundabilityAbstainReason,
    GroundabilityReason,
    GroundabilityStatus,
    GroundabilityVerdict,
)
from dataraum.storage import Base

# Vocabularies derived from the enums so the DB and the code cannot drift apart
# (the DAT-859 template, via additivity_db_models).
_STATUS_VALUES = tuple(sorted(v.value for v in GroundabilityStatus))
_VERDICT_VALUES = tuple(sorted(v.value for v in GroundabilityVerdict))
_REASON_VALUES = tuple(sorted(v.value for v in GroundabilityReason))
_ABSTAIN_REASON_VALUES = tuple(sorted(v.value for v in GroundabilityAbstainReason))
_GROUNDABLE_REASON_VALUES = tuple(sorted(GROUNDABLE_REASONS))


def _in_list(column: str, values: tuple[str, ...], *, nullable: bool) -> str:
    rendered = ", ".join(f"'{v}'" for v in values)
    prefix = f"{column} IS NULL OR " if nullable else ""
    return f"{prefix}{column} IN ({rendered})"


class DimensionGroundability(Base):
    """One discriminator column's groundability verdict, run-versioned (DAT-620).

    ``status`` is the DAT-859 pairing: a CLASSIFIED row carries a ``verdict``
    AND its ``reason`` (``ungroundable`` always pairs with the trigger reason
    ``no_resolving_reference``; ``groundable`` names the first failing leg);
    an ABSTAINED row carries only an ``abstain_reason``. There is no third
    state and no NULL-means-something encoding — "we did not judge this" is a
    row that says so.
    """

    __tablename__ = "dimension_groundability"
    __table_args__ = (
        UniqueConstraint("column_id", "run_id", name="uq_dimension_groundability_column_run"),
        CheckConstraint(_in_list("status", _STATUS_VALUES, nullable=False), name="status"),
        CheckConstraint(_in_list("verdict", _VERDICT_VALUES, nullable=True), name="verdict"),
        CheckConstraint(_in_list("reason", _REASON_VALUES, nullable=True), name="reason"),
        CheckConstraint(
            _in_list("abstain_reason", _ABSTAIN_REASON_VALUES, nullable=True),
            name="abstain_reason",
        ),
        # The status/verdict/reason pairing, repeated from the enum chokepoint so
        # a hand-written row cannot encode an impossible state: `ungroundable`
        # carries exactly the trigger reason (all three legs held; the reason
        # names the cure), `groundable` exactly one of the leg-fail reasons.
        CheckConstraint(
            "(status = 'classified' AND verdict IS NOT NULL AND reason IS NOT NULL"
            " AND abstain_reason IS NULL"
            " AND ((verdict = 'ungroundable' AND reason = 'no_resolving_reference')"
            " OR (verdict = 'groundable' AND reason IN ("
            + ", ".join(f"'{v}'" for v in _GROUNDABLE_REASON_VALUES)
            + "))))"
            " OR (status = 'abstained' AND verdict IS NULL AND reason IS NULL"
            " AND abstain_reason IS NOT NULL)",
            name="status_verdict_reason",
        ),
    )

    groundability_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Snapshot version axis (DAT-413): the operating_model run that computed this.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    #: Provenance, not identity — see the module note.
    vertical: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # The typed discriminator column — the cross-run-stable identity.
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id"), nullable=False, index=True
    )
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    #: As-of-this-run names, so the read surface names the column without a join.
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    table_name: Mapped[str] = mapped_column(String, nullable=False)

    status: Mapped[str] = mapped_column(String, nullable=False)
    verdict: Mapped[str | None] = mapped_column(String)
    reason: Mapped[str | None] = mapped_column(String)
    abstain_reason: Mapped[str | None] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
