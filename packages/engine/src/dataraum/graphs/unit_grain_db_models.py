"""SQLAlchemy model for a metric's per-entity breakdown (DAT-671 B1).

The durable form of the unit-grain composition (logic in
:mod:`dataraum.graphs.unit_grain`): one row per entity of the axis a metric was
broken down on, written by the operating_model ``metrics`` phase once the served
per-(target × axis) additivity verdict PERMITS the breakdown. Modelled on
``metric_axis_additivity`` — same target vocabulary, same run-versioning, same
upsert contract — because it is the same drill target seen one level down.

**Rows exist only where the verdict allowed them.** A target the served verdict
withholds writes NO rows and the phase discloses why; an empty row-set therefore
means "not offered", and the reason lives in the phase output rather than in a
half-filled row here. That is why there is no ``offered`` column: a row IS the
offer.

**``value`` is NULLABLE and NULL means "not computable for this entity".** It is
the FULL OUTER case — an entity carried by one operand and not the other (a
vendor with a payable but no purchases in the window), kept in the breakdown
because it belongs there, with an arithmetic result that genuinely does not
exist. It must NEVER be coalesced to 0: a zero is a measurement, and asserting
one nobody made would still sum to the right total, so nothing downstream could
catch it. An entity absent from the relation altogether is absent from this
table entirely — a different, equally honest, statement.

``reconciles`` / ``recompute`` ride every row from the gate's decision, so a
consumer rendering the breakdown knows without a second lookup whether the parts
sum to the workspace total (else: the honest dash) and whether each part was
RECOMPUTED from its carriers rather than summed.

Run-versioned like the verdicts it is gated by: the version axis is the
operating_model ``run_id``, current once that run is promoted under the
``(catalog, "operating_model")`` head (``current_metric_unit_grain``, DAT-506
read-view machinery). The
``(target_kind, target_key, axis, entity_value, run_id)`` UNIQUE is the
run-grain contract's form-(a) upsert key (ADR-0010).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import Boolean, CheckConstraint, DateTime, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base

#: Drill-target vocabulary, identical to ``metric_axis_additivity``'s — a
#: breakdown is keyed by the same target its verdict is (DAT-859 template:
#: the CHECK is derived from the one tuple so DB and code cannot drift).
_TARGET_KIND_VALUES: tuple[str, ...] = ("measure", "metric")


class MetricUnitGrain(Base):
    """One entity's value for one drill target on one axis, run-versioned.

    ``target_kind='metric'`` keys by ``graph_id`` and ``target_kind='measure'``
    by ``standard_field``, exactly as the verdict table does — the breakdown and
    the verdict that gated it resolve on the same key.

    ``axis`` is a served column name (a curated categorical slice), never a
    column chosen because its name looks like an id — or, for a CROSS-FACT
    drill-across (DAT-809), the ``bus_matrix.conformed_group`` identity. The
    second case has no single column name to record: the whole point of a
    drill-across is that each fact realizes one conformed dimension with its own
    column (``account_id`` here, ``acct`` there), so the identity is the only
    name that denotes the axis on every carrier. It is also stable where a label
    is not (DAT-800), which matters because this column is part of the UNIQUE key
    below. Human-facing surfaces render the concept label instead.

    ``entity_value`` is one distinct value of that column, rendered as text: the
    axis is categorical, and a breakdown label is read, not computed.
    """

    __tablename__ = "metric_unit_grain"
    __table_args__ = (
        UniqueConstraint(
            "target_kind",
            "target_key",
            "axis",
            "entity_value",
            "run_id",
            name="uq_metric_unit_grain_entity",
        ),
        CheckConstraint(
            "target_kind IN (" + ", ".join(f"'{v}'" for v in _TARGET_KIND_VALUES) + ")",
            name="target_kind",
        ),
    )

    unit_grain_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Snapshot version axis (DAT-413): the operating_model run that composed this.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    target_kind: Mapped[str] = mapped_column(String, nullable=False)  # 'metric' | 'measure'
    target_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    #: The served categorical column the target was broken down on.
    axis: Mapped[str] = mapped_column(String, nullable=False)
    #: One distinct value of ``axis``. NOT NULL: a row whose entity cannot be
    #: named is not a breakdown row, and the composer refuses the whole
    #: breakdown rather than publish one (or invent a label for it).
    entity_value: Mapped[str] = mapped_column(String, nullable=False)
    #: NULL = "not computable for this entity" — never zero. See the module note.
    value: Mapped[Decimal | None] = mapped_column(Numeric)
    #: Do these parts sum to the workspace total? Straight off the served verdict.
    reconciles: Mapped[bool] = mapped_column(Boolean, nullable=False)
    #: Was each part RECOMPUTED from its carriers rather than summed (a ratio, an
    #: average, a distinct count)?
    recompute: Mapped[bool] = mapped_column(Boolean, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
