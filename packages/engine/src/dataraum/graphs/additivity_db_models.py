"""SQLAlchemy model for the per-(target × axis) additivity verdict (DAT-857/868).

The durable form of the drill's additivity verdicts, computed at the
operating_model ``metrics`` phase (logic in :mod:`dataraum.graphs.additivity`).
The drill reads ``current_metric_axis_additivity`` to decide, per axis, whether
to offer a time bucketing, how to compose it, and whether a breakdown
*reconciles* (sums to the total) or shows the honest dash.

A drill **target** is either kind of canvas node (a measure is also a metric):

* ``target_kind='metric'`` — a formula metric; ``target_key`` is its
  ``lifecycle_artifacts.artifact_key`` (``graph_id``), drilled via ``{metricKey}``.
* ``target_kind='measure'`` — a grounded measure/extract; ``target_key`` is its
  ``standard_field``, drilled via ``{standardField}``. Its verdict is the
  measure's single extract classified directly (no formula roll-up).

**Grain: one row per (target, axis).** ``axis_kind`` names the axis class and
``axis_key`` the concrete axis — a served column name — with the sentinel ``'*'``
meaning "every axis of this kind", the CLASS-level verdict. Class rows are
written for every target unconditionally, so a consumer ALWAYS resolves to a row
(verdict or typed abstention) and can never mistake "not judged" for "no"; a
concrete ``axis_key`` row REFINES the class verdict where the substrate says
something axis-specific, and a consumer resolves most-specific-first. The old
two-boolean verdict is exactly the pair of class rows, so it stays reproducible
(DAT-857's no-fork constraint).

The sentinel is a non-NULL string on purpose: a NULLable key column would make
the ``ON CONFLICT`` upsert inference NULLS-DISTINCT, so every class row would
duplicate on each re-run instead of updating.

Run-versioned like the ``lifecycle_artifacts`` it derives from: the version axis
is the operating_model ``run_id``, current once that run is promoted under the
``(catalog, "operating_model")`` head (``current_metric_axis_additivity``,
DAT-506 read-view machinery). Recomputed every session cascade from the run's
live materialization evidence — never frozen. The
``(target_kind, target_key, axis_kind, axis_key, run_id)`` UNIQUE is the
run-grain contract's form-(a) upsert key (ADR-0010).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import CheckConstraint, DateTime, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.graphs.additivity import (
    CLASSIFIED_REASONS,
    AbstainReason,
    AdditivityStatus,
    AxisKind,
    AxisVerdict,
)
from dataraum.storage import Base

#: ``axis_key`` value meaning "every axis of this kind" — see the module note.
#:
#: HAND-MIRRORED cross-package (the worker/contracts.py discipline). The sentinel
#: is a SERVED VALUE: the cockpit reads these rows straight off
#: ``current_metric_axis_additivity`` and must recognise the class row to resolve
#: most-specific-first, so it keeps its own copy in
#: ``packages/cockpit/src/tools/concept-graph.ts`` (exported ``AXIS_KEY_ALL``) and
#: ``packages/cockpit/src/tools/drill-axes.ts``. A drift here silently turns every
#: class-level verdict into an unrecognised concrete axis — the drill would then
#: find no verdict for an axis it has one for. Pinned from this side by
#: ``tests/unit/graphs/test_hand_mirrored_constants.py``, from the cockpit side by
#: ``concept-graph-load.integration.test.ts`` against real engine-served rows.
AXIS_KEY_ALL = "*"

#: Bucket grains a time axis can be served at, coarsest-last. Mirrors the
#: ``og_period_grain`` ladder (DAT-730) — the vocabulary a consumer walks up.
BUCKET_GRAINS: tuple[str, ...] = ("day", "month", "quarter", "year")

# Vocabularies derived from the enums so the DB and the code cannot drift apart
# (the DAT-859 template).
_TARGET_KIND_VALUES: tuple[str, ...] = ("measure", "metric")
_AXIS_KIND_VALUES = tuple(sorted(v.value for v in AxisKind))
_STATUS_VALUES = tuple(sorted(v.value for v in AdditivityStatus))
_VERDICT_VALUES = tuple(sorted(v.value for v in AxisVerdict))
_ABSTAIN_REASON_VALUES = tuple(sorted(v.value for v in AbstainReason))
_REASON_VALUES = tuple(sorted(CLASSIFIED_REASONS))


def _in_list(column: str, values: tuple[str, ...], *, nullable: bool) -> str:
    rendered = ", ".join(f"'{v}'" for v in values)
    prefix = f"{column} IS NULL OR " if nullable else ""
    return f"{prefix}{column} IN ({rendered})"


class MetricAxisAdditivity(Base):
    """One (drill target × axis) additivity verdict, run-versioned (DAT-857/868).

    ``status`` is the DAT-859 pairing: a CLASSIFIED row carries a ``verdict``
    (and, when that verdict is not ``additive``, the doctrine ``reason`` naming
    why it does not sum); an ABSTAINED row carries only an ``abstain_reason``.
    There is no third state and no NULL-means-something encoding — "we did not
    judge this" is a row that says so.

    ``bucket_grain`` is the axis's observed cadence (time axes only): the finest
    bucket the data actually supports, off the temporal profile. A consumer
    offers that grain and coarser, and never a finer one that would render one
    row per empty period.
    """

    __tablename__ = "metric_axis_additivity"
    __table_args__ = (
        UniqueConstraint(
            "target_kind",
            "target_key",
            "axis_kind",
            "axis_key",
            "run_id",
            name="uq_metric_axis_additivity_target",
        ),
        CheckConstraint(
            _in_list("target_kind", _TARGET_KIND_VALUES, nullable=False), name="target_kind"
        ),
        CheckConstraint(_in_list("axis_kind", _AXIS_KIND_VALUES, nullable=False), name="axis_kind"),
        CheckConstraint(_in_list("status", _STATUS_VALUES, nullable=False), name="status"),
        CheckConstraint(_in_list("verdict", _VERDICT_VALUES, nullable=True), name="verdict"),
        CheckConstraint(_in_list("reason", _REASON_VALUES, nullable=True), name="reason"),
        CheckConstraint(
            _in_list("abstain_reason", _ABSTAIN_REASON_VALUES, nullable=True), name="abstain_reason"
        ),
        CheckConstraint(
            _in_list("bucket_grain", BUCKET_GRAINS, nullable=True), name="bucket_grain"
        ),
        # The status/verdict/reason pairing, repeated from the dataclass
        # chokepoint so a hand-written row cannot encode an impossible state.
        # `additive` reconciles, so it has no reason NOT to; every other verdict
        # must name its doctrine reason.
        CheckConstraint(
            "(status = 'classified' AND verdict IS NOT NULL AND abstain_reason IS NULL"
            " AND ((verdict = 'additive' AND reason IS NULL)"
            " OR (verdict <> 'additive' AND reason IS NOT NULL)))"
            " OR (status = 'abstained' AND verdict IS NULL AND reason IS NULL"
            " AND abstain_reason IS NOT NULL)",
            name="status_verdict_reason",
        ),
        # A cadence is a property of a time axis; a categorical axis has none.
        CheckConstraint(
            "axis_kind = 'time' OR bucket_grain IS NULL", name="bucket_grain_time_axis_only"
        ),
    )

    additivity_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Snapshot version axis (DAT-413): the operating_model run that computed this.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    #: The vocabulary this verdict was computed against. NOT part of the identity
    #: (one run has one vertical, so ``run_id`` already pins it) — it is the row's
    #: PROVENANCE, and it exists because ``target_key`` is only HALF of a concept's
    #: stable ``(vertical, name)`` identity. ``og_has_additivity`` joins on the full
    #: pair, so a verdict computed under the previous vertical cannot bind to a
    #: same-named concept of the newly-framed one in the window between a vertical
    #: change and the next operating_model promotion. Mirrors its own twin,
    #: :class:`~dataraum.analysis.semantic.reconciliation_db_models.ConceptReconciliation`,
    #: written by the same phase in the same run.
    vertical: Mapped[str] = mapped_column(String, nullable=False, index=True)
    target_kind: Mapped[str] = mapped_column(String, nullable=False)  # 'metric' | 'measure'
    target_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    axis_kind: Mapped[str] = mapped_column(String, nullable=False)  # 'time' | 'categorical'
    #: A served column name, or ``'*'`` for the class-level verdict.
    axis_key: Mapped[str] = mapped_column(String, nullable=False)

    status: Mapped[str] = mapped_column(
        String, nullable=False, default=AdditivityStatus.CLASSIFIED.value
    )
    verdict: Mapped[str | None] = mapped_column(String)
    reason: Mapped[str | None] = mapped_column(String)
    abstain_reason: Mapped[str | None] = mapped_column(String)
    bucket_grain: Mapped[str | None] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
