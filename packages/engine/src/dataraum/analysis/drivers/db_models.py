"""SQLAlchemy model for the persisted driver-rankings artifact (DAT-546).

The pure driver-discovery engine (:mod:`dataraum.analysis.drivers.processor`,
DAT-545/561/563) returns an in-memory :class:`~dataraum.analysis.drivers.models.DriverRanking`
per measure. This is its durable form: one run-versioned row per
``(measure_column_id, run_id)``, written by the ``driver_rankings`` begin_session
phase. Persisting it (rather than recomputing on demand) is the engine's
pre-computed-context thesis — expensive numpy/permutation work, computed once,
read many times by the answer agent (``look_drivers``).

Run-versioned like ``MeasureAggregationLineage``: the version axis is the
begin_session ``run_id``; a row becomes current once that run is promoted under
the workspace catalog head (``current_driver_rankings``, DAT-506 read-view
machinery). The grain-labeled findings (DAT-563) persist GRANULARLY — the
primary family's ``grain``/``entity`` plus the ``secondary_dimensions`` list of
non-primary grain families — never merged into one cross-grain ranking; the
grains are not comparable and downstream consumers decide how to combine them.

The ``status``/``abstain_reason`` pair (DAT-859) carries a two-layer contract:
:class:`~dataraum.analysis.drivers.models.DriverRanking`'s ``__post_init__`` is the
writer-side chokepoint (layer 1); the CHECK constraints here — derived from the
same ``RankingStatus``/``AbstainReason`` enums so the two can never drift — are the
DB-enforced backstop (the DAT-781 two-layer standard, copying the precedent at
``analysis/hierarchies/db_models.py``'s ``role_verdict``, plus the paired
status/reason CHECK precedent at ``entropy/db_models.py``'s ``status_score_reason``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.analysis.drivers.models import AbstainReason, RankingStatus
from dataraum.storage import Base

# The closed vocabularies, derived from the single-home enums (DAT-859, the
# DAT-781 two-layer standard) — sorted for a deterministic CHECK string in the
# offline DDL dump.
_RANKING_STATUS_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in RankingStatus))
_ABSTAIN_REASON_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in AbstainReason))


class DriverRankingArtifact(Base):
    """One measure's driver ranking, run-versioned (DAT-546).

    Keyed ``(measure_column_id, run_id)``: the measure is a ``semantic_role='measure'``
    fact column, so its ``column_id`` is the stable artifact id. The grain-labeled
    output is stored faithfully to :class:`DriverRanking` — ``grain``/``entity`` name
    the primary family's exchangeable unit (row, or which identity's entity grain),
    ``n_rows`` is the effective sample size (so a "no significant driver" result is
    honestly attributable), and ``secondary_dimensions`` carries every non-primary
    grain family's significant dims as a flat labeled list.
    """

    __tablename__ = "driver_rankings"
    __table_args__ = (
        UniqueConstraint("measure_column_id", "run_id", name="uq_driver_rankings_column_run"),
        CheckConstraint(
            "status IN (" + ", ".join(f"'{v}'" for v in _RANKING_STATUS_VALUES) + ")",
            name="status",
        ),
        CheckConstraint(
            "abstain_reason IS NULL OR abstain_reason IN ("
            + ", ".join(f"'{v}'" for v in _ABSTAIN_REASON_VALUES)
            + ")",
            name="abstain_reason",
        ),
        # The pairing (DAT-859, mirroring entropy/db_models.py's
        # status_score_reason IN FULL, including its measured-side value guard): a
        # measured row carries no reason and MUST know its target_type (a Measure's
        # own __post_init__ never admits a blank one); an abstained row always
        # carries a reason. ``target_type`` is NOT required on the abstained side —
        # an abstained ranking may still know its target_type (the 3 processor.py
        # honest-empty sites do; only the unresolved-temporal_behavior abstention at
        # persistence.py does not), so its nullability there is independent of status.
        CheckConstraint(
            "(status = 'measured' AND abstain_reason IS NULL AND target_type IS NOT NULL)"
            " OR (status = 'abstained' AND abstain_reason IS NOT NULL)",
            name="status_abstain_reason",
        ),
    )

    ranking_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    # Snapshot version axis (DAT-413): the begin_session run that discovered this.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    measure_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    measure_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id"), nullable=False, index=True
    )
    # A human label for the measure (the column name / ratio label) — what the
    # ranking was computed FOR, denormalized for the read surface.
    measure_label: Mapped[str] = mapped_column(String, nullable=False)
    # flow | stock | ratio; NULL exactly when abstained with an unresolved type
    # (DAT-859 — never a silent "flow" default).
    target_type: Mapped[str | None] = mapped_column(String, nullable=True)

    # DAT-859: MEASURED (the default) or ABSTAINED, paired with abstain_reason by
    # the CHECK above. Never inferred from empty ranked content — see
    # DriverRanking's docstring.
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=RankingStatus.MEASURED.value
    )
    abstain_reason: Mapped[str | None] = mapped_column(String, nullable=True)

    # The PRIMARY family's exchangeable grain (DAT-552/561/563): "row", or "entity"
    # when the cluster-aware path made an entity-grain family primary; ``entity`` then
    # names which identity column that grain belongs to (None at row grain).
    grain: Mapped[str] = mapped_column(String, nullable=False)
    entity: Mapped[str | None] = mapped_column(String)
    n_rows: Mapped[int] = mapped_column(Integer, nullable=False)

    # Grain-labeled findings, stored granularly (never merged across grains):
    ranked_dimensions: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )  # [{dimension, gain}] — the primary family's significant dims, strongest first
    driver_paths: Mapped[list[list[str]]] = mapped_column(
        JSON, nullable=False, default=list
    )  # [[dim, ...]] — surviving drill vectors of the primary tree
    interesting_slices: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )  # [{dimension, value, effect, support}] — sharp-deviation slices, strongest first
    secondary_dimensions: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )  # [{dimension, gain, grain, entity}] — non-primary grain families' dims

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
