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
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


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
    target_type: Mapped[str] = mapped_column(String, nullable=False)  # flow | stock | ratio

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
