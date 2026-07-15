"""SQLAlchemy model for dimension-hierarchy discovery (DAT-537)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class DimensionHierarchy(Base):
    """One drill-down chain, alias group or role pair over a fact's enriched view, per run.

    The deterministic FD pass (DAT-537, stack v4 since DAT-761) writes one row
    per discovered structure. Three kinds share the table via the ``kind``
    discriminator:

    - ``kind='drilldown'``: an ordered drill-down hierarchy. ``members`` lists the
      levels **finest → coarsest** (each FD-determines the next, e.g. ``zip → city
      → state``); ``canonical_label`` renders the chain.
    - ``kind='alias'``: a 1:1 redundant-axis group (bidirectional ``g3 ≈ 0``).
      ``members`` lists the equivalent columns; ``canonical_label`` is the chosen
      canonical axis name. A near-copy the role check could not decide surfaces
      as an alias with ``needs_confirmation=True`` (never silently merged).
    - ``kind='role'`` (DAT-761): a role-playing near-copy pair (bill-to ⇄ pay-to)
      whose disagreement set is membership-systematic — the two columns are the
      SAME domain in different roles: kept as separate axes, never merged, never
      stacked as levels. ``score`` is the value-disagreement rate.

    Run-versioned like ``MeasureAggregationLineage`` (DAT-491): form-(a) writer —
    one row per ``(signature, run_id)``, UPSERTed, so a Temporal success-redelivery
    (same ``run_id``, deterministic g3) converges in place and prior runs' rows
    coexist. ``signature`` is the run-grain dedup key:
    ``"{kind}:{table_id}:" + "|".join(sorted(member column_names))``.

    Sealed under the begin_session ``(catalog, "catalog")`` head: it is on
    ``read_views._CATALOG_GRAIN``, so ``current_dimension_hierarchies`` resolves
    the promoted run and ``session_promote_to_latest`` flips it with the rest of
    the catalog — no separate promote.
    """

    __tablename__ = "dimension_hierarchies"
    __table_args__ = (
        UniqueConstraint("signature", "run_id", name="uq_dimension_hierarchy_signature_run"),
        Index("idx_dimension_hierarchies_table", "table_id"),
        Index("idx_dimension_hierarchies_run", "run_id"),
    )

    hierarchy_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Snapshot version axis (DAT-448): the begin_session run that derived this.
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    # The fact whose grain-verified enriched view the chain was computed over
    # (cross-table levels surface for free on the denormalized view).
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    kind: Mapped[str] = mapped_column(String, nullable=False)  # 'drilldown' | 'alias' | 'role'

    # Ordered member columns. Each entry: {column_name, column_id, distinct_count}.
    # drilldown: finest → coarsest (the drill path). alias: the equivalent group,
    # canonical first. ``column_id`` is the catalog SliceDefinition's underlying
    # column (provenance); ``column_name`` is the enriched-view column the g3 pass
    # measured (may be an FK-prefixed dim col) and is the member identity.
    members: Mapped[list[dict[str, object]]] = mapped_column(JSON, nullable=False)
    canonical_label: Mapped[str] = mapped_column(String, nullable=False)

    # Run-grain dedup key (see class docstring); the UNIQUE rides on it because a
    # JSON ``members`` column cannot be a conflict target.
    signature: Mapped[str] = mapped_column(String, nullable=False)

    # The g3 evidence: the highest edge g3 in a drilldown chain (the weakest link;
    # max over edges), or the bidirectional g3 for an alias. Lower = stronger
    # (0 = every edge is an exact FD). Audit + the support/confidence ordering for
    # downstream consumers.
    score: Mapped[float] = mapped_column(Float, nullable=False)

    # 'g3' (auto-discovered) | 'manual' (a teach add/alias assertion).
    detection_source: Mapped[str] = mapped_column(String, nullable=False, default="g3")
    # Low-support / borderline edges are surfaced for confirmation, not silently
    # auto-asserted (DAT-537 guard). A manual teach clears it.
    needs_confirmation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
