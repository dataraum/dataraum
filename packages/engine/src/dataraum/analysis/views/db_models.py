"""SQLAlchemy models for enriched views.

Tracks which DuckDB views have been created, their SQL,
and the relationships they are based on.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base


class EnrichedView(Base):
    """Record of an enriched DuckDB view.

    Tracks views created by joining fact tables with their confirmed
    dimension tables. **Latest-only** (DAT-415): one row per ``fact_table_id``,
    reconciled in place each run — ``run_id`` is a provenance stamp (the run that
    last materialized it), NOT a version axis. The version history + reset live in
    the :class:`~dataraum.analysis.typing.db_models.MaterializationRecipe`
    (``layer="enriched"``) — the view's ``CREATE VIEW`` DDL is stored there
    (canonical-SQL-gated, the single rebuild source), never here. ``view_sql`` was
    removed (it was write-only).

    The latest-only "one row per ``fact_table_id``" invariant the reconcile and
    every reader (e.g. ``dimension_coverage`` via ``scalar_one_or_none``) rely on
    is **DB-enforced** by ``uq_enriched_view_fact_table`` — not just an app-level
    convention. A second row for the same fact fails loudly at insert instead of
    silently surfacing as ``MultipleResultsFound`` in a reader.
    """

    __tablename__ = "enriched_views"
    __table_args__ = (UniqueConstraint("fact_table_id", name="uq_enriched_view_fact_table"),)

    view_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # The fact table this view is based on
    fact_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    # The view registered as a Table record (layer="enriched")
    view_table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))
    view_table = relationship("Table", foreign_keys=[view_table_id])

    view_name: Mapped[str] = mapped_column(String, nullable=False)

    # Snapshot version axis (DAT-413/DAT-415): the begin_session run that
    # materialized this view definition.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # Which relationships were used to build this view (the EXPOSED subset). These are
    # per-run ``relationship_id``s (run-stamped uuid4); the cross-run-stable identity is
    # the column pair below, not these ids.
    relationship_ids: Mapped[list[str] | None] = mapped_column(JSON)

    # The candidate FK column-pairs already JUDGED for this fact — exposed OR
    # rejected-by-the-enrichment-LLM — as
    # ``[[from_column_id, to_column_id, cardinality], ...]`` (DAT-516/791). The
    # enriched-view shape is sticky: a re-run feeds the LLM only the *undecided* pairs
    # (candidates not in this set) and inherits the rest, so the shape is monotonic
    # (grows on a newly-confirmed relationship, shrinks only on an explicit reject).
    # The third element is the measured cardinality the verdict was made on — the
    # re-open basis is topology-only (DAT-791): a cardinality flip re-opens the pair;
    # confidence/coverage jitter never does. Keyed on ``column_id`` — stable across
    # begin_session re-runs (typed-table columns are minted at add_source, not re-run)
    # — NOT ``relationship_id`` (per-run).
    # ``None`` = first run / legacy row: nothing decided yet → judge every candidate.
    considered_relationship_pairs: Mapped[list[list[str | None]] | None] = mapped_column(JSON)

    # The EXPOSED joins, serialized so a re-run rebuilds the shape WITHOUT re-judging
    # (DAT-516). One entry per exposed dimension join:
    # ``{"from_column_id","to_column_id","fact_fk_column","dim_pk_column",
    #    "dim_table_name","include_columns"}``. ``include_columns`` is itself LLM-judged
    # (the agent rates each dim attr high/medium/low and keeps high+medium), so the pair
    # alone can't reconstruct the shape — the whole spec is inherited. ``from/to_column_id``
    # (the stable key) lets a re-run drop a join whose relationship Layer A no longer
    # confirms. ``None`` = first run / legacy → built fresh from the LLM.
    exposed_dimension_joins: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)

    # Which dimension tables are joined
    dimension_table_ids: Mapped[list[str] | None] = mapped_column(JSON)

    # Columns added from dimension tables (e.g., ["customers__name", "customers__country"])
    dimension_columns: Mapped[list[str] | None] = mapped_column(JSON)

    # Grain verification: COUNT(*) of view == fact table row_count
    is_grain_verified: Mapped[bool] = mapped_column(Boolean, default=False)

    # LLM enrichment evidence (reasoning, dimension type, model used)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = ["EnrichedView"]
