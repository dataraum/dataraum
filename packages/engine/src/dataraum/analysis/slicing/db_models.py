"""SQLAlchemy models for slicing analysis.

Contains the database model for slice definitions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column, Table


class SliceDefinition(Base):
    """The dimension inventory: one aggregation/filter dimension per row.

    Existence is DETERMINISTIC (DAT-725 rescope): the slicing phase persists
    every eligible column — grain-safe pre-filter survivor (DAT-805: not a
    constant, not majority-NULL, not a near-unique key, on a scale-invariant
    near-key fraction) whose ``semantic_role`` is not measure/timestamp. The
    LLM's role is ENRICHMENT only: the slicing agent judges which dimensions are
    business breakdown axes, and its ``slice_interest`` / ``business_context`` /
    ``reasoning`` / ``confidence`` merge onto rows that exist regardless.
    Same data + same code ⇒ the same persisted dimension set, run to run — an
    elected-subset catalog silently dropped real axes (a folded ``account_id``
    was elected 0-2 times across runs) from every existence consumer (drivers,
    lineage, bus_matrix).

    Grain-safety is by construction — enriched dimensions are grain-verified FK
    joins; thin per-group support is folded by the driver-tree's ``min_support``
    (DAT-538 removed the redundant always-true ``grain_safe`` flag). Curation
    surfaces (cycles/graphs/validation context + the cockpit ``<dimensions>``
    block) take the JUDGED rows ordered by ``(slice_interest, slice_relevance
    DESC)`` and REPORT what they left behind (DAT-879 — ``curated_slices`` in
    ``slicing/curation.py``); existence consumers read the full inventory. The
    old ``ORDER BY slice_priority LIMIT CURATED_SLICE_BUDGET`` cut silently, and
    because every un-ranked row tied at the priority floor its remaining budget
    was filled ALPHABETICALLY. Slice *materialization* was removed
    (DAT-536): the structural_reconciliation substrate is aggregated inline over
    the enriched views, so there is no ``sql_template`` to store.

    One definition per ``(table_id, column_name, run_id)`` (DAT-502): the writer
    dedups in-batch and UPSERTs on this key — a Temporal success-redelivery
    (same ``run_id``) converges instead of duplicating, and a new run's
    definitions coexist with prior runs'.
    """

    __tablename__ = "slice_definitions"
    __table_args__ = (
        UniqueConstraint("table_id", "column_name", "run_id", name="uq_slice_def_table_column_run"),
        Index("idx_slice_definitions_table", "table_id"),
        Index("idx_slice_definitions_column", "column_id"),
        Index("idx_slice_definitions_dim_table", "dimension_table_id"),
        # Closed-vocabulary enforcement (DAT-802 enum-standard sweep): the ONLY
        # value ``slicing_phase.py`` (the sole writer) ever produces today —
        # numeric/date-bucket slice types are not yet built. Extending the CHECK
        # is the cost of shipping a second slice type, same as any other closed
        # vocabulary here.
        CheckConstraint("slice_type IN ('categorical')", name="slice_type"),
        # Detection-source vocabulary (DAT-802 / DAT-725): 'llm' = the slicing
        # agent judged this row (its enrichment fields are LLM-derived);
        # 'structural' = a deterministic inventory row the ranker did not touch
        # (enrichment + ``slice_interest`` NULL; ``slice_relevance`` is still
        # measured — it comes from the profile, not the agent).
        # ``slicing_phase.py`` is still the sole writer.
        CheckConstraint("detection_source IN ('llm', 'structural')", name="detection_source"),
        # Interest vocabulary (DAT-879). NULL is a member of the domain and
        # means "the agent did not judge this row" — a distinct state from
        # either label, and the reads report it as such.
        CheckConstraint(
            "slice_interest IS NULL OR slice_interest IN ('primary', 'supporting')",
            name="slice_interest",
        ),
        # A measured score is a fraction. Out-of-range would mean the scorer
        # broke, not that an axis is unusual — fail loud at the write.
        CheckConstraint(
            "slice_relevance IS NULL OR (slice_relevance >= 0.0 AND slice_relevance <= 1.0)",
            name="slice_relevance_range",
        ),
    )

    slice_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    # Snapshot version axis (DAT-448): the begin_session run that derived this
    # definition. Definitions were table-scoped and immortal before — stale
    # cross-run reuse was the DAT-405 bug class.
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Actual column name used for slicing — may differ from columns.column_name when the
    # slice dimension is an enriched FK-prefixed dim col (e.g. "kontonummer_des_gegenkontos__land")
    # while column_id points to the underlying FK column record.
    column_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Referenced-dimension identity (DAT-756): what makes two slices "the same
    # dimension" — resolved structurally from the confirmed relationship catalog,
    # never from ``column_name``. For an enriched slice (``column_id`` is the fact's
    # FK column), ``dimension_table_id`` is the FK-target dim table, ``fk_role`` is
    # the FK column name (carried for role-playing dims — NOT yet a Phase-A identity
    # key), and ``dimension_attribute`` is the enriched suffix (the level, e.g.
    # ``account_type``; NULL when grouping by the FK key itself). All three are NULL
    # for a folded slice (an own categorical column with no grain-safe FK): a folded
    # dimension has no cross-table identity in Phase A and abstains from conformed
    # pairing (that residual is DAT-757). The identity ``(dimension_table_id,
    # dimension_attribute)`` is the single key both the lineage stock/flow witness
    # (``shared_dims``) and the operating-model ``conformed_dimension`` edge group on.
    dimension_table_id: Mapped[str | None] = mapped_column(
        ForeignKey("tables.table_id"), nullable=True
    )
    dimension_attribute: Mapped[str | None] = mapped_column(String, nullable=True)
    fk_role: Mapped[str | None] = mapped_column(String, nullable=True)

    # Curation signal (DAT-879). Two fields, two different kinds of claim:
    #
    # ``slice_relevance`` is MEASURED from this run's profile — coverage x
    # evenness over the value distribution (``slicing/relevance.py``), in
    # [0, 1], kind-agnostic so a future banded numeric axis lands on the same
    # scale (DAT-280). NULL means UNMEASURED (no statistical profile), never
    # "scored zero" — a zero says the axis resolves nothing, which is a claim
    # we have no right to make without a profile.
    #
    # ``slice_interest`` is the agent's ABSOLUTE judgment ('primary' /
    # 'supporting'), NULL for a row the ranker never returned. Absolute rather
    # than ordinal so it is comparable across tables and across slice types.
    # The two are deliberately not folded into one number: relevance says what
    # the data supports, interest says what a reader wants, and the measured
    # number must never silently overrule the judgment.
    slice_relevance: Mapped[float | None] = mapped_column(Float)
    slice_interest: Mapped[str | None] = mapped_column(String)
    slice_type: Mapped[str] = mapped_column(String, nullable=False, default="categorical")

    # The axis's MEMBERSHIP, always measured (DAT-671): this run's statistical
    # profile for the column this row names — ``column_name``, which for an
    # enriched row is the joined ``{fk}__{attr}`` view column, NOT the fact FK
    # that ``column_id`` points at (see the DAT-756 note above). It is never the
    # ranking agent's echo of a value list; a judged row and a structural row
    # carry evidence of identical provenance.
    #
    # Bounded by the profiler's stored top-K, which is NOT one number: 200 for
    # typed fact columns (``phases/statistics.yaml``), 10 for enriched dimension
    # columns (``enriched_views_phase``). So a shorter-than-``value_count`` list
    # is routine, and every consumer that serves these values MUST disclose the
    # "N of M distinct" split rather than present them as the complete set.
    distinct_values: Mapped[list[str] | None] = mapped_column(JSON)

    # The column's measured COUNT(DISTINCT) — never ``len(distinct_values)``
    # (DAT-879). It is the disclosure that makes the bounded list above honest.
    value_count: Mapped[int | None] = mapped_column(Integer)

    # Analysis reasoning
    reasoning: Mapped[str | None] = mapped_column(Text)
    business_context: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float)

    # Provenance
    detection_source: Mapped[str] = mapped_column(String, nullable=False, default="llm")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships. ``table_id`` and ``dimension_table_id`` both FK to
    # ``tables.table_id`` (DAT-756), so the fact-table relationship must name its
    # column explicitly; the dimension table is read as a plain id, no ORM edge.
    table: Mapped[Table] = relationship(foreign_keys=[table_id])
    column: Mapped[Column] = relationship()


__all__ = [
    "SliceDefinition",
]
