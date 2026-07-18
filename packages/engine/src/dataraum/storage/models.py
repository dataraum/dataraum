"""Core entity models (Sources, Tables, Columns).

These are the fundamental entities that don't change across the 5-pillar architecture.
They serve as anchor points for all context metadata.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage.base import Base

if TYPE_CHECKING:
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import (
        SemanticAnnotation,
        TableEntity,
    )
    from dataraum.analysis.statistics.db_models import (
        StatisticalProfile,
    )
    from dataraum.analysis.statistics.quality_db_models import (
        StatisticalQualityMetrics,
    )
    from dataraum.analysis.temporal.db_models import TemporalColumnProfile
    from dataraum.analysis.typing.db_models import (
        TypeCandidate,
        TypeDecision,
    )


class Source(Base):
    """Data sources (CSV files, databases, APIs, etc.)."""

    __tablename__ = "sources"

    source_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    source_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'csv', 'parquet', 'postgres', etc.
    connection_config: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    # Source management fields (onboarding)
    # Journey stage the source has reached (DAT-378). The cockpit drives a source
    # through ``connect → frame → select → add_source`` before triggering the
    # workflow; this column is the persisted cursor the cockpit's journey
    # readiness reads (``journey/stages.ts``). It is the only cross-package field
    # this slice adds — the Temporal contract is untouched (the multi-URI list
    # rides in ``connection_config``, not ``AddSourceInput``). Nullable so a
    # legacy / engine-seeded source with no journey reads as the implicit first
    # stage on the cockpit side.
    stage: Mapped[str | None] = mapped_column(String, nullable=True)
    backend: Mapped[str | None] = mapped_column(String, nullable=True)
    discovered_schema: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Relationships
    # passive_deletes=True: lean on the DB's ON DELETE CASCADE (declared on the
    # child FK) instead of having SQLAlchemy load children and NULL their FKs on
    # flush. Required for bulk SQL DELETE statements to behave like the ORM
    # cascade declaration claims.
    tables: Mapped[list[Table]] = relationship(
        back_populates="source", cascade="all, delete-orphan", passive_deletes=True
    )


class Table(Base):
    """Tables from data sources.

    A table can exist in different layers (DAT-802 audit: the class comment
    previously omitted 'enriched', a real layer ``enriched_views_phase.py``
    writes on every fact table's dimension-widened view):
    - 'raw': VARCHAR-first staging layer.
    - 'typed': After type resolution.
    - 'quarantine': Failed type casts.
    - 'enriched': A fact table's dimension-widened view (``enriched_views_phase.py``).
    """

    __tablename__ = "tables"
    # Workspace-unique table identity (DAT-639): the per-workspace DuckLake
    # catalog IS the namespace, so a workspace holds exactly one ``orders`` raw
    # table — uniqueness is ``(table_name, layer)``, NOT scoped by source. The
    # source is an atomic content-keyed wrapper (how the table arrived), never a
    # disambiguator; two sources cannot each own an ``orders`` (import fails loud
    # and tells the user to retire the existing one first).
    __table_args__ = (
        UniqueConstraint("table_name", "layer", name="uq_table_name_layer"),
        # Closed-vocabulary enforcement (DAT-802 enum-standard sweep): the 4
        # values every writer produces (import/typing/surrogate-mint/enriched-views
        # phases + the 3 source loaders) — see the class docstring above.
        CheckConstraint("layer IN ('raw', 'typed', 'quarantine', 'enriched')", name="layer"),
    )

    table_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    # Closed vocab: see ck_tables_layer and the class docstring.
    layer: Mapped[str] = mapped_column(String, nullable=False)
    # Unqualified DuckDB table name — NARROW, workspace-unique (e.g. ``orders``,
    # no source prefix — DAT-639). Schema is derived from ``layer`` via
    # ``dataraum.core.duckdb_naming.schema_for_layer``; cross-layer SQL composes
    # the full ``"schema.table"`` form via ``qualified_table(layer, table_name)``.
    # The catalog alias (``lake`` in slice 1) is NOT stored here — it's resolved
    # at query time.
    duckdb_path: Mapped[str | None] = mapped_column(String)
    row_count: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    last_profiled_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Relationships
    source: Mapped[Source] = relationship(back_populates="tables")
    columns: Mapped[list[Column]] = relationship(
        back_populates="table", cascade="all, delete-orphan", passive_deletes=True
    )

    # Semantic context relationships
    entity_detections: Mapped[list[TableEntity]] = relationship(
        back_populates="table", cascade="all, delete-orphan", passive_deletes=True
    )


class Column(Base):
    """Columns in tables.

    Core column metadata. Type information and statistical profiles
    are stored in separate context-specific tables.
    """

    __tablename__ = "columns"
    __table_args__ = (UniqueConstraint("table_id", "column_name", name="uq_table_column"),)

    column_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    original_name: Mapped[str | None] = mapped_column(String, nullable=True)
    column_position: Mapped[int] = mapped_column(Integer, nullable=False)

    # Type information
    raw_type: Mapped[str | None] = mapped_column(String)  # Original inferred type (usually VARCHAR)
    resolved_type: Mapped[str | None] = mapped_column(
        String
    )  # Final decided type after type resolution

    # DAT-811 — served-column identity for ENRICHED-view columns. An enriched view is
    # ``SELECT f.* + joined dim columns``; every one of its columns is a real Column row
    # so the catalog (``og_columns``) describes the view completely, WITHOUT walking back
    # to origin tables. Both fields are NULL on base (typed/raw) columns, where the
    # distinction is moot — they carry meaning only under an ``layer='enriched'`` table.
    #   ``origin``           — how the column reached the view: ``'fact'`` (the fact's own
    #                          column, carried through by ``f.*``) or ``'dimension'`` (added
    #                          by a grain-preserving dim join). The discriminator the
    #                          dims-only consumers filter on (``origin == 'dimension'``).
    #   ``source_column_id`` — the TYPED source column this one projects. Read views resolve
    #                          semantics (concept, role, anchor axis, granularity) THROUGH
    #                          this link, so an enriched column inherits its source's meaning
    #                          while keeping its OWN ``column_id`` (the property-graph vertex
    #                          KEY must stay unique — a typed id must not appear twice). Set
    #                          at the enriched_views phase from the join recipe — NEVER parsed
    #                          from the ``{fk}__{col}`` name. SET NULL if the source is torn
    #                          down; the enriched view is re-derived each run regardless.
    origin: Mapped[str | None] = mapped_column(String)
    source_column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id", ondelete="SET NULL")
    )

    # Relationships
    table: Mapped[Table] = relationship(back_populates="columns")

    # Context-specific relationships (defined in their respective modules).
    # passive_deletes=True everywhere a Column-owned child sits on the other end:
    # bulk SQL DELETE statements only work if SQLAlchemy DOES NOT pre-load and
    # NULL the FK on flush, and lets the DB's ON DELETE CASCADE do the work
    # instead.
    statistical_profiles: Mapped[list[StatisticalProfile]] = relationship(
        back_populates="column", cascade="all, delete-orphan", passive_deletes=True
    )
    statistical_quality_metrics: Mapped[list[StatisticalQualityMetrics]] = relationship(
        back_populates="column", cascade="all, delete-orphan", passive_deletes=True
    )

    # Type inference relationships
    type_candidates: Mapped[list[TypeCandidate]] = relationship(
        back_populates="column", cascade="all, delete-orphan", passive_deletes=True
    )
    # One TypeDecision per column PER RUN (DAT-413) — runs coexist, so this is
    # a list; readers pick the relevant run's row (or the latest manual), never
    # "the" decision.
    type_decisions: Mapped[list[TypeDecision]] = relationship(
        back_populates="column",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # Semantic context relationships
    semantic_annotation: Mapped[SemanticAnnotation | None] = relationship(
        back_populates="column",
        uselist=False,
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # Temporal analysis
    temporal_profiles: Mapped[list[TemporalColumnProfile]] = relationship(
        back_populates="column", cascade="all, delete-orphan", passive_deletes=True
    )

    # Relationship tracking — relationships are owned by the relationships
    # phase, not the Column; deletion semantics live at the DB-level CASCADE on
    # the FK (passive_deletes prevents the pre-flush FK-NULL probe).
    relationships_from: Mapped[list[Relationship]] = relationship(
        foreign_keys="Relationship.from_column_id",
        back_populates="from_column",
        passive_deletes=True,
    )
    relationships_to: Mapped[list[Relationship]] = relationship(
        foreign_keys="Relationship.to_column_id",
        back_populates="to_column",
        passive_deletes=True,
    )


# Indexes for common queries
Index("idx_columns_table", Column.table_id)
Index("idx_tables_source", Table.source_id)
