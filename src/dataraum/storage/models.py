"""Core entity models (Sources, Tables, Columns).

These are the fundamental entities that don't change across the 5-pillar architecture.
They serve as anchor points for all context metadata.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
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
    status: Mapped[str | None] = mapped_column(String, nullable=True)
    backend: Mapped[str | None] = mapped_column(String, nullable=True)
    discovered_schema: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Relationships
    tables: Mapped[list[Table]] = relationship(
        back_populates="source", cascade="all, delete-orphan"
    )


class Table(Base):
    """Tables from data sources.

    A table can exist in different layers:
    - 'raw': VARCHAR-first staging layer
    - 'typed': After type resolution
    - 'quarantine': Failed type casts
    """

    __tablename__ = "tables"
    __table_args__ = (
        UniqueConstraint("source_id", "table_name", "layer", name="uq_source_table_layer"),
    )

    table_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    layer: Mapped[str] = mapped_column(String, nullable=False)  # 'raw', 'typed', 'quarantine'
    duckdb_path: Mapped[str | None] = mapped_column(String)  # Path to DuckDB table/parquet
    row_count: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    last_profiled_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Relationships
    source: Mapped[Source] = relationship(back_populates="tables")
    columns: Mapped[list[Column]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )

    # Semantic context relationships
    entity_detections: Mapped[list[TableEntity]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
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

    # Relationships
    table: Mapped[Table] = relationship(back_populates="columns")

    # Context-specific relationships (defined in their respective modules)
    statistical_profiles: Mapped[list[StatisticalProfile]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )
    statistical_quality_metrics: Mapped[list[StatisticalQualityMetrics]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )

    # Type inference relationships
    type_candidates: Mapped[list[TypeCandidate]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )
    type_decision: Mapped[TypeDecision | None] = relationship(
        back_populates="column", uselist=False, cascade="all, delete-orphan"
    )

    # Semantic context relationships
    semantic_annotation: Mapped[SemanticAnnotation | None] = relationship(
        back_populates="column", uselist=False, cascade="all, delete-orphan"
    )

    # Temporal analysis
    temporal_profiles: Mapped[list[TemporalColumnProfile]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )

    # Relationship tracking
    relationships_from: Mapped[list[Relationship]] = relationship(
        foreign_keys="Relationship.from_column_id", back_populates="from_column"
    )
    relationships_to: Mapped[list[Relationship]] = relationship(
        foreign_keys="Relationship.to_column_id", back_populates="to_column"
    )


class DBMetadataHints(Base):
    """Structural metadata harvested from a database source.

    Captured at extraction time from the source database's
    information_schema (PK, FK, indexes, CHECK constraints) so the
    pipeline has authoritative DB-side context without re-querying the
    source. Phase A captures; Phase B consumes (FKs become prior
    evidence in the relationships phase; PKs/uniques inform the
    typing/key-candidate logic).

    One row per source. Payloads are JSON lists with backend-agnostic
    shapes so adding new backends does not require schema migrations.
    """

    __tablename__ = "db_metadata_hints"

    hint_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(
        ForeignKey("sources.source_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    # [{"table": "Invoices", "columns": ["invoice_id"]}, ...]
    primary_keys: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)

    # [{"from_table": "Orders", "from_columns": ["customer_id"],
    #   "to_table": "Customers", "to_columns": ["customer_id"],
    #   "constraint_name": "fk_orders_customers"}, ...]
    foreign_keys: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)

    # [{"table": "Invoices", "name": "ix_invoices_date",
    #   "columns": ["invoice_date"], "unique": false}, ...]
    indexes: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)

    # [{"table": "Invoices", "name": "ck_total_positive",
    #   "expression": "total > 0"}, ...]
    check_constraints: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )

    harvested_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# Indexes for common queries
Index("idx_columns_table", Column.table_id)
Index("idx_tables_source", Table.source_id)
