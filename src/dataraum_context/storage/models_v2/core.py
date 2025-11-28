"""Core entity models (Sources, Tables, Columns).

These are the fundamental entities that don't change across the 5-pillar architecture.
They serve as anchor points for all context metadata.
"""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage.models_v2.base import Base


class Source(Base):
    """Data sources (CSV files, databases, APIs, etc.)."""

    __tablename__ = "sources"

    source_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    source_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'csv', 'parquet', 'postgres', etc.
    connection_config: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    # Relationships
    tables: Mapped[list["Table"]] = relationship(
        back_populates="source", cascade="all, delete-orphan"
    )
    quality_contexts: Mapped[list["QualityContext"]] = relationship(
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
    source: Mapped["Source"] = relationship(back_populates="tables")
    columns: Mapped[list["Column"]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )

    # Quality context relationships
    domain_quality_metrics: Mapped[list["DomainQualityMetrics"]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )
    financial_quality_metrics: Mapped[list["FinancialQualityMetrics"]] = relationship(
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
    column_position: Mapped[int] = mapped_column(Integer, nullable=False)

    # Type information
    raw_type: Mapped[str | None] = mapped_column(String)  # Original inferred type (usually VARCHAR)
    resolved_type: Mapped[str | None] = mapped_column(
        String
    )  # Final decided type after type resolution

    # Relationships
    table: Mapped["Table"] = relationship(back_populates="columns")

    # Context-specific relationships (defined in their respective modules)
    statistical_profiles: Mapped[list["StatisticalProfile"]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )
    statistical_quality_metrics: Mapped[list["StatisticalQualityMetrics"]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )


# Indexes for common queries
Index("idx_columns_table", Column.table_id)
Index("idx_tables_source", Table.source_id)
