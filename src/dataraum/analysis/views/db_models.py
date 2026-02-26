"""SQLAlchemy models for enriched views.

Tracks which DuckDB views have been created, their SQL,
and the relationships they are based on.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class EnrichedView(Base):
    """Record of an enriched DuckDB view.

    Tracks views created by joining fact tables with their confirmed
    dimension tables. The view SQL and metadata are stored for
    reproducibility and downstream consumption.
    """

    __tablename__ = "enriched_views"

    view_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # The fact table this view is based on
    fact_table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    # The view registered as a Table record (layer="enriched")
    view_table_id: Mapped[str | None] = mapped_column(
        ForeignKey("tables.table_id", ondelete="SET NULL")
    )

    view_name: Mapped[str] = mapped_column(String, nullable=False)
    view_sql: Mapped[str] = mapped_column(Text, nullable=False)

    # Which relationships were used to build this view
    relationship_ids: Mapped[list[str] | None] = mapped_column(JSON)

    # Which dimension tables are joined
    dimension_table_ids: Mapped[list[str] | None] = mapped_column(JSON)

    # Columns added from dimension tables (e.g., ["customers__name", "customers__country"])
    dimension_columns: Mapped[list[str] | None] = mapped_column(JSON)

    # Grain verification: COUNT(*) of view == fact table row_count
    is_grain_verified: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


class SlicingView(Base):
    """Record of a slicing DuckDB view.

    Projection of enriched_view keeping only slice-relevant dimension columns.
    The view contains all fact table columns plus only the dimension columns
    that correspond to SliceDefinitions for this table.
    """

    __tablename__ = "slicing_views"

    view_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # The fact table this view is based on
    fact_table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    view_name: Mapped[str] = mapped_column(String, nullable=False)
    view_sql: Mapped[str] = mapped_column(Text, nullable=False)

    # Which slice definitions drove column selection
    slice_definition_ids: Mapped[list[str] | None] = mapped_column(JSON)

    # Dimension columns kept in this view (subset of enriched_view.dimension_columns)
    slice_columns: Mapped[list[str] | None] = mapped_column(JSON)

    # Grain verification: COUNT(*) of view == fact table row_count
    is_grain_verified: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = ["EnrichedView", "SlicingView"]
