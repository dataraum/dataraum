"""Ontology Models.

SQLAlchemy models for storing and applying domain ontologies.
Ontologies guide semantic interpretation and define business metrics.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Table


class Ontology(Base):
    """Domain ontologies.

    Defines business concepts, metrics, quality rules, and semantic hints
    for a specific domain (e.g., financial_reporting, marketing_analytics).
    """

    __tablename__ = "ontologies"

    ontology_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    version: Mapped[str | None] = mapped_column(String)

    # Content (loaded from YAML configs)
    concepts: Mapped[dict | None] = mapped_column(JSON)  # Business concepts and their definitions
    metrics: Mapped[dict | None] = mapped_column(JSON)  # Computable metrics with formulas
    quality_rules: Mapped[dict | None] = mapped_column(JSON)  # Domain-specific quality rules
    semantic_hints: Mapped[dict | None] = mapped_column(
        JSON
    )  # Column pattern -> semantic role mappings

    # Metadata
    is_builtin: Mapped[bool] = mapped_column(Boolean, default=False)  # Built-in vs user-defined
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
    applications: Mapped[list["OntologyApplication"]] = relationship(
        back_populates="ontology", cascade="all, delete-orphan"
    )


class OntologyApplication(Base):
    """Ontology application log.

    Records when an ontology was applied to a table and what was matched.
    """

    __tablename__ = "ontology_applications"

    application_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    ontology_id: Mapped[str] = mapped_column(ForeignKey("ontologies.ontology_id"), nullable=False)

    # Results
    matched_concepts: Mapped[dict | None] = mapped_column(JSON)  # Which concepts were detected
    applicable_metrics: Mapped[dict | None] = mapped_column(JSON)  # Which metrics can be computed
    applied_rules: Mapped[dict | None] = mapped_column(JSON)  # Which quality rules were applied

    applied_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped["Table"] = relationship(back_populates="ontology_applications")
    ontology: Mapped["Ontology"] = relationship(back_populates="applications")
