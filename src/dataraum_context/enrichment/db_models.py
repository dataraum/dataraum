"""SQLAlchemy models for enrichment domain.

This module contains all database models for the enrichment subsystem:
- Semantic Context (SemanticAnnotation, TableEntity)
- Relationship Models (Relationship, JoinPath)
- Topological Context (TopologicalQualityMetrics, MultiTableTopologyMetrics, BusinessCycleClassification)
- Temporal Context (TemporalQualityMetrics, TemporalTableSummaryMetrics)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
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

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Column, Table


# =============================================================================
# Semantic Context Models (Pillar 3)
# =============================================================================


class SemanticAnnotation(Base):
    """Semantic annotations for columns.

    Stores LLM-generated or manually-provided semantic metadata
    including business terms, roles, and ontology mappings.
    """

    __tablename__ = "semantic_annotations"
    __table_args__ = (UniqueConstraint("column_id", name="uq_column_semantic_annotation"),)

    annotation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Classification
    semantic_role: Mapped[str | None] = mapped_column(
        String
    )  # 'identifier', 'measure', 'attribute', 'dimension'
    entity_type: Mapped[str | None] = mapped_column(
        String
    )  # 'customer', 'product', 'transaction', etc.

    # Business terms
    business_name: Mapped[str | None] = mapped_column(String)
    business_description: Mapped[str | None] = mapped_column(Text)
    business_domain: Mapped[str | None] = mapped_column(
        String
    )  # 'finance', 'marketing', 'operations'

    # Ontology mapping
    ontology_term: Mapped[str | None] = mapped_column(String)
    ontology_uri: Mapped[str | None] = mapped_column(String)

    # Provenance
    annotation_source: Mapped[str | None] = mapped_column(
        String
    )  # 'llm', 'manual', 'config_override'
    annotated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    annotated_by: Mapped[str | None] = mapped_column(String)
    confidence: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="semantic_annotation")


class TableEntity(Base):
    """Entity detection at table level.

    Identifies the type of entity represented by the table
    and classifies it as fact/dimension table with grain analysis.
    """

    __tablename__ = "table_entities"

    entity_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    detected_entity_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'customer', 'order', 'product', etc.
    description: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Grain analysis
    grain_columns: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # List of column IDs that define grain
    is_fact_table: Mapped[bool | None] = mapped_column(Boolean)
    is_dimension_table: Mapped[bool | None] = mapped_column(Boolean)

    # Provenance
    detection_source: Mapped[str | None] = mapped_column(String)  # 'llm', 'heuristic', 'manual'
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship(back_populates="entity_detections")


# =============================================================================
# Relationship Models (Pillar 2)
# =============================================================================


class Relationship(Base):
    """Detected relationships between columns.

    Represents foreign key relationships or other associations
    detected through TDA, cardinality analysis, or semantic similarity.
    """

    __tablename__ = "relationships"
    __table_args__ = (
        UniqueConstraint("from_column_id", "to_column_id", name="uq_relationship_columns"),
    )

    relationship_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    # Source side
    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    from_column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)

    # Target side
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)

    # Classification
    relationship_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'foreign_key', 'semantic_reference', 'derived'
    cardinality: Mapped[str | None] = mapped_column(String)  # '1:1', '1:N', 'N:1', 'N:M'

    # Confidence and evidence
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detection_method: Mapped[str | None] = mapped_column(
        String
    )  # 'tda', 'cardinality', 'semantic', 'manual'
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Verification (human-in-loop)
    is_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime)
    confirmed_by: Mapped[str | None] = mapped_column(String)

    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    from_column: Mapped[Column] = relationship(
        foreign_keys=[from_column_id], back_populates="relationships_from"
    )
    to_column: Mapped[Column] = relationship(
        foreign_keys=[to_column_id], back_populates="relationships_to"
    )


Index("idx_relationships_from", Relationship.from_table_id)
Index("idx_relationships_to", Relationship.to_table_id)


class JoinPath(Base):
    """Computed join paths between tables.

    Pre-computed paths for efficient multi-table queries.
    Updated when relationships change.
    """

    __tablename__ = "join_paths"
    __table_args__ = (
        UniqueConstraint("from_table_id", "to_table_id", "path_steps", name="uq_join_path"),
    )

    path_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    # Path definition
    path_steps: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False
    )  # List of relationship IDs forming the path
    path_length: Mapped[int] = mapped_column(Integer, nullable=False)  # Number of hops
    total_confidence: Mapped[float | None] = mapped_column(
        Float
    )  # Product of relationship confidences

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# =============================================================================
# Topological Context Models (Pillar 2)
# =============================================================================


class TopologicalQualityMetrics(Base):
    """Topological quality metrics for a table.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable core dimensions (Betti numbers, flags)
    - JSONB field: Full topological analysis results

    Tracks structural features like Betti numbers, persistence, and stability.
    These metrics help detect structural anomalies and track topology changes over time.
    """

    __tablename__ = "topological_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # STRUCTURED: Queryable core dimensions
    # Betti numbers (homology dimensions)
    betti_0: Mapped[int | None] = mapped_column(Integer)  # Connected components
    betti_1: Mapped[int | None] = mapped_column(Integer)  # Cycles / holes
    betti_2: Mapped[int | None] = mapped_column(Integer)  # Voids / cavities

    # Overall complexity
    structural_complexity: Mapped[int | None] = mapped_column(Integer)  # Sum of Betti numbers
    orphaned_components: Mapped[int | None] = mapped_column(Integer)  # Disconnected subgraphs

    # Flags for filtering
    homologically_stable: Mapped[bool | None] = mapped_column(Boolean)  # Within threshold?
    has_cycles: Mapped[bool | None] = mapped_column(Boolean)  # betti_1 > 0
    has_anomalies: Mapped[bool | None] = mapped_column(Boolean)

    # JSONB: Full topological analysis
    # Stores: persistence diagrams, stability metrics, complexity history, anomalous cycles, etc.
    topology_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class MultiTableTopologyMetrics(Base):
    """Multi-table topology analysis results.

    Tracks cross-table relationships, graph topology, and business process detection
    across multiple related tables. Enables historical tracking of schema evolution,
    relationship changes, and business process patterns over time.
    """

    __tablename__ = "multi_table_topology_metrics"

    analysis_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Table IDs included in analysis (JSON array of strings)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # STRUCTURED: Queryable cross-table metrics
    cross_table_cycles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    graph_betti_0: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )  # Graph connectivity
    relationship_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Flags for filtering
    has_cross_table_cycles: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_connected_graph: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )  # graph_betti_0 == 1

    # JSONB: Full multi-table analysis result
    # Stores: per_table results (references), cross_table details, domain_analysis, business_processes
    analysis_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class BusinessCycleClassification(Base):
    """LLM-classified business cycle from cross-table topology.

    Stores business process cycles detected in multi-table analysis,
    classified by LLM (e.g., accounts_receivable_cycle, revenue_cycle).

    These classifications provide context for:
    - Filter generation (which business process is this data part of?)
    - Quality assessment (is the cycle complete?)
    - Downstream calculations (which tables feed into which calculations?)
    """

    __tablename__ = "business_cycle_classifications"

    cycle_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    analysis_id: Mapped[str] = mapped_column(
        ForeignKey("multi_table_topology_metrics.analysis_id", ondelete="CASCADE"),
        nullable=False,
    )
    classified_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Classification result
    cycle_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g., "accounts_receivable_cycle"
    confidence: Mapped[float] = mapped_column(Float, nullable=False)  # 0.0 to 1.0
    business_value: Mapped[str] = mapped_column(String, nullable=False)  # "high", "medium", "low"
    completeness: Mapped[str] = mapped_column(
        String, nullable=False
    )  # "complete", "partial", "incomplete"

    # Tables involved in this cycle (JSON array of table_ids)
    table_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # LLM explanation
    explanation: Mapped[str | None] = mapped_column(String, nullable=True)

    # Missing elements if incomplete
    missing_elements: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # Source tracking
    llm_model: Mapped[str | None] = mapped_column(
        String, nullable=True
    )  # Model used for classification


# =============================================================================
# Temporal Context Models (Pillar 4)
# =============================================================================


class TemporalQualityMetrics(Base):
    """Temporal quality metrics for a time column.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable dimensions (IDs, timestamps, key metrics)
    - JSONB field: Full Pydantic model for flexibility

    This allows:
    - Fast queries on core dimensions
    - Schema flexibility for experimentation
    - Zero mapping code (Pydantic handles serialization)
    """

    __tablename__ = "temporal_quality_metrics"

    metric_id: Mapped[str] = mapped_column(String, primary_key=True)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # STRUCTURED: Queryable core dimensions
    min_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    max_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    detected_granularity: Mapped[str] = mapped_column(String, nullable=False)
    completeness_ratio: Mapped[float | None] = mapped_column(Float)

    # Flags for filtering (fast queries)
    has_seasonality: Mapped[bool | None] = mapped_column(Boolean)
    has_trend: Mapped[bool | None] = mapped_column(Boolean)
    is_stale: Mapped[bool | None] = mapped_column(Boolean)

    # JSONB: Full temporal profile + quality data
    # Stores complete Pydantic models: TemporalProfile, quality analysis results
    temporal_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class TemporalTableSummaryMetrics(Base):
    """Table-level temporal quality summary across multiple temporal columns.

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable aggregates (counts, scores, freshness)
    - JSONB field: Full TemporalTableSummary Pydantic model

    This allows dashboards to quickly query:
    - Tables with seasonality patterns
    - Tables with stale data
    - Average temporal quality across tables
    """

    __tablename__ = "temporal_table_summary_metrics"

    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), primary_key=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # STRUCTURED: Queryable aggregates
    temporal_column_count: Mapped[int] = mapped_column(nullable=False)
    total_issues: Mapped[int] = mapped_column(nullable=False)

    # Pattern counts (for filtering)
    columns_with_seasonality: Mapped[int] = mapped_column(nullable=False, default=0)
    columns_with_trends: Mapped[int] = mapped_column(nullable=False, default=0)
    columns_with_change_points: Mapped[int] = mapped_column(nullable=False, default=0)
    columns_with_fiscal_alignment: Mapped[int] = mapped_column(nullable=False, default=0)

    # Freshness tracking
    stalest_column_days: Mapped[int | None] = mapped_column()
    has_stale_columns: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # JSONB: Full table summary data
    summary_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Semantic Context
    "SemanticAnnotation",
    "TableEntity",
    # Relationship Models
    "Relationship",
    "JoinPath",
    # Topological Context
    "TopologicalQualityMetrics",
    "MultiTableTopologyMetrics",
    "BusinessCycleClassification",
    # Temporal Context
    "TemporalQualityMetrics",
    "TemporalTableSummaryMetrics",
]
