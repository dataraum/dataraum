"""SQLAlchemy models for metadata storage.

DEPRECATED: This module is being replaced by storage.models_v2 with a cleaner 5-pillar architecture.

DO NOT ADD NEW MODELS HERE. Use storage/models_v2/ instead.

This file will be removed after the migration to the new architecture is complete (after Phase 6).

Deprecation date: 2025-11-28
Planned removal: After Phase 6 completion
New location: storage/models_v2/

Note: This module defines database models (SQLAlchemy ORM).
For API/interface models (Pydantic), see core.models.
"""

import warnings

warnings.warn(
    "storage.models is deprecated and will be removed after Phase 6. "
    "Use storage.models_v2 instead.",
    DeprecationWarning,
    stacklevel=2,
)

from datetime import UTC, datetime
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

from dataraum_context.storage.base import Base

# ============================================================================
# Schema Version Tracking
# ============================================================================


class DBSchemaVersion(Base):
    """Track schema versions for compatibility.

    Note: Named DBSchemaVersion to avoid conflict with core.models.
    """

    __tablename__ = "schema_version"

    version: Mapped[str] = mapped_column(String, primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# ============================================================================
# Core Tables
# ============================================================================


class Source(Base):
    """Data sources (CSV files, databases, APIs, etc.)."""

    __tablename__ = "sources"

    source_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    source_type: Mapped[str] = mapped_column(String, nullable=False)
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
    tables: Mapped[list[Table]] = relationship(
        back_populates="source", cascade="all, delete-orphan"
    )


class Table(Base):
    """Tables from data sources (raw, typed, or quarantine layers)."""

    __tablename__ = "tables"
    __table_args__ = (
        UniqueConstraint("source_id", "table_name", "layer", name="uq_source_table_layer"),
    )

    table_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    layer: Mapped[str] = mapped_column(String, nullable=False)
    duckdb_path: Mapped[str | None] = mapped_column(String)
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
    entity_detections: Mapped[list[TableEntity]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )
    quality_rules: Mapped[list[QualityRule]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )
    quality_scores: Mapped[list[QualityScore]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )
    ontology_applications: Mapped[list[OntologyApplication]] = relationship(
        back_populates="table", cascade="all, delete-orphan"
    )


class Column(Base):
    """Columns in tables."""

    __tablename__ = "columns"
    __table_args__ = (UniqueConstraint("table_id", "column_name", name="uq_table_column"),)

    column_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    column_name: Mapped[str] = mapped_column(String, nullable=False)
    column_position: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_type: Mapped[str | None] = mapped_column(String)
    resolved_type: Mapped[str | None] = mapped_column(String)

    # Relationships
    table: Mapped[Table] = relationship(back_populates="columns")
    profiles: Mapped[list[ColumnProfile]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )
    type_candidates: Mapped[list[TypeCandidate]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )
    type_decision: Mapped[TypeDecision | None] = relationship(
        back_populates="column", uselist=False, cascade="all, delete-orphan"
    )
    semantic_annotation: Mapped[SemanticAnnotation | None] = relationship(
        back_populates="column", uselist=False, cascade="all, delete-orphan"
    )
    temporal_profile: Mapped[TemporalProfile | None] = relationship(
        back_populates="column", uselist=False, cascade="all, delete-orphan"
    )
    quality_rules: Mapped[list[QualityRule]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )
    quality_scores: Mapped[list[QualityScore]] = relationship(
        back_populates="column", cascade="all, delete-orphan"
    )
    relationships_from: Mapped[list[Relationship]] = relationship(
        foreign_keys="Relationship.from_column_id", back_populates="from_column"
    )
    relationships_to: Mapped[list[Relationship]] = relationship(
        foreign_keys="Relationship.to_column_id", back_populates="to_column"
    )


Index("idx_columns_table", Column.table_id)


# ============================================================================
# Statistical Metadata Tables
# ============================================================================


class ColumnProfile(Base):
    """Statistical profile of a column (versioned by profiled_at)."""

    __tablename__ = "column_profiles"

    profile_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    profiled_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Counts
    total_count: Mapped[int] = mapped_column(Integer, nullable=False)
    null_count: Mapped[int] = mapped_column(Integer, nullable=False)
    distinct_count: Mapped[int | None] = mapped_column(Integer)

    # Numeric stats
    min_value: Mapped[float | None] = mapped_column(Float)
    max_value: Mapped[float | None] = mapped_column(Float)
    mean_value: Mapped[float | None] = mapped_column(Float)
    stddev_value: Mapped[float | None] = mapped_column(Float)
    percentiles: Mapped[dict | None] = mapped_column(JSON)

    # String stats
    min_length: Mapped[int | None] = mapped_column(Integer)
    max_length: Mapped[int | None] = mapped_column(Integer)
    avg_length: Mapped[float | None] = mapped_column(Float)

    # Distribution
    histogram: Mapped[dict | None] = mapped_column(JSON)
    top_values: Mapped[dict | None] = mapped_column(JSON)

    # Computed metrics
    cardinality_ratio: Mapped[float | None] = mapped_column(Float)
    null_ratio: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="profiles")


Index("idx_column_profiles_latest", ColumnProfile.column_id, ColumnProfile.profiled_at.desc())


class TypeCandidate(Base):
    """Type candidates from pattern detection."""

    __tablename__ = "type_candidates"

    candidate_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    data_type: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    parse_success_rate: Mapped[float | None] = mapped_column(Float)
    failed_examples: Mapped[dict | None] = mapped_column(JSON)

    # Pattern info
    detected_pattern: Mapped[str | None] = mapped_column(String)
    pattern_match_rate: Mapped[float | None] = mapped_column(Float)

    # Unit detection (from Pint)
    detected_unit: Mapped[str | None] = mapped_column(String)
    unit_confidence: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="type_candidates")


Index("idx_type_candidates_column", TypeCandidate.column_id)


class TypeDecision(Base):
    """Type decisions (human-reviewable)."""

    __tablename__ = "type_decisions"
    __table_args__ = (UniqueConstraint("column_id", name="uq_column_type_decision"),)

    decision_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    decided_type: Mapped[str] = mapped_column(String, nullable=False)
    decision_source: Mapped[str] = mapped_column(String, nullable=False)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    decided_by: Mapped[str | None] = mapped_column(String)

    # Audit trail
    previous_type: Mapped[str | None] = mapped_column(String)
    decision_reason: Mapped[str | None] = mapped_column(String)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="type_decision")


# ============================================================================
# Semantic Metadata Tables
# ============================================================================


class SemanticAnnotation(Base):
    """Semantic annotations (LLM-generated or manual)."""

    __tablename__ = "semantic_annotations"
    __table_args__ = (UniqueConstraint("column_id", name="uq_column_semantic_annotation"),)

    annotation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Classification
    semantic_role: Mapped[str | None] = mapped_column(String)
    entity_type: Mapped[str | None] = mapped_column(String)

    # Business terms
    business_name: Mapped[str | None] = mapped_column(String)
    business_description: Mapped[str | None] = mapped_column(Text)
    business_domain: Mapped[str | None] = mapped_column(String)

    # Ontology mapping
    ontology_term: Mapped[str | None] = mapped_column(String)
    ontology_uri: Mapped[str | None] = mapped_column(String)

    # Provenance
    annotation_source: Mapped[str | None] = mapped_column(String)
    annotated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    annotated_by: Mapped[str | None] = mapped_column(String)
    confidence: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="semantic_annotation")


class TableEntity(Base):
    """Entity detection at table level."""

    __tablename__ = "table_entities"

    entity_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    detected_entity_type: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float)
    evidence: Mapped[dict | None] = mapped_column(JSON)

    # Grain
    grain_columns: Mapped[dict | None] = mapped_column(JSON)
    is_fact_table: Mapped[bool | None] = mapped_column(Boolean)
    is_dimension_table: Mapped[bool | None] = mapped_column(Boolean)

    # Provenance
    detection_source: Mapped[str | None] = mapped_column(String)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship(back_populates="entity_detections")


# ============================================================================
# Topological Metadata Tables
# ============================================================================


class Relationship(Base):
    """Detected relationships (from TDA or other methods)."""

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
    relationship_type: Mapped[str] = mapped_column(String, nullable=False)
    cardinality: Mapped[str | None] = mapped_column(String)

    # Confidence
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detection_method: Mapped[str | None] = mapped_column(String)
    evidence: Mapped[dict | None] = mapped_column(JSON)

    # Verification
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
    """Computed join paths between tables."""

    __tablename__ = "join_paths"
    __table_args__ = (
        UniqueConstraint("from_table_id", "to_table_id", "path_steps", name="uq_join_path"),
    )

    path_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    path_steps: Mapped[dict] = mapped_column(JSON, nullable=False)
    path_length: Mapped[int] = mapped_column(Integer, nullable=False)
    total_confidence: Mapped[float | None] = mapped_column(Float)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# ============================================================================
# Temporal Metadata Tables
# ============================================================================


class TemporalProfile(Base):
    """Temporal profiles for time columns."""

    __tablename__ = "temporal_profiles"
    __table_args__ = (UniqueConstraint("column_id", name="uq_column_temporal_profile"),)

    temporal_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Range
    min_timestamp: Mapped[datetime | None] = mapped_column(DateTime)
    max_timestamp: Mapped[datetime | None] = mapped_column(DateTime)

    # Granularity
    detected_granularity: Mapped[str | None] = mapped_column(String)
    granularity_confidence: Mapped[float | None] = mapped_column(Float)
    dominant_gap: Mapped[str | None] = mapped_column(String)

    # Completeness
    expected_periods: Mapped[int | None] = mapped_column(Integer)
    actual_periods: Mapped[int | None] = mapped_column(Integer)
    completeness_ratio: Mapped[float | None] = mapped_column(Float)

    # Gaps
    gap_count: Mapped[int | None] = mapped_column(Integer)
    largest_gap: Mapped[str | None] = mapped_column(String)
    gap_details: Mapped[dict | None] = mapped_column(JSON)

    # Patterns
    has_seasonality: Mapped[bool | None] = mapped_column(Boolean)
    seasonality_period: Mapped[str | None] = mapped_column(String)
    trend_direction: Mapped[str | None] = mapped_column(String)

    profiled_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    column: Mapped[Column] = relationship(back_populates="temporal_profile")


# ============================================================================
# Quality Metadata Tables
# ============================================================================


class QualityRule(Base):
    """Quality rules (LLM-generated or manual)."""

    __tablename__ = "quality_rules"

    rule_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Scope
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    column_id: Mapped[str | None] = mapped_column(ForeignKey("columns.column_id"))

    # Rule definition
    rule_name: Mapped[str] = mapped_column(String, nullable=False)
    rule_type: Mapped[str] = mapped_column(String, nullable=False)
    rule_expression: Mapped[str | None] = mapped_column(Text)
    rule_parameters: Mapped[dict | None] = mapped_column(JSON)

    # Metadata
    severity: Mapped[str] = mapped_column(String, default="warning")
    source: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    created_by: Mapped[str | None] = mapped_column(String)

    # Relationships
    table: Mapped[Table] = relationship(back_populates="quality_rules")
    column: Mapped[Column | None] = relationship(back_populates="quality_rules")
    results: Mapped[list[QualityResult]] = relationship(
        back_populates="rule", cascade="all, delete-orphan"
    )


Index("idx_quality_rules_table", QualityRule.table_id)
Index("idx_quality_rules_column", QualityRule.column_id)


class QualityResult(Base):
    """Quality rule execution results."""

    __tablename__ = "quality_results"

    result_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    rule_id: Mapped[str] = mapped_column(
        ForeignKey("quality_rules.rule_id", ondelete="CASCADE"), nullable=False
    )

    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Results
    total_records: Mapped[int | None] = mapped_column(Integer)
    passed_records: Mapped[int | None] = mapped_column(Integer)
    failed_records: Mapped[int | None] = mapped_column(Integer)
    pass_rate: Mapped[float | None] = mapped_column(Float)

    # Failure details
    failure_samples: Mapped[dict | None] = mapped_column(JSON)

    # Trend
    previous_pass_rate: Mapped[float | None] = mapped_column(Float)
    trend_direction: Mapped[str | None] = mapped_column(String)

    # Relationships
    rule: Mapped[QualityRule] = relationship(back_populates="results")


class QualityScore(Base):
    """Aggregate quality scores."""

    __tablename__ = "quality_scores"

    score_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Scope (one of these is set)
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))
    column_id: Mapped[str | None] = mapped_column(ForeignKey("columns.column_id"))

    # Scores by dimension (0-1)
    completeness_score: Mapped[float | None] = mapped_column(Float)
    validity_score: Mapped[float | None] = mapped_column(Float)
    consistency_score: Mapped[float | None] = mapped_column(Float)
    uniqueness_score: Mapped[float | None] = mapped_column(Float)
    timeliness_score: Mapped[float | None] = mapped_column(Float)

    # Overall
    overall_score: Mapped[float | None] = mapped_column(Float)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table | None] = relationship(back_populates="quality_scores")
    column: Mapped[Column | None] = relationship(back_populates="quality_scores")


# ============================================================================
# Ontology Definitions
# ============================================================================


class Ontology(Base):
    """Ontological contexts."""

    __tablename__ = "ontologies"

    ontology_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    version: Mapped[str | None] = mapped_column(String)

    # Content
    concepts: Mapped[dict | None] = mapped_column(JSON)
    metrics: Mapped[dict | None] = mapped_column(JSON)
    quality_rules: Mapped[dict | None] = mapped_column(JSON)
    semantic_hints: Mapped[dict | None] = mapped_column(JSON)

    # Metadata
    is_builtin: Mapped[bool] = mapped_column(Boolean, default=False)
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
    applications: Mapped[list[OntologyApplication]] = relationship(
        back_populates="ontology", cascade="all, delete-orphan"
    )


class OntologyApplication(Base):
    """Ontology application log."""

    __tablename__ = "ontology_applications"

    application_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    ontology_id: Mapped[str] = mapped_column(ForeignKey("ontologies.ontology_id"), nullable=False)

    # Results
    matched_concepts: Mapped[dict | None] = mapped_column(JSON)
    applicable_metrics: Mapped[dict | None] = mapped_column(JSON)
    applied_rules: Mapped[dict | None] = mapped_column(JSON)

    applied_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship(back_populates="ontology_applications")
    ontology: Mapped[Ontology] = relationship(back_populates="applications")


# ============================================================================
# Dataflow Checkpoints (Human-in-Loop)
# ============================================================================


class Checkpoint(Base):
    """Dataflow execution checkpoints for resume capability."""

    __tablename__ = "checkpoints"

    checkpoint_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    dataflow_name: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)

    # Status
    status: Mapped[str] = mapped_column(String, nullable=False)
    checkpoint_type: Mapped[str] = mapped_column(String, nullable=False)

    # Timing
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    resumed_at: Mapped[datetime | None] = mapped_column(DateTime)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)

    # State for resume (Hamilton inputs/outputs)
    checkpoint_state: Mapped[dict | None] = mapped_column(JSON)

    # Results
    result_summary: Mapped[dict | None] = mapped_column(JSON)
    error_message: Mapped[str | None] = mapped_column(Text)

    # Relationships
    review_items: Mapped[list[ReviewQueue]] = relationship(
        back_populates="checkpoint", cascade="all, delete-orphan"
    )


Index("idx_checkpoints_status", Checkpoint.status)


class ReviewQueue(Base):
    """Human review queue."""

    __tablename__ = "review_queue"

    review_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    checkpoint_id: Mapped[str] = mapped_column(
        ForeignKey("checkpoints.checkpoint_id"), nullable=False
    )
    review_type: Mapped[str] = mapped_column(String, nullable=False)
    item_id: Mapped[str] = mapped_column(String, nullable=False)

    # Context
    context_data: Mapped[dict | None] = mapped_column(JSON)
    suggested_action: Mapped[dict | None] = mapped_column(JSON)

    # Status
    status: Mapped[str] = mapped_column(String, default="pending")
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime)
    reviewed_by: Mapped[str | None] = mapped_column(String)
    review_notes: Mapped[str | None] = mapped_column(Text)

    # Relationships
    checkpoint: Mapped[Checkpoint] = relationship(back_populates="review_items")


Index("idx_review_queue_status", ReviewQueue.status)


# ============================================================================
# LLM Response Cache
# ============================================================================


class LLMCache(Base):
    """Cache LLM responses to avoid redundant API calls."""

    __tablename__ = "llm_cache"

    cache_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Cache key (hash of inputs)
    cache_key: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    feature: Mapped[str] = mapped_column(String, nullable=False)

    # Request context
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.source_id"))
    table_ids: Mapped[dict | None] = mapped_column(JSON)
    ontology: Mapped[str | None] = mapped_column(String)

    # LLM details
    provider: Mapped[str] = mapped_column(String, nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    prompt_hash: Mapped[str | None] = mapped_column(String)

    # Response
    response_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)

    # Timing
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Invalidation
    is_valid: Mapped[bool] = mapped_column(Boolean, default=True)


Index("idx_llm_cache_key", LLMCache.cache_key)
Index("idx_llm_cache_feature", LLMCache.feature, LLMCache.source_id)
