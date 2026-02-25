"""SQL Knowledge Base database models.

SQLAlchemy models for persisting SQL snippets and tracking their usage.
Snippets are keyed SQL fragments that grow through usage by both the
Graph Agent (producer) and Query Agent (consumer).

Snippet types:
- extract: Level 1 graph steps (keyed by standard_field + statement + aggregation)
- constant: Parameter-derived values (keyed by parameter_name + parameter_value)
- formula: Level 2+ formulas (keyed by normalized expression pattern)
- query: Query-agent-derived patterns (keyed by semantic hash, discovered via embeddings)

Embeddings for semantic search are stored in the vectors DuckDB database.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base


class SQLSnippetRecord(Base):
    """A keyed SQL fragment in the knowledge base.

    Snippets are discovered and reused across agents:
    - Graph agent produces extract, constant, and formula snippets
    - Query agent discovers snippets via semantic similarity
    - Both track usage for stabilization metrics
    """

    __tablename__ = "sql_snippets"

    __table_args__ = (
        UniqueConstraint(
            "snippet_type",
            "standard_field",
            "statement",
            "aggregation",
            "schema_mapping_id",
            "parameter_value",
            name="uq_snippet_semantic_key",
        ),
    )

    snippet_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    # Discriminator: extract | constant | formula | query
    snippet_type: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # --- Semantic key (for exact match on extract/constant snippets) ---
    standard_field: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    statement: Mapped[str | None] = mapped_column(String, nullable=True)
    aggregation: Mapped[str | None] = mapped_column(String, nullable=True)
    schema_mapping_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    parameter_value: Mapped[str | None] = mapped_column(String, nullable=True)

    # --- Formula template (for expression pattern match) ---
    normalized_expression: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    input_fields: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # --- Content ---
    sql: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    column_mappings: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False, default=dict)

    # --- For semantic search (query-derived snippets) ---
    embedding_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    # --- Provenance ---
    source: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g. "graph:dso", "query:exec_456"
    llm_model: Mapped[str | None] = mapped_column(String, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)

    # --- Quality tracking ---
    is_validated: Mapped[bool] = mapped_column(Integer, nullable=False, default=False)
    execution_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    column_hash: Mapped[str | None] = mapped_column(
        String, nullable=True
    )  # For schema change invalidation

    # --- Timestamps ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # --- Relationships ---
    usages: Mapped[list[SnippetUsageRecord]] = relationship(
        "SnippetUsageRecord",
        back_populates="snippet",
        cascade="all, delete-orphan",
    )


class SnippetUsageRecord(Base):
    """Tracks how a snippet was used in a specific execution.

    Records whether the snippet was exactly reused, adapted, provided but
    not used, or newly generated. This enables stabilization metrics.
    """

    __tablename__ = "snippet_usage"

    usage_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    # --- Execution link ---
    execution_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    execution_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # "graph" | "query"

    # --- Snippet link ---
    snippet_id: Mapped[str | None] = mapped_column(
        ForeignKey("sql_snippets.snippet_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )  # NULL for newly_generated (no snippet existed)

    # --- Usage classification ---
    usage_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # exact_reuse | adapted | provided_not_used | newly_generated
    match_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    sql_match_ratio: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # --- Metadata ---
    step_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # --- Relationships ---
    snippet: Mapped[SQLSnippetRecord | None] = relationship(
        "SQLSnippetRecord",
        back_populates="usages",
        foreign_keys=[snippet_id],
    )


__all__ = ["SQLSnippetRecord", "SnippetUsageRecord"]
