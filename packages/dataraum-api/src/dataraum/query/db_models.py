"""Query library database models.

SQLAlchemy models for persisting queries to enable reuse via similarity search.
Queries can originate from:
- Natural language questions (QueryAgent)
- Graph definitions (seeded from graphs/)

Embeddings are stored separately in vectors.duckdb for similarity search.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base


class QueryLibraryEntry(Base):
    """A saved query for reuse via similarity search.

    Queries are scoped per-source since different datasets have different
    semantics (e.g., "revenue" means different things in different contexts).

    Embedding vectors are stored in a separate DuckDB file (vectors.duckdb)
    and linked via query_id.
    """

    __tablename__ = "query_library"

    query_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(
        ForeignKey("sources.source_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Origin - either NL question OR graph definition (mutually exclusive)
    original_question: Mapped[str | None] = mapped_column(Text, nullable=True)
    graph_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    # User-provided metadata
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Generated SQL and semantic content (aligned with GeneratedCodeRecord)
    summary: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # Plain English description of what the query calculates
    steps_json: Mapped[list[dict[str, str]]] = mapped_column(
        JSON, nullable=False, default=list
    )  # [{step_id, sql, description}]
    final_sql: Mapped[str] = mapped_column(Text, nullable=False)
    column_mappings: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False, default=dict)

    # Assumptions made during generation
    assumptions: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)

    # Contract and confidence
    contract_name: Mapped[str | None] = mapped_column(String, nullable=True)
    confidence_level: Mapped[str] = mapped_column(
        String, nullable=False, default="GREEN"
    )  # GREEN/YELLOW/ORANGE/RED

    # Embedding metadata (actual vector in DuckDB)
    embedding_text: Mapped[str] = mapped_column(Text, nullable=False)
    embedding_model: Mapped[str] = mapped_column(String, nullable=False, default="all-MiniLM-L6-v2")

    # Usage tracking
    usage_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Validation - has the query been verified by a human?
    is_validated: Mapped[bool] = mapped_column(
        Integer, nullable=False, default=False
    )  # SQLite uses INTEGER for bool
    validated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    validated_by: Mapped[str | None] = mapped_column(String, nullable=True)

    # Relationships
    executions: Mapped[list[QueryExecutionRecord]] = relationship(
        "QueryExecutionRecord",
        back_populates="library_entry",
        cascade="all, delete-orphan",
    )


class QueryExecutionRecord(Base):
    """Record of a query execution, linked to library entry if reused.

    Tracks each time a query is executed, whether from:
    - Fresh generation (no library_entry_id)
    - Library reuse (has library_entry_id)
    """

    __tablename__ = "query_executions"

    execution_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    # Link to library entry (if reused)
    library_entry_id: Mapped[str | None] = mapped_column(
        ForeignKey("query_library.query_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Source context
    source_id: Mapped[str] = mapped_column(
        ForeignKey("sources.source_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # The question asked
    question: Mapped[str] = mapped_column(Text, nullable=False)

    # Execution details
    sql_executed: Mapped[str] = mapped_column(Text, nullable=False)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Results summary
    success: Mapped[bool] = mapped_column(Integer, nullable=False, default=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Confidence at execution time
    confidence_level: Mapped[str] = mapped_column(String, nullable=False, default="GREEN")
    contract_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Similarity score (if reused from library)
    similarity_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Relationships
    library_entry: Mapped[QueryLibraryEntry | None] = relationship(
        "QueryLibraryEntry",
        back_populates="executions",
    )


__all__ = ["QueryLibraryEntry", "QueryExecutionRecord"]
