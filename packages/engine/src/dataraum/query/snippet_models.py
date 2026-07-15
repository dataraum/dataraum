"""SQL Knowledge Base database models.

SQLAlchemy models for persisting SQL snippets and tracking their usage.
Snippets are keyed SQL fragments that grow through usage by both the
Graph Agent (producer) and Query Agent (consumer).

Snippet types:
- extract: Level 1 graph steps (keyed by standard_field + statement + aggregation) —
  the sole shared, cross-metric cache the graph agent discovers by key.
- constant: Parameter-derived values (keyed by parameter_name + parameter_value).
- formula: a metric's composed computation, persisted PER-METRIC (keyed by source +
  expression, DAT-646) — never shared across metrics by expression shape. The cockpit
  reuse KB groups it by ``source`` (``graph:{graph_id}``); the engine does not look it
  up by shape.
- query: Query-agent-derived patterns (keyed by semantic hash).

Cockpit consumer discovery groups snippets by ``source`` and matches term-based
vocabulary against standard_field, statement, and aggregation values.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class SQLSnippetRecord(Base):
    """A keyed SQL fragment in the knowledge base.

    Snippets are discovered and reused across agents:
    - Graph agent produces extract, constant, and formula snippets
    - Query agent discovers snippets via term-based vocabulary matching
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
        CheckConstraint(
            "snippet_type IN ('extract', 'constant', 'formula', 'query')",
            name="snippet_type",
        ),
    )

    snippet_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    workspace_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

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

    # --- Provenance ---
    source: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g. "graph:dso", "query:exec_456"
    provenance: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # --- Clause parts (DAT-671, parts-at-source; EXTRACT snippets only) ---
    # The structured artifact `sql` is rendered from — `{select: [{expr, alias}],
    # from: [relation], where: [pred, …]}`. The cockpit drill builder composes
    # every variant (scalar, sliced, pinned) from these parts without parsing
    # SQL; `sql` is their one-time render (compose_extract_sql), kept for
    # display and non-drill consumers. NULL on formula/constant/query snippets
    # and on rows authored before the cut (healed by re-injection).
    parts: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # --- Quality tracking ---
    execution_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # --- Timestamps ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = ["SQLSnippetRecord"]
