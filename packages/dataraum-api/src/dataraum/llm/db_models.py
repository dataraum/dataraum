"""LLM Integration Database Models.

SQLAlchemy models for LLM response caching.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class LLMCache(Base):
    """Cache LLM responses to avoid redundant API calls.

    Uses a cache_key (hash of inputs) to detect duplicate requests
    and return cached responses, reducing API costs and latency.
    """

    __tablename__ = "llm_cache"

    cache_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Cache key (hash of inputs)
    cache_key: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    feature: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'semantic_analysis', 'rule_generation', etc.

    # Request context
    source_id: Mapped[str | None] = mapped_column(ForeignKey("sources.source_id"))
    table_ids: Mapped[dict[str, Any] | None] = mapped_column(JSON)  # List of table IDs
    ontology: Mapped[str | None] = mapped_column(String)

    # LLM details
    provider: Mapped[str] = mapped_column(String, nullable=False)  # 'anthropic', 'openai', etc.
    model: Mapped[str] = mapped_column(String, nullable=False)  # 'claude-3-5-sonnet-20241022', etc.
    prompt_hash: Mapped[str | None] = mapped_column(String)

    # Response
    response_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)

    # Timing and expiration
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Invalidation
    is_valid: Mapped[bool] = mapped_column(Boolean, default=True)


Index("idx_llm_cache_key", LLMCache.cache_key)
Index("idx_llm_cache_feature", LLMCache.feature, LLMCache.source_id)
