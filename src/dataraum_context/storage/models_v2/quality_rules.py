"""Quality Rules Models.

SQLAlchemy models for rule-based quality framework:
- Quality rules (LLM-generated or manual)
- Quality results (rule execution results)
- Quality scores (aggregate quality dimensions)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Column, Table


class QualityRule(Base):
    """Quality rules (LLM-generated or manual).

    Defines data quality rules that can be executed against tables/columns.
    """

    __tablename__ = "quality_rules"

    rule_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Scope
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id")
    )  # NULL = table-level rule

    # Rule definition
    rule_name: Mapped[str] = mapped_column(String, nullable=False)
    rule_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'range', 'regex', 'uniqueness', 'referential', 'custom'
    rule_expression: Mapped[str | None] = mapped_column(
        Text
    )  # SQL WHERE clause or other expression
    rule_parameters: Mapped[dict | None] = mapped_column(JSON)

    # Metadata
    severity: Mapped[str] = mapped_column(
        String, default="warning"
    )  # 'info', 'warning', 'error', 'critical'
    source: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'llm', 'manual', 'config', 'heuristic'
    description: Mapped[str | None] = mapped_column(Text)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    created_by: Mapped[str | None] = mapped_column(String)

    # Relationships
    table: Mapped["Table"] = relationship(back_populates="quality_rules")
    column: Mapped["Column | None"] = relationship(back_populates="quality_rules")
    results: Mapped[list["QualityResult"]] = relationship(
        back_populates="rule", cascade="all, delete-orphan"
    )


Index("idx_quality_rules_table", QualityRule.table_id)
Index("idx_quality_rules_column", QualityRule.column_id)


class QualityResult(Base):
    """Quality rule execution results.

    Records the outcome of executing a quality rule.
    """

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
    failure_samples: Mapped[dict | None] = mapped_column(
        JSON
    )  # Sample of failing records for investigation

    # Trend
    previous_pass_rate: Mapped[float | None] = mapped_column(Float)
    trend_direction: Mapped[str | None] = mapped_column(
        String
    )  # 'improving', 'stable', 'degrading'

    # Relationships
    rule: Mapped["QualityRule"] = relationship(back_populates="results")


class QualityScore(Base):
    """Aggregate quality scores.

    Computes quality dimensions (completeness, validity, etc.) at table or column level.
    """

    __tablename__ = "quality_scores"

    score_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Scope (one of these is set)
    table_id: Mapped[str | None] = mapped_column(ForeignKey("tables.table_id"))
    column_id: Mapped[str | None] = mapped_column(ForeignKey("columns.column_id"))

    # Scores by dimension (0-1 scale)
    completeness_score: Mapped[float | None] = mapped_column(Float)
    validity_score: Mapped[float | None] = mapped_column(Float)
    consistency_score: Mapped[float | None] = mapped_column(Float)
    uniqueness_score: Mapped[float | None] = mapped_column(Float)
    timeliness_score: Mapped[float | None] = mapped_column(Float)
    accuracy_score: Mapped[float | None] = mapped_column(Float)

    # Overall
    overall_score: Mapped[float | None] = mapped_column(Float)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped["Table | None"] = relationship(back_populates="quality_scores")
    column: Mapped["Column | None"] = relationship(back_populates="quality_scores")
