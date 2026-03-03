"""Graph execution persistence database models.

SQLAlchemy models for persisting graph executions and generated code to enable
audit trails, reproducibility, and trend analysis.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base


class GraphExecutionRecord(Base):
    """Persisted graph execution record.

    Stores the result of executing a transformation graph (filter or metric)
    including all step results and final output.
    """

    __tablename__ = "graph_executions"

    execution_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    # Graph identification
    graph_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    graph_type: Mapped[str] = mapped_column(String, nullable=False)  # "filter" | "metric"
    graph_version: Mapped[str] = mapped_column(String, nullable=False)

    # Source tracking
    source: Mapped[str] = mapped_column(String, nullable=False)  # "system" | "user" | "llm"

    # Execution context
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    period: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    is_period_final: Mapped[bool] = mapped_column(Boolean, default=False)

    # Results
    output_value: Mapped[Any] = mapped_column(JSON, nullable=True)
    output_interpretation: Mapped[str | None] = mapped_column(String, nullable=True)

    # Traceability
    execution_hash: Mapped[str] = mapped_column(String, nullable=False, default="")
    executed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Dependencies
    depends_on_executions: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Relationships
    step_results: Mapped[list[StepResultRecord]] = relationship(
        "StepResultRecord",
        back_populates="execution",
        cascade="all, delete-orphan",
    )


class StepResultRecord(Base):
    """Individual step result for drill-down and tracing."""

    __tablename__ = "step_results"

    result_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    execution_id: Mapped[str] = mapped_column(
        ForeignKey("graph_executions.execution_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Step identification
    step_id: Mapped[str] = mapped_column(String, nullable=False)
    level: Mapped[int] = mapped_column(Integer, nullable=False)
    step_type: Mapped[str] = mapped_column(String, nullable=False)

    # Value (polymorphic)
    value_scalar: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_boolean: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    value_string: Mapped[str | None] = mapped_column(String, nullable=True)
    value_json: Mapped[Any | None] = mapped_column(JSON, nullable=True)

    # Classification (for predicate steps)
    classification: Mapped[str | None] = mapped_column(String, nullable=True)
    rows_passed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rows_failed: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Trace information
    inputs_used: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    expression_evaluated: Mapped[str | None] = mapped_column(String, nullable=True)
    source_query: Mapped[str | None] = mapped_column(String, nullable=True)
    rows_affected: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Relationship back to execution
    execution: Mapped[GraphExecutionRecord] = relationship(
        "GraphExecutionRecord", back_populates="step_results"
    )
