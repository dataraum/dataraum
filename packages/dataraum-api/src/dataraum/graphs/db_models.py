"""Graph execution persistence database models.

SQLAlchemy models for persisting graph executions and generated code to enable
audit trails, reproducibility, and trend analysis.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base


class GeneratedCodeRecord(Base):
    """Persisted LLM-generated SQL for a graph + schema combination.

    This enables:
    - Cache persistence across restarts (no need to regenerate)
    - Audit trail of what SQL was generated
    - Reproducibility verification
    """

    __tablename__ = "generated_code"

    code_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    # Graph identification
    graph_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    graph_version: Mapped[str] = mapped_column(String, nullable=False)
    schema_mapping_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    # Generated SQL
    steps_json: Mapped[list[dict[str, str]]] = mapped_column(
        JSON, nullable=False, default=list
    )  # [{step_id, sql, description}]
    final_sql: Mapped[str] = mapped_column(Text, nullable=False)
    column_mappings: Mapped[dict[str, str]] = mapped_column(
        JSON, nullable=False, default=dict
    )  # abstract_field -> concrete_column

    # Generation metadata
    llm_model: Mapped[str] = mapped_column(String, nullable=False)
    prompt_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Validation
    is_validated: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_errors: Mapped[list[str]] = mapped_column(JSON, default=list)

    # Relationships - executions that used this code
    executions: Mapped[list[GraphExecutionRecord]] = relationship(
        "GraphExecutionRecord",
        back_populates="generated_code",
        foreign_keys="GraphExecutionRecord.generated_code_id",
    )


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

    # Link to generated code (for agent-based execution)
    generated_code_id: Mapped[str | None] = mapped_column(
        ForeignKey("generated_code.code_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Relationships
    step_results: Mapped[list[StepResultRecord]] = relationship(
        "StepResultRecord",
        back_populates="execution",
        cascade="all, delete-orphan",
    )
    generated_code: Mapped[GeneratedCodeRecord | None] = relationship(
        "GeneratedCodeRecord",
        back_populates="executions",
        foreign_keys=[generated_code_id],
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
