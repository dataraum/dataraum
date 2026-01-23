"""Models for Query Agent.

This module defines the data models used by the Query Agent:
- QueryAnalysisOutput: Pydantic model for structured LLM output (tool use)
- QueryResult: Complete result of answering a question
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from dataraum.entropy.contracts import ConfidenceLevel, ContractEvaluation
from dataraum.graphs.models import AssumptionBasis, QueryAssumption


class SQLStepOutput(BaseModel):
    """A single step in SQL generation."""

    step_id: str = Field(description="Unique identifier for this step (e.g., 'filter_active')")
    sql: str = Field(description="SQL fragment for this step (CTE or subquery)")
    description: str = Field(description="Human-readable description of what this step does")


class QueryAssumptionOutput(BaseModel):
    """An assumption made during query analysis."""

    dimension: str = Field(description="Entropy dimension (e.g., 'semantic.units', 'value.nulls')")
    target: str = Field(description="What the assumption applies to (e.g., 'column:orders.amount')")
    assumption: str = Field(description="Human-readable assumption (e.g., 'Currency is EUR')")
    basis: str = Field(
        description="Basis for assumption: 'system_default', 'inferred', or 'user_specified'"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence in this assumption (0.0 to 1.0)"
    )


class QueryAnalysisOutput(BaseModel):
    """Pydantic model for LLM tool output - query analysis and SQL generation.

    Used as a tool definition for structured LLM output via tool use API.
    The LLM analyzes the natural language question, generates SQL, and
    documents any assumptions made due to data uncertainty.
    """

    # Understanding of the question
    interpreted_question: str = Field(
        description="Restatement of the question showing how it was understood"
    )
    metric_type: str = Field(
        description="Type of answer expected: 'scalar', 'table', 'time_series', 'comparison'"
    )

    # SQL generation
    steps: list[SQLStepOutput] = Field(
        default_factory=list,
        description="List of SQL steps, each with step_id, sql, and description",
    )
    final_sql: str = Field(description="Complete executable SQL that answers the question")
    column_mappings: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from question concepts to concrete column names",
    )

    # Assumptions made due to data uncertainty
    assumptions: list[QueryAssumptionOutput] = Field(
        default_factory=list,
        description="Assumptions made due to data uncertainty (entropy)",
    )

    # Validation notes
    validation_notes: list[str] = Field(
        default_factory=list,
        description="Notes about potential issues or caveats with the query",
    )

    # Answer formatting hints
    suggested_format: str = Field(
        default="table",
        description="Suggested result format: 'table', 'scalar', 'chart', 'markdown'",
    )


@dataclass
class QueryResult:
    """Complete result of answering a question via the Query Agent.

    This is the primary return type from answer_question() and contains
    everything needed for CLI, API, and MCP responses.
    """

    # Execution tracking
    execution_id: str
    question: str
    executed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    # The answer
    answer: str = ""  # Natural language response
    sql: str | None = None  # Generated SQL
    data: list[dict[str, Any]] | None = None  # Query results as records
    columns: list[str] | None = None  # Column names in result order

    # Confidence and quality
    confidence_level: ConfidenceLevel = ConfidenceLevel.GREEN
    entropy_score: float = 0.0  # Overall entropy for columns touched
    assumptions: list[QueryAssumption] = field(default_factory=list)

    # Contract evaluation (if contract specified)
    contract: str | None = None
    contract_evaluation: ContractEvaluation | None = None

    # Query analysis details
    interpreted_question: str = ""
    metric_type: str = "table"
    column_mappings: dict[str, str] = field(default_factory=dict)
    validation_notes: list[str] = field(default_factory=list)

    # Error handling
    success: bool = True
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "execution_id": self.execution_id,
            "question": self.question,
            "executed_at": self.executed_at.isoformat(),
            "answer": self.answer,
            "sql": self.sql,
            "data": self.data,
            "columns": self.columns,
            "confidence_level": self.confidence_level.value,
            "confidence_emoji": self.confidence_level.emoji,
            "confidence_label": self.confidence_level.label,
            "entropy_score": round(self.entropy_score, 3),
            "assumptions": [
                {
                    "dimension": a.dimension,
                    "target": a.target,
                    "assumption": a.assumption,
                    "basis": a.basis.value,
                    "confidence": round(a.confidence, 2),
                }
                for a in self.assumptions
            ],
            "contract": self.contract,
            "contract_evaluation": (
                self.contract_evaluation.to_dict() if self.contract_evaluation else None
            ),
            "interpreted_question": self.interpreted_question,
            "metric_type": self.metric_type,
            "column_mappings": self.column_mappings,
            "validation_notes": self.validation_notes,
            "success": self.success,
            "error": self.error,
        }

    def format_cli_response(self) -> str:
        """Format result for CLI display."""
        lines: list[str] = []

        # Confidence header
        emoji = self.confidence_level.emoji
        label = self.confidence_level.label
        contract_name = self.contract or "default"
        lines.append(f"{emoji} Data Quality: {label} for {contract_name}")
        lines.append("")

        if not self.success:
            lines.append(f"Error: {self.error}")
            return "\n".join(lines)

        # Answer
        lines.append(self.answer)

        # Show data table if available
        if self.data and self.columns:
            lines.append("")
            # Simple table formatting
            if len(self.data) <= 20:
                # Header
                header = " | ".join(self.columns)
                lines.append(header)
                lines.append("-" * len(header))
                # Rows
                for row in self.data:
                    row_str = " | ".join(str(row.get(c, "")) for c in self.columns)
                    lines.append(row_str)

        # Assumptions
        if self.assumptions:
            lines.append("")
            lines.append("Assumptions:")
            for a in self.assumptions:
                lines.append(f"  - {a.assumption} ({a.basis.value})")

        # Validation notes
        if self.validation_notes:
            lines.append("")
            lines.append("Notes:")
            for note in self.validation_notes:
                lines.append(f"  - {note}")

        return "\n".join(lines)


def assumption_output_to_query_assumption(
    output: QueryAssumptionOutput,
    execution_id: str,
) -> QueryAssumption:
    """Convert LLM output assumption to QueryAssumption model."""
    basis_map = {
        "system_default": AssumptionBasis.SYSTEM_DEFAULT,
        "inferred": AssumptionBasis.INFERRED,
        "user_specified": AssumptionBasis.USER_SPECIFIED,
    }
    return QueryAssumption.create(
        execution_id=execution_id,
        dimension=output.dimension,
        target=output.target,
        assumption=output.assumption,
        basis=basis_map.get(output.basis, AssumptionBasis.INFERRED),
        confidence=output.confidence,
    )
