"""Graph execution persistence.

Repository and converter functions for persisting graph executions.
SQLAlchemy models are in graphs/db_models.py.

Usage:
    from dataraum.graphs.persistence import GraphExecutionRepository

    repo = GraphExecutionRepository(session)
    repo.save_execution(execution)

    # Query executions
    executions = repo.get_executions_for_graph("dso", period="2025-Q1")
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from dataraum.graphs.db_models import GraphExecutionRecord, StepResultRecord

from .models import (
    ClassificationSummary,
    GraphExecution,
    StepResult,
)


def execution_to_record(execution: GraphExecution) -> GraphExecutionRecord:
    """Create record from GraphExecution model."""
    # Serialize output_value appropriately
    output_value = execution.output_value
    if isinstance(output_value, ClassificationSummary):
        output_value = {
            "clean_count": output_value.clean_count,
            "exclude_count": output_value.exclude_count,
            "quarantine_count": output_value.quarantine_count,
            "flag_count": output_value.flag_count,
            "total_count": output_value.total_count,
        }

    return GraphExecutionRecord(
        execution_id=execution.execution_id,
        graph_id=execution.graph_id,
        graph_type=execution.graph_type.value,
        graph_version=execution.graph_version,
        source=execution.source.value,
        parameters=execution.parameters,
        period=execution.period,
        is_period_final=execution.is_period_final,
        output_value=output_value,
        output_interpretation=execution.output_interpretation,
        execution_hash=execution.execution_hash,
        executed_at=execution.executed_at,
        depends_on_executions=execution.depends_on_executions,
    )


def step_result_to_record(result: StepResult, execution_id: str) -> StepResultRecord:
    """Create record from StepResult model."""
    classification_str = result.classification.value if result.classification else None

    return StepResultRecord(
        execution_id=execution_id,
        step_id=result.step_id,
        level=result.level,
        step_type=result.step_type.value,
        value_scalar=result.value_scalar,
        value_boolean=result.value_boolean,
        value_string=result.value_string,
        value_json=result.value_list,
        classification=classification_str,
        rows_passed=result.rows_passed,
        rows_failed=result.rows_failed,
        inputs_used=result.inputs_used,
        expression_evaluated=result.expression_evaluated,
        source_query=result.source_query,
        rows_affected=result.rows_affected,
    )


class GraphExecutionRepository:
    """Repository for graph execution persistence operations."""

    def __init__(self, session: Session):
        self.session = session

    def save_execution(self, execution: GraphExecution) -> GraphExecutionRecord:
        """Save a graph execution with all step results.

        Args:
            execution: The GraphExecution to save

        Returns:
            The persisted GraphExecutionRecord
        """
        record = execution_to_record(execution)

        # Create step result records
        for step_result in execution.step_results:
            step_record = step_result_to_record(step_result, execution.execution_id)
            record.step_results.append(step_record)

        self.session.add(record)
        # No flush needed - execution_id is client-generated UUID, available immediately
        return record
