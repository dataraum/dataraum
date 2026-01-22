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

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.graphs.db_models import GraphExecutionRecord, StepResultRecord

from .models import (
    Classification,
    ClassificationSummary,
    GraphExecution,
    GraphSource,
    GraphType,
    StepResult,
    StepType,
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


def record_to_execution(record: GraphExecutionRecord) -> GraphExecution:
    """Convert record back to GraphExecution model."""
    execution = GraphExecution(
        execution_id=record.execution_id,
        graph_id=record.graph_id,
        graph_type=GraphType(record.graph_type),
        graph_version=record.graph_version,
        source=GraphSource(record.source),
        parameters=record.parameters,
        period=record.period,
        is_period_final=record.is_period_final,
        output_value=record.output_value,
        output_interpretation=record.output_interpretation,
        execution_hash=record.execution_hash,
        executed_at=record.executed_at,
        depends_on_executions=record.depends_on_executions,
    )

    # Convert step results
    for step_record in record.step_results:
        execution.step_results.append(step_result_record_to_model(step_record))

    return execution


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


def step_result_record_to_model(record: StepResultRecord) -> StepResult:
    """Convert record back to StepResult model."""
    classification = Classification(record.classification) if record.classification else None

    return StepResult(
        step_id=record.step_id,
        level=record.level,
        step_type=StepType(record.step_type),
        value_scalar=record.value_scalar,
        value_boolean=record.value_boolean,
        value_string=record.value_string,
        value_list=record.value_json,
        classification=classification,
        rows_passed=record.rows_passed,
        rows_failed=record.rows_failed,
        inputs_used=record.inputs_used,
        expression_evaluated=record.expression_evaluated,
        source_query=record.source_query,
        rows_affected=record.rows_affected,
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

    def get_execution(self, execution_id: str) -> GraphExecution | None:
        """Get a specific execution by ID.

        Args:
            execution_id: The execution ID

        Returns:
            GraphExecution or None if not found
        """
        stmt = select(GraphExecutionRecord).where(GraphExecutionRecord.execution_id == execution_id)
        result = self.session.execute(stmt)
        record = result.scalar_one_or_none()

        if not record:
            return None

        return record_to_execution(record)

    def get_executions_for_graph(
        self,
        graph_id: str,
        period: str | None = None,
        limit: int = 100,
    ) -> list[GraphExecution]:
        """Get executions for a specific graph.

        Args:
            graph_id: The graph ID to filter by
            period: Optional period to filter by
            limit: Maximum number of results

        Returns:
            List of GraphExecution objects
        """
        stmt = (
            select(GraphExecutionRecord)
            .where(GraphExecutionRecord.graph_id == graph_id)
            .order_by(GraphExecutionRecord.executed_at.desc())
            .limit(limit)
        )

        if period:
            stmt = stmt.where(GraphExecutionRecord.period == period)

        result = self.session.execute(stmt)
        records = result.scalars().all()

        return [record_to_execution(record) for record in records]

    def get_latest_execution(
        self, graph_id: str, period: str | None = None
    ) -> GraphExecution | None:
        """Get the most recent execution for a graph.

        Args:
            graph_id: The graph ID
            period: Optional period filter

        Returns:
            Most recent GraphExecution or None
        """
        executions = self.get_executions_for_graph(graph_id, period, limit=1)
        return executions[0] if executions else None

    def get_final_period_executions(self, graph_id: str) -> list[GraphExecution]:
        """Get all finalized period executions for trend analysis.

        Args:
            graph_id: The graph ID

        Returns:
            List of finalized GraphExecution objects
        """
        stmt = (
            select(GraphExecutionRecord)
            .where(GraphExecutionRecord.graph_id == graph_id)
            .where(GraphExecutionRecord.is_period_final == True)  # noqa: E712
            .order_by(GraphExecutionRecord.period)
        )

        result = self.session.execute(stmt)
        records = result.scalars().all()

        return [record_to_execution(record) for record in records]

    def delete_executions_for_graph(self, graph_id: str) -> int:
        """Delete all executions for a graph.

        Args:
            graph_id: The graph ID

        Returns:
            Number of deleted records
        """
        stmt = select(GraphExecutionRecord).where(GraphExecutionRecord.graph_id == graph_id)
        result = self.session.execute(stmt)
        records = result.scalars().all()

        count = len(records)
        for record in records:
            self.session.delete(record)

        return count
