"""E2E tests: verify vertical-specific pipeline phases.

Tests graph execution (finance metrics), validation checks,
and business cycle detection against clean pipeline output.

GROUND TRUTH: Do not modify assertions to fix failures — fix the production code instead.
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
from dataraum.analysis.validation.db_models import ValidationResultRecord
from dataraum.graphs.db_models import GraphExecutionRecord, StepResultRecord
from dataraum.pipeline.runner import RunResult
from dataraum.query.snippet_models import SQLSnippetRecord

pytestmark = pytest.mark.e2e


# =============================================================================
# Graph execution (finance metrics: DSO, DPO, cash_conversion_cycle, etc.)
# =============================================================================


class TestGraphExecution:
    """Verify the graph_execution phase produced metric results."""

    def test_graph_executions_exist(self, metadata_session: Session) -> None:
        """At least one graph execution should have been recorded."""
        count = metadata_session.execute(
            select(func.count()).select_from(GraphExecutionRecord)
        ).scalar()
        assert count is not None and count > 0, "No graph executions found"

    def test_graph_steps_have_values(self, metadata_session: Session) -> None:
        """Each execution's step results should have values populated."""
        steps = metadata_session.execute(select(StepResultRecord)).scalars().all()
        assert len(steps) > 0, "No step results found"

        with_value = [
            s
            for s in steps
            if s.value_scalar is not None
            or s.value_json is not None
            or s.value_boolean is not None
            or s.value_string is not None
        ]
        assert len(with_value) > 0, (
            f"No step results have any value populated out of {len(steps)} steps"
        )

    def test_sql_snippets_created(self, metadata_session: Session) -> None:
        """Graph execution should store reusable SQL snippets."""
        count = metadata_session.execute(
            select(func.count()).select_from(SQLSnippetRecord)
        ).scalar()
        assert count is not None and count > 0, "No SQL snippets created"


# =============================================================================
# Validation (trial_balance, double_entry_balance, etc.)
# =============================================================================


class TestValidation:
    """Verify the validation phase ran checks against clean data."""

    def test_validation_results_exist(self, metadata_session: Session) -> None:
        """Validation results should be recorded."""
        count = metadata_session.execute(
            select(func.count()).select_from(ValidationResultRecord)
        ).scalar()
        assert count is not None and count > 0, "No validation results found"

    def test_clean_data_passes_double_entry(self, metadata_session: Session) -> None:
        """The double_entry_balance validation should pass on clean data."""
        results = (
            metadata_session.execute(
                select(ValidationResultRecord).where(
                    ValidationResultRecord.validation_id.contains("double_entry")
                )
            )
            .scalars()
            .all()
        )
        if results:
            # At least one double_entry validation should pass
            passed = [r for r in results if r.passed]
            assert len(passed) > 0, (
                f"double_entry_balance failed on clean data: "
                f"{[(r.validation_id, r.message) for r in results]}"
            )

    def test_validation_has_sql(self, metadata_session: Session) -> None:
        """Validation results should have sql_used populated."""
        results = (
            metadata_session.execute(
                select(ValidationResultRecord).where(ValidationResultRecord.sql_used.isnot(None))
            )
            .scalars()
            .all()
        )
        assert len(results) > 0, "No validation results have sql_used populated"


# =============================================================================
# Business cycles
# =============================================================================


class TestBusinessCycles:
    """Verify the business_cycles phase detected cycles in clean data."""

    def test_cycles_detected(self, pipeline_run: RunResult, metadata_session: Session) -> None:
        """DetectedBusinessCycle entries should exist."""
        count = metadata_session.execute(
            select(func.count())
            .select_from(DetectedBusinessCycle)
            .where(DetectedBusinessCycle.source_id == pipeline_run.source_id)
        ).scalar()
        assert count is not None and count > 0, "No business cycles detected"

    def test_cycles_have_structure(
        self, pipeline_run: RunResult, metadata_session: Session
    ) -> None:
        """Detected cycles should have tables_involved and stages populated."""
        cycles = (
            metadata_session.execute(
                select(DetectedBusinessCycle).where(
                    DetectedBusinessCycle.source_id == pipeline_run.source_id
                )
            )
            .scalars()
            .all()
        )
        assert len(cycles) > 0

        for cycle in cycles:
            assert cycle.tables_involved and len(cycle.tables_involved) > 0, (
                f"Cycle '{cycle.cycle_name}' has no tables_involved"
            )
            assert cycle.stages and len(cycle.stages) > 0, (
                f"Cycle '{cycle.cycle_name}' has no stages"
            )

    def test_known_cycle_types(self, pipeline_run: RunResult, metadata_session: Session) -> None:
        """At least one cycle should match the vocabulary (is_known_type=True)."""
        count = metadata_session.execute(
            select(func.count())
            .select_from(DetectedBusinessCycle)
            .where(
                DetectedBusinessCycle.source_id == pipeline_run.source_id,
                DetectedBusinessCycle.is_known_type.is_(True),
            )
        ).scalar()
        assert count is not None and count > 0, "No detected cycles matched the known vocabulary"
