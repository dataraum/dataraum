"""Tests for the validation phase — the lifecycle orchestrator (DAT-438).

The LLM machinery (bind/execute) is mocked at the agent boundary; everything
else — spec loading, lifecycle artifacts, persistence, supersession — runs
against the real session fixture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.validation.db_models import ValidationResultRecord
from dataraum.analysis.validation.models import (
    ValidationResult,
    ValidationSeverity,
    ValidationSpec,
    ValidationStatus,
)
from dataraum.investigation.db_models import InvestigationSession
from dataraum.lifecycle import ArtifactState, LifecycleArtifact
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.validation_phase import ValidationPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb

_SESSION_ID = "sess-validation-phase"


@pytest.fixture()
def _mock_llm():
    """Patch LLM infrastructure so the phase can initialize without config."""
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}
    mock_config.limits.max_output_tokens_per_request = 8000

    with (
        patch(
            "dataraum.pipeline.phases.validation_phase.load_llm_config",
            return_value=mock_config,
        ),
        patch(
            "dataraum.pipeline.phases.validation_phase.create_provider",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.validation_phase.PromptRenderer",
            return_value=MagicMock(),
        ),
    ):
        yield


@pytest.fixture
def workspace_table(session: Session) -> Table:
    """A typed table with a column + the journey session the FKs need."""
    session.add(InvestigationSession(session_id=_SESSION_ID, intent="test"))
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="journal_entries",
        layer="typed",
        duckdb_path="typed_journal_entries",
        row_count=10,
    )
    session.add(table)
    session.flush()
    session.add(
        Column(
            table_id=table.table_id,
            column_name="amount",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="DECIMAL(18,2)",
        )
    )
    session.commit()
    return table


def _make_ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str = "run-om-1",
) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=None,  # source-free: operating_model scope is table_ids
        table_ids=table_ids,
        session_id=_SESSION_ID,
        run_id=run_id,
        # base_runs is the workflow-resolved pin (ADR-0008), threaded by the
        # validation activity; empty pins are legitimate (fail-closed reads).
        config={"vertical": "finance", "base_runs": {}},
    )


def _spec(validation_id: str) -> ValidationSpec:
    return ValidationSpec(
        validation_id=validation_id,
        name=validation_id,
        description="test spec",
        category="financial",
        check_type="balance",
    )


def _result(validation_id: str, status: ValidationStatus, message: str = "") -> ValidationResult:
    return ValidationResult(
        validation_id=validation_id,
        spec_name=validation_id,
        status=status,
        severity=ValidationSeverity.ERROR,
        table_name="journal_entries",
        passed=status == ValidationStatus.PASSED,
        message=message,
        sql_used="SELECT 1" if status != ValidationStatus.SKIPPED else None,
    )


def _artifacts(session: Session, run_id: str) -> dict[str, LifecycleArtifact]:
    rows = (
        session.execute(select(LifecycleArtifact).where(LifecycleArtifact.run_id == run_id))
        .scalars()
        .all()
    )
    return {a.artifact_key: a for a in rows}


class TestValidationPhaseOutcomes:
    def test_fails_without_table_ids(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        result = ValidationPhase()._run(_make_ctx(session, duckdb_conn, table_ids=[]))
        assert result.status == PhaseStatus.FAILED
        assert "No tables in session scope" in (result.error or "")

    def test_no_vertical_is_loud_explicit_outcome(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, workspace_table: Table
    ) -> None:
        ctx = _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        ctx.config = {}

        result = ValidationPhase()._run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_vertical"
        assert result.outputs["declared"] == 0
        assert _artifacts(session, "run-om-1") == {}

    @patch("dataraum.pipeline.phases.validation_phase.load_all_validation_specs")
    def test_no_specs_is_loud_explicit_outcome(
        self,
        mock_load: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        mock_load.return_value = {}

        result = ValidationPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_declared_validations"

    def test_missing_base_runs_pin_fails_loud(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, workspace_table: Table
    ) -> None:
        """No per-phase head resolution (ADR-0008): an unthreaded pin is a wiring bug."""
        ctx = _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        ctx.config = {"vertical": "finance"}  # no base_runs

        result = ValidationPhase()._run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "base_runs missing" in (result.error or "")


class TestValidationLifecycleFlow:
    @patch("dataraum.analysis.validation.agent.ValidationAgent.execute_validation")
    @patch("dataraum.analysis.validation.agent.ValidationAgent.bind_validation")
    @patch("dataraum.pipeline.phases.validation_phase.load_all_validation_specs")
    def test_declared_bind_execute_flow(
        self,
        mock_load: MagicMock,
        mock_bind: MagicMock,
        mock_execute: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """One spec executes; one is ungroundable and stays declared with reason."""
        mock_load.return_value = {
            "double_entry": _spec("double_entry"),
            "three_way_match": _spec("three_way_match"),
        }
        generated = MagicMock(sql_query="SELECT 1")
        mock_bind.side_effect = [
            (generated, None),  # double_entry grounds
            (None, _result("three_way_match", ValidationStatus.SKIPPED, "no PO table")),
        ]
        mock_execute.return_value = _result("double_entry", ValidationStatus.PASSED, "balanced")

        result = ValidationPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        artifacts = _artifacts(session, "run-om-1")
        assert artifacts["double_entry"].state == ArtifactState.EXECUTED.value
        assert artifacts["double_entry"].grounded_against is not None
        assert artifacts["three_way_match"].state == ArtifactState.DECLARED.value
        assert artifacts["three_way_match"].state_reason == "no PO table"
        assert artifacts["three_way_match"].teaches["vertical"] == "finance"

        records = session.execute(select(ValidationResultRecord)).scalars().all()
        assert {(r.validation_id, r.status, r.run_id) for r in records} == {
            ("double_entry", "passed", "run-om-1"),
            ("three_way_match", "skipped", "run-om-1"),
        }
        assert result.outputs["executed"] == 1
        assert result.outputs["stuck_declared"] == 1

    @patch("dataraum.analysis.validation.agent.ValidationAgent.execute_validation")
    @patch("dataraum.analysis.validation.agent.ValidationAgent.bind_validation")
    @patch("dataraum.pipeline.phases.validation_phase.load_all_validation_specs")
    def test_execution_error_stays_grounded_with_reason(
        self,
        mock_load: MagicMock,
        mock_bind: MagicMock,
        mock_execute: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        mock_load.return_value = {"double_entry": _spec("double_entry")}
        mock_bind.return_value = (MagicMock(sql_query="SELECT 1"), None)
        mock_execute.return_value = _result(
            "double_entry", ValidationStatus.ERROR, "SQL execution error: boom"
        )

        ValidationPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        session.flush()

        artifact = _artifacts(session, "run-om-1")["double_entry"]
        assert artifact.state == ArtifactState.GROUNDED.value
        assert "boom" in (artifact.state_reason or "")

    @patch("dataraum.analysis.validation.agent.ValidationAgent.execute_validation")
    @patch("dataraum.analysis.validation.agent.ValidationAgent.bind_validation")
    @patch("dataraum.pipeline.phases.validation_phase.load_all_validation_specs")
    def test_rerun_supersedes_under_fresh_run_id(
        self,
        mock_load: MagicMock,
        mock_bind: MagicMock,
        mock_execute: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """No skip-if-already-ran: the re-run re-flows everything; runs coexist."""
        mock_load.return_value = {"double_entry": _spec("double_entry")}
        mock_bind.return_value = (MagicMock(sql_query="SELECT 1"), None)
        mock_execute.return_value = _result("double_entry", ValidationStatus.PASSED)

        phase = ValidationPhase()
        r1 = phase._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id], "run-1"))
        session.flush()
        r2 = phase._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id], "run-2"))
        session.flush()

        assert r1.status == r2.status == PhaseStatus.COMPLETED
        session.expire_all()
        artifacts = session.execute(select(LifecycleArtifact)).scalars().all()
        assert {(a.run_id, a.state) for a in artifacts} == {
            ("run-1", "executed"),
            ("run-2", "executed"),
        }
        records = session.execute(select(ValidationResultRecord)).scalars().all()
        assert {r.run_id for r in records} == {"run-1", "run-2"}
