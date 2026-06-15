"""Tests for the business cycles phase — the second lifecycle family (DAT-455).

Mirrors the validation phase's lifecycle tests: the LLM machinery (the cycle
synthesis call) is mocked at the agent boundary; everything else — vocabulary
loading, lifecycle artifacts, persistence, supersession — runs against the real
session fixture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
from dataraum.analysis.cycles.models import BusinessCycleAnalysis, DetectedCycle
from dataraum.core.models.base import Result
from dataraum.lifecycle import ArtifactState, LifecycleArtifact
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.business_cycles_phase import BusinessCyclesPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb

_SESSION_ID = "sess-cycles-phase"


@pytest.fixture()
def _mock_llm():
    """Patch LLM infrastructure so the phase can initialize without config."""
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}
    mock_config.limits.max_output_tokens_per_request = 8000

    with (
        patch(
            "dataraum.pipeline.phases.business_cycles_phase.load_llm_config",
            return_value=mock_config,
        ),
        patch(
            "dataraum.pipeline.phases.business_cycles_phase.create_provider",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.business_cycles_phase.PromptRenderer",
            return_value=MagicMock(),
        ),
    ):
        yield


@pytest.fixture
def workspace_table(session: Session) -> Table:
    """A typed table with a column."""
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="invoices",
        layer="typed",
        duckdb_path="typed_invoices",
        row_count=10,
    )
    session.add(table)
    session.flush()
    session.add(
        Column(
            table_id=table.table_id,
            column_name="status",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",
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
        table_ids=table_ids,
        run_id=run_id,
        # base_runs is the workflow-resolved pin (ADR-0008), threaded by the
        # business_cycles activity; empty pins are legitimate (fail-closed reads).
        config={"vertical": "finance", "base_runs": {}},
    )


def _detected(
    canonical_type: str,
    *,
    completion_rate: float | None = 0.85,
) -> DetectedCycle:
    return DetectedCycle(
        cycle_id=str(uuid4()),
        cycle_name=canonical_type.replace("_", " ").title(),
        cycle_type=canonical_type,
        canonical_type=canonical_type,
        is_known_type=True,
        description="test cycle",
        tables_involved=["invoices"],
        status_column="status",
        completion_rate=completion_rate,
        confidence=0.9,
    )


def _analysis(cycles: list[DetectedCycle]) -> BusinessCycleAnalysis:
    return BusinessCycleAnalysis(
        analysis_id=str(uuid4()),
        tables_analyzed=["invoices"],
        cycles=cycles,
        total_cycles_detected=len(cycles),
    )


def _artifacts(session: Session, run_id: str) -> dict[str, LifecycleArtifact]:
    rows = (
        session.execute(select(LifecycleArtifact).where(LifecycleArtifact.run_id == run_id))
        .scalars()
        .all()
    )
    return {a.artifact_key: a for a in rows}


class TestBusinessCyclesPhaseOutcomes:
    def test_fails_without_table_ids(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        result = BusinessCyclesPhase()._run(_make_ctx(session, duckdb_conn, table_ids=[]))
        assert result.status == PhaseStatus.FAILED
        assert "No tables in session scope" in (result.error or "")

    def test_no_vertical_is_loud_explicit_outcome(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, workspace_table: Table
    ) -> None:
        ctx = _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        ctx.config = {}

        result = BusinessCyclesPhase()._run(ctx)

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_vertical"
        assert result.outputs["declared"] == 0
        assert _artifacts(session, "run-om-1") == {}

    @patch("dataraum.pipeline.phases.business_cycles_phase.get_cycle_types")
    def test_no_declared_cycles_is_loud_explicit_outcome(
        self,
        mock_types: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        mock_types.return_value = {}

        result = BusinessCyclesPhase()._run(
            _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        )

        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_declared_cycles"

    def test_missing_base_runs_pin_fails_loud(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, workspace_table: Table
    ) -> None:
        """No per-phase head resolution (ADR-0008): an unthreaded pin is a wiring bug."""
        ctx = _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        ctx.config = {"vertical": "finance"}  # no base_runs

        result = BusinessCyclesPhase()._run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "base_runs missing" in (result.error or "")

    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.ground_cycles")
    @patch("dataraum.pipeline.phases.business_cycles_phase.get_cycle_types")
    def test_grounding_failure_fails_loud(
        self,
        mock_types: MagicMock,
        mock_ground: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """A hard synthesis failure (no tool call / LLM error) fails the phase."""
        mock_types.return_value = {"order_to_cash": {"business_value": "high"}}
        mock_ground.return_value = Result.fail("LLM did not call submit_analysis tool")

        result = BusinessCyclesPhase()._run(
            _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        )

        assert result.status == PhaseStatus.FAILED
        assert "submit_analysis" in (result.error or "")


class TestCycleLifecycleFlow:
    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.ground_cycles")
    @patch("dataraum.pipeline.phases.business_cycles_phase.get_cycle_types")
    def test_declared_bind_execute_flow(
        self,
        mock_types: MagicMock,
        mock_ground: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """One cycle executes; one is ungroundable; one is detected-but-unmeasured."""
        mock_types.return_value = {
            "order_to_cash": {"business_value": "high"},
            "accounts_payable": {"business_value": "high"},
            "period_close": {"business_value": "high"},
        }
        # order_to_cash grounds + measures; period_close grounds but has no
        # completion measurement; accounts_payable is not detected at all.
        mock_ground.return_value = Result.ok(
            _analysis(
                [
                    _detected("order_to_cash", completion_rate=0.85),
                    _detected("period_close", completion_rate=None),
                ]
            )
        )

        result = BusinessCyclesPhase()._run(
            _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        artifacts = _artifacts(session, "run-om-1")
        assert artifacts["order_to_cash"].state == ArtifactState.EXECUTED.value
        assert artifacts["order_to_cash"].grounded_against is not None
        assert artifacts["period_close"].state == ArtifactState.GROUNDED.value
        assert "no completion measurement" in (artifacts["period_close"].state_reason or "")
        assert artifacts["accounts_payable"].state == ArtifactState.DECLARED.value
        assert "not detected" in (artifacts["accounts_payable"].state_reason or "")
        assert artifacts["order_to_cash"].teaches["vertical"] == "finance"

        # Only the two GROUNDED cycles persist a DetectedBusinessCycle row.
        cycles = session.execute(select(DetectedBusinessCycle)).scalars().all()
        assert {(c.canonical_type, c.run_id) for c in cycles} == {
            ("order_to_cash", "run-om-1"),
            ("period_close", "run-om-1"),
        }
        assert result.outputs["executed"] == 1
        assert result.outputs["stuck_grounded"] == 1
        assert result.outputs["stuck_declared"] == 1
        assert result.outputs["detected_cycles"] == 2

    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.ground_cycles")
    @patch("dataraum.pipeline.phases.business_cycles_phase.get_cycle_types")
    def test_off_vocabulary_detection_is_dropped(
        self,
        mock_types: MagicMock,
        mock_ground: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """A detected cycle whose type was not declared has no artifact — dropped, not persisted."""
        mock_types.return_value = {"order_to_cash": {"business_value": "high"}}
        mock_ground.return_value = Result.ok(
            _analysis(
                [
                    _detected("order_to_cash"),
                    _detected("some_undeclared_cycle"),  # not in the declared set
                ]
            )
        )

        result = BusinessCyclesPhase()._run(
            _make_ctx(session, duckdb_conn, [workspace_table.table_id])
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        cycles = session.execute(select(DetectedBusinessCycle)).scalars().all()
        assert {c.canonical_type for c in cycles} == {"order_to_cash"}

    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.ground_cycles")
    @patch("dataraum.pipeline.phases.business_cycles_phase.get_cycle_types")
    def test_rerun_supersedes_under_fresh_run_id(
        self,
        mock_types: MagicMock,
        mock_ground: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """No skip-if-already-ran: the re-run re-flows everything; runs coexist."""
        mock_types.return_value = {"order_to_cash": {"business_value": "high"}}
        mock_ground.side_effect = lambda *a, **k: Result.ok(_analysis([_detected("order_to_cash")]))

        phase = BusinessCyclesPhase()
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
        cycles = session.execute(select(DetectedBusinessCycle)).scalars().all()
        assert {(c.canonical_type, c.run_id) for c in cycles} == {
            ("order_to_cash", "run-1"),
            ("order_to_cash", "run-2"),
        }

    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.ground_cycles")
    @patch("dataraum.pipeline.phases.business_cycles_phase.get_cycle_types")
    def test_success_redelivery_same_run_converges(
        self,
        mock_types: MagicMock,
        mock_ground: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """The at-least-once redelivery (same run_id, committed rows) converges (DAT-502).

        declare-or-reuse RESETS the committed artifact back to declared so the
        redelivered bind is legal, and the DetectedBusinessCycle upsert
        converges on uq_detected_cycle_run — the fresh detection's fields win
        on the existing row, no IntegrityError, no duplicates.
        """
        mock_types.return_value = {"order_to_cash": {"business_value": "high"}}
        mock_ground.side_effect = lambda *a, **k: Result.ok(_analysis([_detected("order_to_cash")]))

        phase = BusinessCyclesPhase()
        r1 = phase._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id], "run-1"))
        session.commit()  # attempt 1 committed; ack lost
        r2 = phase._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id], "run-1"))
        session.commit()

        assert r1.status == r2.status == PhaseStatus.COMPLETED
        session.expire_all()
        artifacts = session.execute(select(LifecycleArtifact)).scalars().all()
        assert [(a.artifact_key, a.run_id, a.state) for a in artifacts] == [
            ("order_to_cash", "run-1", "executed")
        ]
        cycles = session.execute(select(DetectedBusinessCycle)).scalars().all()
        assert [(c.canonical_type, c.run_id) for c in cycles] == [("order_to_cash", "run-1")]
