"""Tests for the metrics phase — the third lifecycle family (DAT-456).

Mirrors the cycles/validation lifecycle tests: the LLM machinery (the graph
agent's compose+execute call) is mocked at its boundary; everything else — the
declared-set load, the parse gate, lifecycle artifacts, supersession — runs
against the real session fixture. There is NO field-mapping pre-gate: every
parseable metric is handed to the agent, which is mocked here to model the two
real outcomes (executes cleanly → executed / cannot materialize runnable SQL →
grounded with the reason). Born-loud lives at the agent, not in a heuristic skip.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.models.base import Result
from dataraum.investigation.db_models import InvestigationSession
from dataraum.lifecycle import ArtifactState, LifecycleArtifact
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.metrics_phase import MetricsPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb

_SESSION_ID = "sess-metrics-phase"
_WORKSPACE_ID = "ws-metrics"


@pytest.fixture()
def _mock_llm():
    """Patch LLM infrastructure so the phase can initialize without config."""
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}

    with (
        patch(
            "dataraum.pipeline.phases.metrics_phase.load_llm_config",
            return_value=mock_config,
        ),
        patch(
            "dataraum.pipeline.phases.metrics_phase.create_provider",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.metrics_phase.PromptRenderer",
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
            column_name="amount",
            column_position=0,
            raw_type="VARCHAR",
            resolved_type="DOUBLE",
        )
    )
    session.commit()
    return table


def _make_ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str = "run-om-1",
    config: dict[str, Any] | None = None,
) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=None,  # source-free: operating_model scope is table_ids
        table_ids=table_ids,
        session_id=_SESSION_ID,
        run_id=run_id,
        config=config
        if config is not None
        else {"vertical": "finance", "base_runs": {}, "workspace_id": _WORKSPACE_ID},
    )


def _metric_def(graph_id: str, field: str = "accounts_receivable") -> dict[str, Any]:
    """A minimal, parseable transformation-graph definition with one extract field."""
    return {
        "graph_id": graph_id,
        "metadata": {"name": graph_id.upper(), "category": "test"},
        "output": {"type": "scalar"},
        "dependencies": {
            "extract_step": {"type": "extract", "source": {"standard_field": field}},
        },
    }


def _artifacts(session: Session, run_id: str) -> dict[str, LifecycleArtifact]:
    rows = (
        session.execute(select(LifecycleArtifact).where(LifecycleArtifact.run_id == run_id))
        .scalars()
        .all()
    )
    return {a.artifact_key: a for a in rows}


class TestMetricsPhaseOutcomes:
    def test_fails_without_table_ids(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, table_ids=[]))
        assert result.status == PhaseStatus.FAILED
        assert "No tables in session scope" in (result.error or "")

    def test_no_vertical_is_loud_explicit_outcome(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, workspace_table: Table
    ) -> None:
        ctx = _make_ctx(session, duckdb_conn, [workspace_table.table_id], config={})
        result = MetricsPhase()._run(ctx)
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_vertical"
        assert _artifacts(session, "run-om-1") == {}

    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_no_declared_metrics_is_loud_explicit_outcome(
        self,
        mock_defs: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        mock_defs.return_value = {}
        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        assert result.status == PhaseStatus.COMPLETED
        assert result.outputs["outcome"] == "no_declared_metrics"

    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_missing_base_runs_pin_fails_loud(
        self,
        mock_defs: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        """No per-phase head resolution (ADR-0008): an unthreaded pin is a wiring bug."""
        mock_defs.return_value = {"dso": _metric_def("dso")}
        ctx = _make_ctx(
            session,
            duckdb_conn,
            [workspace_table.table_id],
            config={"vertical": "finance", "workspace_id": _WORKSPACE_ID},  # no base_runs
        )
        result = MetricsPhase()._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "base_runs missing" in (result.error or "")

    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_missing_workspace_id_fails_loud(
        self,
        mock_defs: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
    ) -> None:
        """The snippet base needs a workspace-stable schema_mapping_id, threaded by the activity."""
        mock_defs.return_value = {"dso": _metric_def("dso")}
        ctx = _make_ctx(
            session,
            duckdb_conn,
            [workspace_table.table_id],
            config={"vertical": "finance", "base_runs": {}},  # no workspace_id
        )
        result = MetricsPhase()._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "workspace_id missing" in (result.error or "")


def _fake_execute(fail_ids: set[str]):
    """A GraphAgent.execute stand-in: succeeds unless the graph_id is in fail_ids.

    Patched onto the class, so it is called unbound — no ``self`` argument.
    """

    def _execute(session, graph, context, *args, inspiration_sql=None, session_id="", **kw):  # noqa: ANN001
        if graph.graph_id in fail_ids:
            return Result.fail("SQL execution failed against the workspace")
        return Result.ok(MagicMock())

    return _execute


class TestMetricLifecycleFlow:
    @patch("dataraum.graphs.agent.ExecutionContext.with_rich_context")
    @patch("dataraum.graphs.agent.GraphAgent.execute")
    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_declare_compose_execute_flow(
        self,
        mock_defs: MagicMock,
        mock_execute: MagicMock,
        mock_ctx: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """Every parseable metric is composed by the agent: one executes cleanly,
        one cannot materialize runnable SQL and stays grounded with the reason.
        No pre-gate — the agent's outcome decides, not a field-mapping check."""
        mock_defs.return_value = {
            "m_exec": _metric_def("m_exec", field="accounts_receivable"),
            "m_unexec": _metric_def("m_unexec", field="revenue"),
        }
        mock_execute.side_effect = _fake_execute(fail_ids={"m_unexec"})
        mock_ctx.return_value = MagicMock()

        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        artifacts = _artifacts(session, "run-om-1")

        # Executed: composed + ran cleanly.
        assert artifacts["m_exec"].state == ArtifactState.EXECUTED.value
        assert artifacts["m_exec"].grounded_against is not None
        assert artifacts["m_exec"].teaches["vertical"] == "finance"
        # Composed but unexecutable: the agent could not materialize runnable SQL,
        # so it stays grounded with the reason — born-loud at the agent.
        assert artifacts["m_unexec"].state == ArtifactState.GROUNDED.value
        assert "execution failed" in (artifacts["m_unexec"].state_reason or "")

        assert result.outputs["executed"] == 1
        assert result.outputs["stuck_grounded"] == 1
        # No metric stays declared via a heuristic gate — declared is parse-only.
        assert result.outputs["stuck_declared"] == 0

    @patch("dataraum.graphs.agent.ExecutionContext.with_rich_context")
    @patch("dataraum.graphs.agent.GraphAgent.execute")
    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_malformed_definition_stays_declared(
        self,
        mock_defs: MagicMock,
        mock_execute: MagicMock,
        mock_ctx: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """A definition that won't parse stays declared with the parse error — the
        one legitimate pre-gate — never dropped."""
        mock_defs.return_value = {
            "ok_metric": _metric_def("ok_metric", field="accounts_receivable"),
            "broken": {"graph_id": "broken", "output": {"type": "scalar"}},  # missing metadata.name
        }
        mock_execute.side_effect = _fake_execute(fail_ids=set())
        mock_ctx.return_value = MagicMock()

        result = MetricsPhase()._run(_make_ctx(session, duckdb_conn, [workspace_table.table_id]))
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        artifacts = _artifacts(session, "run-om-1")
        assert artifacts["ok_metric"].state == ArtifactState.EXECUTED.value
        assert artifacts["broken"].state == ArtifactState.DECLARED.value
        assert "malformed" in (artifacts["broken"].state_reason or "")

    @patch("dataraum.graphs.agent.ExecutionContext.with_rich_context")
    @patch("dataraum.graphs.agent.GraphAgent.execute")
    @patch("dataraum.graphs.config.get_metric_definitions")
    def test_rerun_supersedes_under_fresh_run_id(
        self,
        mock_defs: MagicMock,
        mock_execute: MagicMock,
        mock_ctx: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        workspace_table: Table,
        _mock_llm: None,
    ) -> None:
        """No skip-if-already-ran: the re-run re-flows everything; runs coexist."""
        mock_defs.return_value = {"dso": _metric_def("dso", field="accounts_receivable")}
        mock_execute.side_effect = _fake_execute(fail_ids=set())
        mock_ctx.return_value = MagicMock()

        phase = MetricsPhase()
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
