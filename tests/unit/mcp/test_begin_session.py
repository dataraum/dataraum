"""Tests for begin_session MCP tool and session wiring."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import InvestigationSession, InvestigationStep
from dataraum.storage import Source


def _id() -> str:
    return str(uuid4())


def _insert_source(session: Session, name: str = "test_source") -> str:
    source_id = _id()
    session.add(Source(source_id=source_id, name=name, source_type="csv"))
    session.flush()
    return source_id


class TestBeginSession:
    def test_creates_session(self, session: Session) -> None:
        """begin_session creates an InvestigationSession and returns orientation."""
        source_id = _insert_source(session)

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _begin_session

            result = _begin_session(session, intent="test investigation")

        assert "error" not in result
        assert "_session_id" in result
        assert result["sources"] == ["test_source"]
        assert result["has_pipeline_data"] is False
        assert "hint" in result

        # Verify session was actually created in DB
        inv = session.get(InvestigationSession, result["_session_id"])
        assert inv is not None
        assert inv.intent == "test investigation"
        assert inv.status == "active"

    def test_default_contract_is_exploratory(self, session: Session) -> None:
        """Without explicit contract, defaults to exploratory_analysis."""
        source_id = _insert_source(session)

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _begin_session

            result = _begin_session(session, intent="test")

        assert result["contract"]["name"] == "exploratory_analysis"
        assert result["contract"]["display_name"] == "Exploratory Analysis"
        assert "description" in result["contract"]

        inv = session.get(InvestigationSession, result["_session_id"])
        assert inv is not None
        assert inv.contract == "exploratory_analysis"

    def test_explicit_contract(self, session: Session) -> None:
        """Explicit contract is validated and stored."""
        source_id = _insert_source(session)

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _begin_session

            result = _begin_session(
                session, intent="compliance check", contract="executive_dashboard"
            )

        assert "error" not in result
        assert result["contract"]["name"] == "executive_dashboard"
        assert result["contract"]["display_name"] == "Executive Dashboard"

        inv = session.get(InvestigationSession, result["_session_id"])
        assert inv is not None
        assert inv.contract == "executive_dashboard"

    def test_unknown_contract_returns_error(self, session: Session) -> None:
        """Invalid contract name returns error with available contracts."""
        _insert_source(session)

        from dataraum.mcp.server import _begin_session

        result = _begin_session(session, intent="test", contract="nonexistent_contract")

        assert "error" in result
        assert "nonexistent_contract" in result["error"]
        assert "exploratory_analysis" in result["error"]

    def test_has_pipeline_data_when_entropy_exists(self, session: Session) -> None:
        """has_pipeline_data is True when entropy records exist."""
        from dataraum.entropy.db_models import EntropyObjectRecord

        source_id = _insert_source(session)
        session.add(
            EntropyObjectRecord(
                source_id=source_id,
                layer="semantic",
                dimension="business_meaning",
                sub_dimension="naming_clarity",
                target="column:orders.amount",
                score=0.5,
                detector_id="naming_clarity",
            )
        )
        session.flush()

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _begin_session

            result = _begin_session(session, intent="check quality")

        assert result["has_pipeline_data"] is True
        # Hint should guide toward exploration, not pipeline trigger
        assert "pipeline" not in result["hint"].lower()

    def test_no_source_returns_error(self, session: Session) -> None:
        """When no sources are registered, returns error."""
        from dataraum.mcp.server import _begin_session

        result = _begin_session(session, intent="test")

        assert "error" in result
        assert "add_source" in result["error"]

    def test_multiple_sources_listed(self, session: Session) -> None:
        """All registered sources are returned."""
        source_id = _insert_source(session, name="source_a")
        _insert_source(session, name="source_b")

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _begin_session

            result = _begin_session(session, intent="test")

        assert len(result["sources"]) == 2
        assert "source_a" in result["sources"]
        assert "source_b" in result["sources"]

    def test_session_id_not_surfaced(self, session: Session) -> None:
        """_session_id is internal, no 'session_id' key in response."""
        source_id = _insert_source(session)

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _begin_session

            result = _begin_session(session, intent="test")

        assert "session_id" not in result
        assert "_session_id" in result


class TestPrerequisiteChecks:
    def test_missing_api_key_returns_error(
        self, session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """begin_session fails with actionable error when API key is missing."""
        _insert_source(session)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

        from dataraum.mcp.server import _begin_session

        result = _begin_session(session, intent="test")

        assert "error" in result
        assert "ANTHROPIC_API_KEY" in result["error"]
        assert "export" in result["error"]
        assert ".env" in result["error"]

    def test_api_key_present_passes(
        self, session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """begin_session succeeds when API key is set."""
        source_id = _insert_source(session)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            from dataraum.mcp.server import _begin_session

            result = _begin_session(session, intent="test")

        assert "error" not in result

    def test_check_prerequisites_returns_none_when_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_check_prerequisites returns None when all checks pass."""
        from dataraum.mcp.server import _check_prerequisites

        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
        assert _check_prerequisites() is None

    def test_check_prerequisites_returns_error_when_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_check_prerequisites returns error string when API key is missing."""
        from dataraum.mcp.server import _check_prerequisites

        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        result = _check_prerequisites()
        assert result is not None
        assert "ANTHROPIC_API_KEY" in result


class TestFlowEnforcement:
    """Tests for call_tool flow enforcement via the MCP server handler."""

    @staticmethod
    async def _call(server, name: str, arguments: dict | None = None):
        """Call a tool through the MCP server handler and parse the JSON result."""
        import json

        from mcp.types import CallToolRequest, CallToolRequestParams

        handler = server.request_handlers[CallToolRequest]
        req = CallToolRequest(
            method="tools/call",
            params=CallToolRequestParams(name=name, arguments=arguments or {}),
        )
        raw = await handler(req)
        return json.loads(raw.root.content[0].text)

    @pytest.mark.asyncio
    async def test_look_blocked_without_session(self, tmp_path) -> None:
        """look returns error when no session is active."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await self._call(server, "look")
        assert "error" in result
        assert "begin_session" in result["error"]

    @pytest.mark.asyncio
    async def test_measure_blocked_without_session(self, tmp_path) -> None:
        """measure returns error when no session is active."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await self._call(server, "measure")
        assert "error" in result
        assert "begin_session" in result["error"]

    @pytest.mark.asyncio
    async def test_run_sql_blocked_without_session(self, tmp_path) -> None:
        """run_sql returns error when no session is active."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await self._call(server, "run_sql", {"sql": "SELECT 1"})
        assert "error" in result
        assert "begin_session" in result["error"]

    @pytest.mark.asyncio
    async def test_begin_session_blocked_without_sources(self, tmp_path) -> None:
        """begin_session returns error when no sources are registered."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await self._call(server, "begin_session", {"intent": "test"})
        assert "error" in result

    @pytest.mark.asyncio
    async def test_add_source_allowed_without_session(self, tmp_path) -> None:
        """add_source is NOT blocked before session starts."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        # add_source will fail on validation (no path/backend), not on flow enforcement
        result = await self._call(server, "add_source", {"name": "test"})
        assert "error" in result
        assert "begin_session" not in result["error"]


class TestRecordToolStep:
    def test_records_step_on_success(self, session: Session) -> None:
        """_record_tool_step writes an InvestigationStep to the DB."""
        from dataraum.investigation.recorder import begin_session as create_session
        from dataraum.mcp.server import _record_tool_step

        source_id = _insert_source(session)
        inv = create_session(session, source_id, intent="test")
        session.flush()

        # Create a mock manager whose session_scope yields our test session
        manager = MagicMock()
        manager.session_scope.return_value.__enter__ = lambda _: session
        manager.session_scope.return_value.__exit__ = lambda *_: None

        _record_tool_step(
            manager,
            session_id=inv.session_id,
            tool_name="look",
            arguments={"target": "orders"},
            result={"tables": []},
            started_at=datetime.now(UTC),
        )

        steps = session.query(InvestigationStep).filter_by(session_id=inv.session_id).all()
        assert len(steps) == 1
        assert steps[0].tool_name == "look"
        assert steps[0].status == "success"
        assert steps[0].target == "orders"

    def test_records_error_status(self, session: Session) -> None:
        """Error results are recorded with status='error'."""
        from dataraum.investigation.recorder import begin_session as create_session
        from dataraum.mcp.server import _record_tool_step

        source_id = _insert_source(session)
        inv = create_session(session, source_id, intent="test")
        session.flush()

        manager = MagicMock()
        manager.session_scope.return_value.__enter__ = lambda _: session
        manager.session_scope.return_value.__exit__ = lambda *_: None

        _record_tool_step(
            manager,
            session_id=inv.session_id,
            tool_name="look",
            arguments={"target": "nonexistent"},
            result={"error": "Table not found"},
            started_at=datetime.now(UTC),
        )

        steps = session.query(InvestigationStep).filter_by(session_id=inv.session_id).all()
        assert len(steps) == 1
        assert steps[0].status == "error"
        assert steps[0].error == "Table not found"

    def test_recording_failure_does_not_raise(self) -> None:
        """_record_tool_step never raises, even with a broken manager."""
        from dataraum.mcp.server import _record_tool_step

        # Manager that raises on session_scope
        manager = MagicMock()
        manager.session_scope.side_effect = RuntimeError("broken")

        _record_tool_step(
            manager,
            session_id="bad-session-id",
            tool_name="look",
            arguments={},
            result={},
            started_at=datetime.now(UTC),
        )
