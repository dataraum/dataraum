"""Tests for begin_session MCP tool, prerequisite checks, and flow enforcement."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import InvestigationStep
from dataraum.storage import Source


def _id() -> str:
    return str(uuid4())


def _insert_source(session: Session, name: str = "test_source") -> str:
    source_id = _id()
    session.add(Source(source_id=source_id, name=name, source_type="csv"))
    session.flush()
    return source_id


async def _call(server, name: str, arguments: dict | None = None):
    """Call a tool through the MCP server handler and parse the JSON result."""
    from mcp.types import CallToolRequest, CallToolRequestParams

    handler = server.request_handlers[CallToolRequest]
    req = CallToolRequest(
        method="tools/call",
        params=CallToolRequestParams(name=name, arguments=arguments or {}),
    )
    raw = await handler(req)
    return json.loads(raw.root.content[0].text)


def _make_csv(tmp_path: Path, name: str = "data.csv") -> Path:
    csv = tmp_path / name
    csv.write_text("a,b\n1,2\n")
    return csv


@pytest.fixture
def server_with_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Server with API key set; tests that go through the prereq check happy-path."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
    from dataraum.mcp.server import create_server

    return create_server(output_dir=tmp_path)


class TestBeginSessionViaServer:
    """begin_session is exercised through the full MCP server flow.

    The two-manager design (workspace + session DBs) means begin_session
    composes calls across managers; testing through call_tool covers the
    same behaviors that older direct-function unit tests covered.
    """

    @pytest.mark.asyncio
    async def test_creates_session(self, server_with_key, tmp_path: Path) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "test_source", "path": str(csv)})

        result = await _call(server_with_key, "begin_session", {"intent": "test investigation"})

        assert "error" not in result
        assert result["sources"] == ["test_source"]
        assert result["has_pipeline_data"] is False
        assert "hint" in result

    @pytest.mark.asyncio
    async def test_default_contract_is_exploratory(self, server_with_key, tmp_path: Path) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})

        result = await _call(server_with_key, "begin_session", {"intent": "test"})

        assert result["contract"]["name"] == "exploratory_analysis"

    @pytest.mark.asyncio
    async def test_explicit_contract(self, server_with_key, tmp_path: Path) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})

        result = await _call(
            server_with_key,
            "begin_session",
            {"intent": "test", "contract": "aggregation_safe"},
        )

        assert result["contract"]["name"] == "aggregation_safe"

    @pytest.mark.asyncio
    async def test_unknown_contract_returns_error(self, server_with_key, tmp_path: Path) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})

        result = await _call(
            server_with_key,
            "begin_session",
            {"intent": "test", "contract": "nonexistent"},
        )

        assert "error" in result
        assert "nonexistent" in result["error"]

    @pytest.mark.asyncio
    async def test_no_source_returns_error(self, server_with_key) -> None:
        """begin_session without registered sources rejects with a clear error."""
        result = await _call(server_with_key, "begin_session", {"intent": "test"})
        assert "error" in result
        assert "add_source" in result["error"]

    @pytest.mark.asyncio
    async def test_multiple_sources_listed(self, server_with_key, tmp_path: Path) -> None:
        for name in ("src1", "src2", "src3"):
            csv = _make_csv(tmp_path, f"{name}.csv")
            await _call(server_with_key, "add_source", {"name": name, "path": str(csv)})

        result = await _call(server_with_key, "begin_session", {"intent": "test"})

        assert set(result["sources"]) == {"src1", "src2", "src3"}

    @pytest.mark.asyncio
    async def test_internal_keys_not_surfaced(self, server_with_key, tmp_path: Path) -> None:
        """The _session_id and _fingerprint internal keys are stripped before response."""
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})

        result = await _call(server_with_key, "begin_session", {"intent": "test"})

        assert "_session_id" not in result
        assert "_fingerprint" not in result


class TestPrerequisiteChecks:
    @pytest.mark.asyncio
    async def test_missing_api_key_returns_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """begin_session fails with actionable error when API key is missing."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path)
        await _call(server, "add_source", {"name": "src", "path": str(csv)})

        result = await _call(server, "begin_session", {"intent": "test"})

        assert "error" in result
        assert "ANTHROPIC_API_KEY" in result["error"]
        assert "export" in result["error"]
        assert ".env" in result["error"]

    @pytest.mark.asyncio
    async def test_api_key_present_passes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """begin_session succeeds when API key is set."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path)
        await _call(server, "add_source", {"name": "src", "path": str(csv)})

        result = await _call(server, "begin_session", {"intent": "test"})

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

    @pytest.mark.asyncio
    async def test_look_blocked_without_session(self, tmp_path) -> None:
        """look returns error when no session is active."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await _call(server, "look")
        assert "error" in result
        assert "begin_session" in result["error"]

    @pytest.mark.asyncio
    async def test_measure_blocked_without_session(self, tmp_path) -> None:
        """measure returns error when no session is active."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await _call(server, "measure")
        assert "error" in result
        assert "begin_session" in result["error"]

    @pytest.mark.asyncio
    async def test_run_sql_blocked_without_session(self, tmp_path) -> None:
        """run_sql returns error when no session is active."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await _call(server, "run_sql", {"sql": "SELECT 1"})
        assert "error" in result
        assert "begin_session" in result["error"]

    @pytest.mark.asyncio
    async def test_begin_session_blocked_without_sources(self, tmp_path) -> None:
        """begin_session returns error when no sources are registered."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await _call(server, "begin_session", {"intent": "test"})
        assert "error" in result

    @pytest.mark.asyncio
    async def test_add_source_allowed_without_session(self, tmp_path) -> None:
        """add_source is NOT blocked before session starts."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await _call(server, "add_source", {"name": "test"})
        # add_source fails validation (no path/backend), not flow enforcement
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
