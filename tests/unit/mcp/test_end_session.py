"""Tests for end_session, session resume, and workspace lifecycle."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from dataraum.investigation.db_models import InvestigationSession
from dataraum.storage import Source


def _id() -> str:
    return str(uuid4())


def _insert_source(session: Session, name: str = "test_source") -> str:
    source_id = _id()
    session.add(Source(source_id=source_id, name=name, source_type="csv"))
    session.flush()
    return source_id


def _create_active_session(
    session: Session, source_id: str, intent: str = "test", contract: str = "exploratory_analysis"
) -> InvestigationSession:
    from dataraum.investigation.recorder import begin_session

    return begin_session(session, source_id, intent, contract=contract)


def _mock_manager(session: Session) -> MagicMock:
    """Create a mock ConnectionManager that yields the given session."""
    manager = MagicMock()
    manager.session_scope.return_value.__enter__ = lambda _: session
    manager.session_scope.return_value.__exit__ = lambda *_: None
    return manager


class TestEndSession:
    def test_ends_active_session(self, session: Session) -> None:
        """end_session closes an active session with outcome."""
        from dataraum.mcp.server import _end_session

        source_id = _insert_source(session)
        inv = _create_active_session(session, source_id)

        result = _end_session(
            _mock_manager(session), inv.session_id, "delivered", summary="Analysis complete"
        )

        assert "error" not in result
        assert result["status"] == "ended"
        assert result["outcome"] == "delivered"
        assert result["summary"] == "Analysis complete"
        assert result["duration_seconds"] is not None

        # Verify DB state
        refreshed = session.get(InvestigationSession, inv.session_id)
        assert refreshed is not None
        assert refreshed.status == "delivered"

    def test_invalid_outcome_returns_error(self, session: Session) -> None:
        """Invalid outcome is rejected before touching DB."""
        from dataraum.mcp.server import _end_session

        result = _end_session(MagicMock(), "any-id", "invalid_outcome")

        assert "error" in result
        assert "invalid_outcome" in result["error"].lower()

    def test_all_valid_outcomes_accepted(self, session: Session) -> None:
        """All four terminal outcomes work."""
        from dataraum.mcp.server import _end_session

        manager = _mock_manager(session)
        for outcome in ("delivered", "refused", "escalated", "abandoned"):
            source_id = _insert_source(session, name=f"src_{outcome}")
            inv = _create_active_session(session, source_id)
            result = _end_session(manager, inv.session_id, outcome)
            assert result["outcome"] == outcome, f"Failed for outcome={outcome}"


class TestResumeSession:
    def test_resumes_with_correct_shape(self, session: Session) -> None:
        """_resume_session reads InvestigationSession from session DB and returns resume payload."""
        from dataraum.mcp.server import _resume_session

        source_id = _insert_source(session, name="zone1")
        inv = _create_active_session(session, source_id, contract="exploratory_analysis")
        session.flush()

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            result = _resume_session(_mock_manager(session), inv.session_id)

        assert result["resumed"] is True
        assert result["sources"] == ["zone1"]
        assert result["contract"]["name"] == "exploratory_analysis"
        assert result["has_pipeline_data"] is False
        assert "end_session" in result["hint"]
        assert result["step_count"] == 0

    def test_resume_includes_step_count(self, session: Session) -> None:
        """Resumed session shows how many steps have been recorded."""
        from dataraum.investigation.recorder import record_step
        from dataraum.mcp.server import _resume_session

        source_id = _insert_source(session)
        inv = _create_active_session(session, source_id)
        record_step(session, inv.session_id, "look", {"target": "orders"})
        record_step(session, inv.session_id, "measure", {})
        session.flush()

        with patch(
            "dataraum.mcp.server._get_pipeline_source",
            return_value=session.get(Source, source_id),
        ):
            result = _resume_session(_mock_manager(session), inv.session_id)

        assert result["step_count"] == 2


class TestEndSessionFullFlow:
    """Integration tests for end_session through the MCP server handler."""

    @staticmethod
    async def _call(server, name: str, arguments: dict | None = None):
        from mcp.types import CallToolRequest, CallToolRequestParams

        handler = server.request_handlers[CallToolRequest]
        req = CallToolRequest(
            method="tools/call",
            params=CallToolRequestParams(name=name, arguments=arguments or {}),
        )
        raw = await handler(req)
        return json.loads(raw.root.content[0].text)

    @pytest.mark.asyncio
    async def test_end_session_archives_and_allows_new(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Full flow: begin → end → session dir archived → workspace.db preserved."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")

        from dataraum.mcp.server import create_server

        # tmp_path is the root in the new layout
        server = create_server(output_dir=tmp_path)

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")

        # Setup: add source (writes to workspace.db) and begin session
        # (creates sessions/{fp}/metadata.db,data.duckdb + sets ActiveSession pointer)
        await self._call(server, "add_source", {"name": "src", "path": str(csv)})
        r1 = await self._call(server, "begin_session", {"intent": "first"})
        assert "error" not in r1

        # Workspace registry exists; sessions/{fp}/ exists; archive does not
        assert (tmp_path / "workspace.db").exists()
        sessions_dir = tmp_path / "sessions"
        assert sessions_dir.exists()
        session_dirs_before = list(sessions_dir.iterdir())
        assert len(session_dirs_before) == 1  # one fingerprint
        assert (session_dirs_before[0] / "metadata.db").exists()

        # End the session
        r2 = await self._call(server, "end_session", {"outcome": "delivered", "summary": "done"})
        assert r2["status"] == "ended"
        assert r2["outcome"] == "delivered"

        # Session dir gone, archive populated, workspace.db preserved
        assert not session_dirs_before[0].exists()
        archive_base = tmp_path / "archive"
        assert archive_base.exists()
        archived = list(archive_base.iterdir())
        assert len(archived) == 1
        assert (archived[0] / "metadata.db").exists()
        assert (tmp_path / "workspace.db").exists()  # registry preserved

    @pytest.mark.asyncio
    async def test_end_session_without_outcome_rejected_by_schema(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """end_session with empty arguments is rejected by MCP schema validation."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")

        from mcp.types import CallToolRequest, CallToolRequestParams

        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")

        await self._call(server, "add_source", {"name": "src", "path": str(csv)})
        await self._call(server, "begin_session", {"intent": "test"})

        # MCP framework validates schema before call_tool — returns isError=True
        handler = server.request_handlers[CallToolRequest]
        req = CallToolRequest(
            method="tools/call",
            params=CallToolRequestParams(name="end_session", arguments={}),
        )
        raw = await handler(req)
        assert raw.root.isError is True
        assert "outcome" in raw.root.content[0].text.lower()


class TestResolveRootDir:
    def test_dataraum_home_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """DATARAUM_HOME env var sets the root directory."""
        from dataraum.mcp.server import _resolve_root_dir

        monkeypatch.setenv("DATARAUM_HOME", "/custom/root")
        assert _resolve_root_dir() == Path("/custom/root")

    def test_defaults_to_home_dir(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without env vars, defaults to ~/.dataraum/."""
        from dataraum.mcp.server import _resolve_root_dir

        monkeypatch.delenv("DATARAUM_HOME", raising=False)
        assert _resolve_root_dir() == Path.home() / ".dataraum"

    def test_expands_tilde(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tilde in env var is expanded."""
        from dataraum.mcp.server import _resolve_root_dir

        monkeypatch.setenv("DATARAUM_HOME", "~/my-data")
        result = _resolve_root_dir()
        assert "~" not in str(result)
        assert result == Path.home() / "my-data"


class TestFlowEnforcementEndSession:
    """Tests for end_session and add_source flow enforcement via the MCP server handler."""

    @staticmethod
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

    @pytest.mark.asyncio
    async def test_end_session_blocked_without_active(self, tmp_path: Path) -> None:
        """end_session returns error when no session is active."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        result = await self._call(server, "end_session", {"outcome": "delivered"})
        assert "error" in result
        assert "No active session" in result["error"]

    @pytest.mark.asyncio
    async def test_add_source_during_session_explains_sealing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """add_source during active session explains source sealing."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")

        result1 = await self._call(server, "add_source", {"name": "src1", "path": str(csv)})
        assert "error" not in result1

        result2 = await self._call(server, "begin_session", {"intent": "test"})
        assert "error" not in result2

        # Now add_source should be blocked with sealing explanation
        result3 = await self._call(server, "add_source", {"name": "src2", "path": str(csv)})
        assert "error" in result3
        assert "sealed" in result3["error"].lower()

    @pytest.mark.asyncio
    async def test_begin_session_resumes_active(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """begin_session with active session returns resumed=True."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")

        await self._call(server, "add_source", {"name": "src1", "path": str(csv)})
        result1 = await self._call(server, "begin_session", {"intent": "first"})
        assert "error" not in result1

        # Second begin_session should resume, not error
        result2 = await self._call(server, "begin_session", {"intent": "second"})
        assert "error" not in result2
        assert result2.get("resumed") is True
        assert "end_session" in result2["hint"]
