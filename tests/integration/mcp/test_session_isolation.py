"""Integration tests for DAT-192 session-isolation invariants.

These tests assert the safety properties of the two-manager design at the
DB level — opening workspace.db and session DBs directly to verify that
data lands where it should and doesn't bleed across boundaries.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest


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


def _make_csv(tmp_path: Path, name: str, rows: str = "a,b\n1,2\n") -> Path:
    csv = tmp_path / name
    csv.write_text(rows)
    return csv


def _query_one(db_path: Path, sql: str) -> tuple | None:
    """Query a SQLite DB outside SQLAlchemy. Returns first row or None."""
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(sql).fetchone()


def _query_all(db_path: Path, sql: str) -> list[tuple]:
    with sqlite3.connect(str(db_path)) as conn:
        return list(conn.execute(sql).fetchall())


def _count(db_path: Path, table: str) -> int:
    """Count rows in a table; returns 0 if table doesn't exist."""
    with sqlite3.connect(str(db_path)) as conn:
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except sqlite3.OperationalError:
            return 0


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")


class TestAddSourceWritesOnlyToWorkspace:
    """AC2: add_source writes only to workspace DB. No table/column/entropy data leaks in."""

    @pytest.mark.asyncio
    async def test_add_source_populates_workspace_sources_only(
        self, tmp_path: Path, api_key: None
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        workspace_db = tmp_path / "workspace.db"
        assert workspace_db.exists()

        # Sources is populated
        assert _count(workspace_db, "sources") == 1
        row = _query_one(workspace_db, "SELECT name, source_type FROM sources")
        assert row == ("src1", "csv")

        # Tables, columns, entropy data must NOT be in the workspace
        # (these are session-DB concerns; pipeline hasn't run yet anyway,
        # but the workspace should never see them even after sessions run).
        assert _count(workspace_db, "tables") == 0
        assert _count(workspace_db, "columns") == 0
        assert _count(workspace_db, "entropy_objects") == 0

        # No session dirs created yet — begin_session not called
        assert not (tmp_path / "sessions").exists()


class TestSameSourcesReuseSessionDir:
    """AC3: begin_session with the same source set reuses sessions/{fingerprint}/."""

    @pytest.mark.asyncio
    async def test_same_sources_same_fingerprint_dir(self, tmp_path: Path, api_key: None) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        await _call(server, "begin_session", {"intent": "first"})
        sessions_dir = tmp_path / "sessions"
        first_dirs = sorted(p.name for p in sessions_dir.iterdir())
        assert len(first_dirs) == 1

        # End and begin again — same sources → same fingerprint → same dir reused (after archive)
        await _call(server, "end_session", {"outcome": "abandoned"})
        # After end, the session dir is archived; begin again recreates the same fp dir
        await _call(server, "begin_session", {"intent": "second"})

        second_dirs = sorted(p.name for p in sessions_dir.iterdir())
        assert second_dirs == first_dirs, "Same sources must produce the same fingerprint directory"


class TestDifferentSourcesIsolation:
    """AC4: different source sets land in different session directories."""

    @pytest.mark.asyncio
    async def test_different_sources_different_dirs_no_overwrite(
        self, tmp_path: Path, api_key: None
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)

        # Cycle 1: source A
        csv_a = _make_csv(tmp_path, "a.csv", "x,y\n1,2\n")
        await _call(server, "add_source", {"name": "src_a", "path": str(csv_a)})
        await _call(server, "begin_session", {"intent": "session A"})
        await _call(server, "end_session", {"outcome": "abandoned"})

        # Cycle 2: source B (different name + path → different fingerprint)
        csv_b = _make_csv(tmp_path, "b.csv", "x,y\n3,4\n")
        await _call(server, "add_source", {"name": "src_b", "path": str(csv_b)})
        await _call(server, "begin_session", {"intent": "session B"})
        await _call(server, "end_session", {"outcome": "abandoned"})

        # Both archived sessions present, in distinct directories
        archive = tmp_path / "archive"
        archived_dirs = sorted(p for p in archive.iterdir())
        assert len(archived_dirs) == 2

        # Each archive contains its own metadata.db with the right session intent
        intents: list[str] = []
        for adir in archived_dirs:
            metadata = adir / "metadata.db"
            assert metadata.exists()
            row = _query_one(
                metadata,
                "SELECT intent FROM investigation_sessions ORDER BY started_at DESC LIMIT 1",
            )
            assert row is not None
            intents.append(row[0])
        assert set(intents) == {"session A", "session B"}, (
            "Each archived session must contain only its own InvestigationSession row"
        )


class TestBeginSessionWritesToBothDBs:
    """begin_session sets ActiveSession pointer in workspace AND InvestigationSession in session DB."""

    @pytest.mark.asyncio
    async def test_pointer_and_session_row_present(self, tmp_path: Path, api_key: None) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        await _call(server, "begin_session", {"intent": "verify_writes"})

        # Workspace has ActiveSession pointer
        workspace_db = tmp_path / "workspace.db"
        pointer = _query_one(workspace_db, "SELECT session_id, fingerprint FROM active_session")
        assert pointer is not None
        session_id, fingerprint = pointer
        assert session_id and fingerprint

        # Session DB has the InvestigationSession with the same id, status=active
        sessions_dir = tmp_path / "sessions"
        session_dirs = list(sessions_dir.iterdir())
        assert len(session_dirs) == 1
        assert session_dirs[0].name == fingerprint  # dir name matches pointer fingerprint

        session_metadata = session_dirs[0] / "metadata.db"
        row = _query_one(
            session_metadata,
            "SELECT session_id, intent, status FROM investigation_sessions",
        )
        assert row == (session_id, "verify_writes", "active")


class TestGhostSessionCleanup:
    """Retried begin_session does not leak orphan 'active' InvestigationSession rows.

    Simulates the failure mode where a prior begin_session wrote to the
    session DB but failed to set the workspace ActiveSession pointer (e.g.,
    crashed between the two writes). On retry, the orphan must be marked
    as abandoned.
    """

    @pytest.mark.asyncio
    async def test_retry_marks_orphan_as_abandoned(self, tmp_path: Path, api_key: None) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        # First begin_session creates a session DB with an active InvestigationSession
        await _call(server, "begin_session", {"intent": "first attempt"})

        # Simulate a crashed begin_session: clear the workspace ActiveSession
        # pointer but leave the session DB and its 'active' InvestigationSession
        # in place. From the server's perspective, no session is active —
        # but the orphan row sits in sessions/{fp}/metadata.db.
        workspace_db = tmp_path / "workspace.db"
        with sqlite3.connect(str(workspace_db)) as conn:
            conn.execute("DELETE FROM active_session")
            conn.commit()

        # Retry begin_session. The orphan from "first attempt" must be marked
        # as abandoned, and a fresh active session created.
        await _call(server, "begin_session", {"intent": "fresh attempt"})

        sessions_dir = tmp_path / "sessions"
        session_dirs = list(sessions_dir.iterdir())
        assert len(session_dirs) == 1
        session_metadata = session_dirs[0] / "metadata.db"
        rows = _query_all(
            session_metadata,
            "SELECT intent, status FROM investigation_sessions ORDER BY started_at",
        )
        intents = [r[0] for r in rows]
        statuses = [r[1] for r in rows]
        assert intents == ["first attempt", "fresh attempt"]
        assert statuses == ["abandoned", "active"]
