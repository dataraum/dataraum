"""Tests for resume_session MCP tool and the underlying restore helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text


def _workspace_query(sql: str, params: tuple | dict | None = None) -> list[tuple]:
    """Run a raw SQL query against the workspace Postgres and return rows.

    Convenience for tests that previously poked sqlite3 at workspace.db /
    per-session metadata.db. Post-DAT-321 all SQLAlchemy state lives in
    workspace Postgres (DATABASE_URL).
    """
    engine = create_engine(os.environ["DATABASE_URL"], future=True)
    try:
        with engine.connect() as conn:
            return list(conn.execute(text(sql), params or {}).fetchall())
    finally:
        engine.dispose()


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
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
    from dataraum.mcp.server import create_server

    return create_server(output_dir=tmp_path)


class TestArchivedSessionWritten:
    """end_session writes a row to archived_sessions in the workspace Postgres."""

    @pytest.mark.asyncio
    async def test_end_session_writes_archive_index(self, server_with_key, tmp_path: Path) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        r1 = await _call(
            server_with_key,
            "begin_session",
            {"source": "src", "intent": "investigate things", "contract": "aggregation_safe"},
        )
        assert "error" not in r1
        await _call(server_with_key, "end_session", {"outcome": "delivered", "summary": "done"})

        rows = _workspace_query(
            "SELECT session_id, fingerprint, intent, contract, outcome, "
            "summary, source_name FROM archived_sessions"
        )

        assert len(rows) == 1
        session_id, fingerprint, intent, contract, outcome, summary, source_name = rows[0]
        assert session_id and fingerprint
        assert intent == "investigate things"
        assert contract == "aggregation_safe"
        assert outcome == "delivered"
        assert summary == "done"
        assert source_name == "src"


class TestResponseFormatting:
    """Surface-level invariants on the listing response."""

    @pytest.mark.asyncio
    async def test_datetimes_serialized_as_iso_utc(self, server_with_key, tmp_path: Path) -> None:
        """started_at / ended_at come back as ISO 8601 UTC, not naive Python repr."""
        import re

        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "fmt"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        listing = await _call(server_with_key, "resume_session", {})
        archive = listing["archived_sessions"][0]

        iso_utc = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
        assert iso_utc.match(archive["started_at"]), archive["started_at"]
        assert iso_utc.match(archive["ended_at"]), archive["ended_at"]

    @pytest.mark.asyncio
    async def test_vertical_normalized_to_adhoc_in_listing(
        self, server_with_key, tmp_path: Path
    ) -> None:
        """Cold-start sessions (no vertical) show '_adhoc' in listing,
        matching what begin_session and resume_session responses emit."""
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "fmt"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        listing = await _call(server_with_key, "resume_session", {})
        assert listing["archived_sessions"][0]["vertical"] == "_adhoc"

    @pytest.mark.asyncio
    async def test_archive_listing_surfaces_source(self, server_with_key, tmp_path: Path) -> None:
        """Archive listing shows the single registered source name (scalar)."""
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "fmt"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        listing = await _call(server_with_key, "resume_session", {})
        assert listing["archived_sessions"][0]["source"] == "src"


class TestResumeSessionListing:
    """resume_session without args lists archived sessions."""

    @pytest.mark.asyncio
    async def test_no_args_returns_empty_list_when_no_archives(self, server_with_key) -> None:
        result = await _call(server_with_key, "resume_session", {})
        assert result["archived_sessions"] == []
        assert "hint" in result

    @pytest.mark.asyncio
    async def test_no_args_returns_archives_newest_first(
        self, server_with_key, tmp_path: Path
    ) -> None:
        csv1 = _make_csv(tmp_path, "a.csv")
        csv2 = _make_csv(tmp_path, "b.csv")
        # First cycle: src_a
        await _call(server_with_key, "add_source", {"name": "src_a", "path": str(csv1)})
        await _call(server_with_key, "begin_session", {"source": "src_a", "intent": "first"})
        await _call(server_with_key, "end_session", {"outcome": "abandoned"})
        # Second cycle: src_b (different fingerprint)
        await _call(server_with_key, "add_source", {"name": "src_b", "path": str(csv2)})
        await _call(server_with_key, "begin_session", {"source": "src_b", "intent": "second"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        result = await _call(server_with_key, "resume_session", {})
        archives = result["archived_sessions"]
        assert len(archives) == 2
        # Newest first
        assert archives[0]["intent"] == "second"
        assert archives[1]["intent"] == "first"
        # Shape
        for a in archives:
            assert {
                "session_id",
                "fingerprint",
                "intent",
                "contract",
                "outcome",
                "source",
                "started_at",
                "ended_at",
                "step_count",
            } <= set(a.keys())


class TestResumeSessionRestore:
    """resume_session(session_id) restores an archive and makes it active."""

    @pytest.mark.asyncio
    async def test_restore_round_trip(self, server_with_key, tmp_path: Path) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        r1 = await _call(
            server_with_key,
            "begin_session",
            {"source": "src", "intent": "original", "contract": "data_science"},
        )
        assert "error" not in r1
        await _call(server_with_key, "end_session", {"outcome": "delivered", "summary": "ship it"})

        # Find the archived session_id
        listing = await _call(server_with_key, "resume_session", {})
        archive_id = listing["archived_sessions"][0]["session_id"]
        original_fp = listing["archived_sessions"][0]["fingerprint"]

        # Restore
        result = await _call(server_with_key, "resume_session", {"session_id": archive_id})

        assert "error" not in result
        assert result["resumed_from"] == archive_id
        assert result["source"] == "src"
        assert result["contract"]["name"] == "data_science"
        # Internal keys stripped
        assert "_session_id" not in result
        assert "_fingerprint" not in result

        # Filesystem invariants: per-session DuckDB restored, no metadata.db file
        assert not (tmp_path / "archive" / archive_id).exists()
        assert (tmp_path / "sessions" / original_fp).exists()
        assert (tmp_path / "sessions" / original_fp / "data.duckdb").exists()
        assert not (tmp_path / "sessions" / original_fp / "metadata.db").exists()

        # Workspace Postgres state: ActiveSession set, ArchivedSession row consumed
        archived_rows = _workspace_query(
            "SELECT COUNT(*) FROM archived_sessions WHERE session_id = :sid",
            {"sid": archive_id},
        )
        assert archived_rows[0][0] == 0
        active_rows = _workspace_query("SELECT fingerprint FROM active_session")
        assert len(active_rows) == 1
        assert active_rows[0][0] == original_fp

    @pytest.mark.asyncio
    async def test_restore_preserves_contract_without_re_specification(
        self, server_with_key, tmp_path: Path
    ) -> None:
        """Contract is carried from archive — agent doesn't pass it again."""
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(
            server_with_key,
            "begin_session",
            {"source": "src", "intent": "audit", "contract": "regulatory_reporting"},
        )
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        listing = await _call(server_with_key, "resume_session", {})
        archive_id = listing["archived_sessions"][0]["session_id"]
        # Note: no contract passed — must come from archive
        result = await _call(server_with_key, "resume_session", {"session_id": archive_id})
        assert result["contract"]["name"] == "regulatory_reporting"

    @pytest.mark.asyncio
    async def test_restore_default_intent_prefixes_resumed(
        self, server_with_key, tmp_path: Path
    ) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "look at Q1"})
        await _call(server_with_key, "end_session", {"outcome": "abandoned"})

        listing = await _call(server_with_key, "resume_session", {})
        archive_id = listing["archived_sessions"][0]["session_id"]
        result = await _call(server_with_key, "resume_session", {"session_id": archive_id})
        assert "error" not in result
        # The active InvestigationSession's intent should be prefixed.
        # The per-session session_dir still exists for the restored DuckDB;
        # InvestigationSession rows live in workspace Postgres.
        session_dirs = list((tmp_path / "sessions").iterdir())
        assert len(session_dirs) == 1
        rows = _workspace_query(
            "SELECT intent, status FROM investigation_sessions ORDER BY started_at"
        )
        # First row: original (status set by end_session), second row: resumed (active)
        assert len(rows) == 2
        assert rows[0] == ("look at Q1", "abandoned")
        assert rows[1] == ("Resumed: look at Q1", "active")

    @pytest.mark.asyncio
    async def test_restore_explicit_intent_overrides_default(
        self, server_with_key, tmp_path: Path
    ) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "Q1 audit"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})
        listing = await _call(server_with_key, "resume_session", {})
        archive_id = listing["archived_sessions"][0]["session_id"]
        await _call(
            server_with_key,
            "resume_session",
            {"session_id": archive_id, "intent": "Q2 follow-up"},
        )

        session_dirs = list((tmp_path / "sessions").iterdir())
        assert len(session_dirs) == 1
        rows = _workspace_query(
            "SELECT intent FROM investigation_sessions WHERE status = 'active' LIMIT 1"
        )
        assert rows[0][0] == "Q2 follow-up"


class TestResumeSessionGuards:
    @pytest.mark.asyncio
    async def test_unknown_session_id_returns_error_with_list(
        self, server_with_key, tmp_path: Path
    ) -> None:
        # Create one archive so the list is non-empty
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "test"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        result = await _call(server_with_key, "resume_session", {"session_id": "nonexistent-id"})
        assert "error" in result
        assert "nonexistent-id" in result["error"]
        assert "available" in result
        assert len(result["available"]) == 1

    @pytest.mark.asyncio
    async def test_resume_blocked_when_session_active(
        self, server_with_key, tmp_path: Path
    ) -> None:
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "test"})

        result = await _call(server_with_key, "resume_session", {"session_id": "anything"})
        assert "error" in result
        assert "active" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_listing_allowed_while_session_active(
        self, server_with_key, tmp_path: Path
    ) -> None:
        """No-args listing is allowed even when a session is active so the
        agent can browse archives before deciding whether to end the current
        session."""
        # Create one archive first so the list is non-empty.
        csv = _make_csv(tmp_path, "a.csv")
        await _call(server_with_key, "add_source", {"name": "src_a", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src_a", "intent": "first"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        # Now start a new session and confirm listing still works.
        csv2 = _make_csv(tmp_path, "b.csv")
        await _call(server_with_key, "add_source", {"name": "src_b", "path": str(csv2)})
        await _call(server_with_key, "begin_session", {"source": "src_b", "intent": "second"})

        result = await _call(server_with_key, "resume_session", {})
        assert "error" not in result
        assert len(result["archived_sessions"]) == 1
        assert result["archived_sessions"][0]["intent"] == "first"

    @pytest.mark.asyncio
    async def test_resume_with_no_archives_lists_empty(self, server_with_key) -> None:
        result = await _call(server_with_key, "resume_session", {})
        assert result["archived_sessions"] == []

    @pytest.mark.asyncio
    async def test_restore_rolls_back_move_on_failure(
        self, server_with_key, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If a step after the dir-move fails, the archive is moved back so
        the user can retry instead of being stuck."""
        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "test"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})

        listing = await _call(server_with_key, "resume_session", {})
        archive_id = listing["archived_sessions"][0]["session_id"]
        archive_dir = tmp_path / "archive" / archive_id
        assert archive_dir.exists()

        # Force a failure inside the post-move section by patching the
        # InvestigationSession recorder to raise.
        from dataraum.investigation import recorder

        def boom(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003, ANN202
            raise RuntimeError("simulated mid-restore failure")

        monkeypatch.setattr(recorder, "begin_session", boom)

        result = await _call(server_with_key, "resume_session", {"session_id": archive_id})
        assert "error" in result
        assert "rolled back" in result["error"].lower()

        # Archive directory is back; sessions/ has no stale fingerprint dir;
        # archived_sessions index still has the row so the user can retry.
        assert archive_dir.exists()
        sessions_dir = tmp_path / "sessions"
        if sessions_dir.exists():
            assert list(sessions_dir.iterdir()) == []
        listing_after = await _call(server_with_key, "resume_session", {})
        assert len(listing_after["archived_sessions"]) == 1
        assert listing_after["archived_sessions"][0]["session_id"] == archive_id

    @pytest.mark.asyncio
    async def test_resume_cleans_up_stale_index_when_archive_dir_missing(
        self, server_with_key, tmp_path: Path
    ) -> None:
        """If the archive directory was manually deleted, the index entry
        is removed when we try to restore so the user can recover."""
        import shutil

        csv = _make_csv(tmp_path)
        await _call(server_with_key, "add_source", {"name": "src", "path": str(csv)})
        await _call(server_with_key, "begin_session", {"source": "src", "intent": "test"})
        await _call(server_with_key, "end_session", {"outcome": "delivered"})
        listing = await _call(server_with_key, "resume_session", {})
        archive_id = listing["archived_sessions"][0]["session_id"]

        # Manually delete the archive dir, simulating accidental rm.
        shutil.rmtree(tmp_path / "archive" / archive_id)

        result = await _call(server_with_key, "resume_session", {"session_id": archive_id})
        assert "error" in result
        # Stale-archive error includes the listing so the agent has alternatives.
        assert "available" in result
        assert result["available"] == []

        # Index entry consumed, so a follow-up listing is empty.
        listing2 = await _call(server_with_key, "resume_session", {})
        assert listing2["archived_sessions"] == []
