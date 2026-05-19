"""MCP session-lifecycle integration tests (post-DAT-321).

DAT-192 originally enforced "two SQLite files, no leak" by opening
``workspace.db`` and per-session ``metadata.db`` files directly via
``sqlite3.connect()``. DAT-321 unified everything onto a single workspace
Postgres: there are no separate DB files anymore, only Postgres tables
scoped by ``session_id``.

The product invariants these tests cover are unchanged:

- ``add_source`` populates ``sources`` but no per-session tables.
- Same source set → same per-session DuckDB directory (DuckDB stays
  per-session post-DAT-321 — L4 swaps it for DuckLake).
- Different source sets → different per-session directories.
- ``begin_session`` writes both the workspace ``ActiveSession`` pointer
  and a new ``InvestigationSession`` row in the same Postgres DB.
- A retried ``begin_session`` marks orphan ``active`` rows as
  ``abandoned``.
- ``resume_session`` restores the per-session DuckDB directory in place
  and the workspace pointer matches.

Assertions go through SQLAlchemy against the testcontainers Postgres,
not raw ``sqlite3.connect``.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from dataraum.investigation.db_models import InvestigationSession
from dataraum.mcp.db_models import ActiveSession, ArchivedSession
from dataraum.storage import Source


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


@contextmanager
def _ws_session(pg_url: str) -> Iterator[Session]:
    """Read-side SQLAlchemy session bound to the testcontainer Postgres.

    Owns the engine for the duration of the ``with`` block and disposes it
    on exit — otherwise the pooled psycopg connections leak (Python 3.12+
    raises ``ResourceWarning`` when GC catches an open psycopg connection).
    """
    engine = create_engine(pg_url, future=True)
    try:
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        with factory() as sess:
            yield sess
    finally:
        engine.dispose()


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")


@pytest.fixture
def db_url(monkeypatch: pytest.MonkeyPatch, pg_url_clean: str) -> str:
    """Point the MCP server at the testcontainer Postgres (clean per test)."""
    monkeypatch.setenv("DATABASE_URL", pg_url_clean)
    return pg_url_clean


class TestAddSourceWritesOnlyToWorkspace:
    """add_source populates `sources` but no per-session tables."""

    @pytest.mark.asyncio
    async def test_add_source_populates_workspace_sources_only(
        self, tmp_path: Path, api_key: None, db_url: str
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        with _ws_session(db_url) as s:
            sources = list(s.execute(select(Source)).scalars().all())
            assert len(sources) == 1
            assert sources[0].name == "src1"
            assert sources[0].source_type == "csv"

            # Per-session tables must be empty — pipeline hasn't run yet.
            for table in ("tables", "columns", "entropy_objects"):
                count = s.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                assert count == 0, f"{table} should be empty after add_source"

        # No per-session DuckDB dirs created yet — begin_session not called.
        assert not (tmp_path / "sessions").exists()


class TestSameSourcesReuseSessionDir:
    """Same source → same fingerprint → same per-session DuckDB directory."""

    @pytest.mark.asyncio
    async def test_same_sources_same_fingerprint_dir(
        self, tmp_path: Path, api_key: None, db_url: str
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        await _call(server, "begin_session", {"source": "src1", "intent": "first"})
        sessions_dir = tmp_path / "sessions"
        first_dirs = sorted(p.name for p in sessions_dir.iterdir())
        assert len(first_dirs) == 1

        await _call(server, "end_session", {"outcome": "abandoned"})
        await _call(server, "begin_session", {"source": "src1", "intent": "second"})

        second_dirs = sorted(p.name for p in sessions_dir.iterdir())
        assert second_dirs == first_dirs, "Same sources must produce the same fingerprint directory"


class TestDifferentSourcesIsolation:
    """Different source sets land in different per-session directories,
    and each archived InvestigationSession row carries the matching intent."""

    @pytest.mark.asyncio
    async def test_different_sources_different_dirs_no_overwrite(
        self, tmp_path: Path, api_key: None, db_url: str
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)

        csv_a = _make_csv(tmp_path, "a.csv", "x,y\n1,2\n")
        await _call(server, "add_source", {"name": "src_a", "path": str(csv_a)})
        await _call(server, "begin_session", {"source": "src_a", "intent": "session A"})
        await _call(server, "end_session", {"outcome": "abandoned"})

        csv_b = _make_csv(tmp_path, "b.csv", "x,y\n3,4\n")
        await _call(server, "add_source", {"name": "src_b", "path": str(csv_b)})
        await _call(server, "begin_session", {"source": "src_b", "intent": "session B"})
        await _call(server, "end_session", {"outcome": "abandoned"})

        archive = tmp_path / "archive"
        archived_dirs = sorted(p for p in archive.iterdir())
        assert len(archived_dirs) == 2, "Each source set archives its own DuckDB dir"

        # All InvestigationSession rows live in the unified Postgres now.
        # Both archived sessions must be present with the right intents.
        with _ws_session(db_url) as s:
            rows = list(
                s.execute(
                    select(InvestigationSession.intent).where(
                        InvestigationSession.intent.in_(["session A", "session B"])
                    )
                )
                .scalars()
                .all()
            )
            assert set(rows) == {"session A", "session B"}, (
                "Each InvestigationSession is scoped by its own session_id; "
                "intents must be present and distinct"
            )


class TestBeginSessionWritesToBothDBs:
    """begin_session sets the workspace ActiveSession pointer AND the
    InvestigationSession row, with matching session_id."""

    @pytest.mark.asyncio
    async def test_pointer_and_session_row_present(
        self, tmp_path: Path, api_key: None, db_url: str
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        await _call(server, "begin_session", {"source": "src1", "intent": "verify_writes"})

        with _ws_session(db_url) as s:
            pointer = s.execute(select(ActiveSession)).scalar_one()
            assert pointer.session_id and pointer.fingerprint

            inv = s.execute(
                select(InvestigationSession).where(
                    InvestigationSession.session_id == pointer.session_id
                )
            ).scalar_one()
            assert inv.intent == "verify_writes"
            assert inv.status == "active"

        # Per-session DuckDB dir name matches the workspace pointer fingerprint.
        sessions_dir = tmp_path / "sessions"
        session_dirs = list(sessions_dir.iterdir())
        assert len(session_dirs) == 1
        assert session_dirs[0].name == pointer.fingerprint


class TestGhostSessionCleanup:
    """A retried begin_session abandons orphan ``active`` InvestigationSession
    rows (left over from a crashed prior attempt that wrote the row but
    failed to set the workspace ActiveSession pointer)."""

    @pytest.mark.asyncio
    async def test_retry_marks_orphan_as_abandoned(
        self, tmp_path: Path, api_key: None, db_url: str
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        await _call(server, "begin_session", {"source": "src1", "intent": "first attempt"})

        # Simulate a crashed begin_session: clear the workspace pointer
        # but leave the active InvestigationSession in place.
        with _ws_session(db_url) as s:
            s.execute(text("DELETE FROM active_session"))
            s.commit()

        await _call(server, "begin_session", {"source": "src1", "intent": "fresh attempt"})

        with _ws_session(db_url) as s:
            rows = list(
                s.execute(
                    select(InvestigationSession.intent, InvestigationSession.status)
                    .where(InvestigationSession.intent.in_(["first attempt", "fresh attempt"]))
                    .order_by(InvestigationSession.started_at)
                ).all()
            )
            assert [r.intent for r in rows] == ["first attempt", "fresh attempt"]
            assert [r.status for r in rows] == ["abandoned", "active"]


class TestResumeSessionRestoresArchive:
    """resume_session restores an archived per-session DuckDB directory in
    place, consumes the archive index row, and sets the workspace pointer
    to the restored session."""

    @pytest.mark.asyncio
    async def test_full_archive_restore_cycle(
        self, tmp_path: Path, api_key: None, db_url: str
    ) -> None:
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})

        await _call(server, "begin_session", {"source": "src1", "intent": "first pass"})
        sessions_dir = tmp_path / "sessions"
        original_fp = next(sessions_dir.iterdir()).name

        await _call(server, "end_session", {"outcome": "delivered", "summary": "first done"})
        assert list(sessions_dir.iterdir()) == []
        archives_before = list((tmp_path / "archive").iterdir())
        assert len(archives_before) == 1
        archive_id = archives_before[0].name

        result = await _call(server, "resume_session", {"session_id": archive_id})
        assert "error" not in result
        assert result["resumed_from"] == archive_id

        restored_dirs = list(sessions_dir.iterdir())
        assert len(restored_dirs) == 1
        assert restored_dirs[0].name == original_fp
        assert not (tmp_path / "archive" / archive_id).exists()

        with _ws_session(db_url) as s:
            # Archive index row consumed.
            archived = s.execute(
                select(ArchivedSession).where(ArchivedSession.session_id == archive_id)
            ).scalar_one_or_none()
            assert archived is None

            # Workspace pointer matches the restored fingerprint.
            pointer = s.execute(select(ActiveSession)).scalar_one()
            assert pointer.fingerprint == original_fp

            # Original session is terminal; new resumed session is active.
            rows = list(
                s.execute(
                    select(InvestigationSession.intent, InvestigationSession.status)
                    .where(InvestigationSession.intent.in_(["first pass", "Resumed: first pass"]))
                    .order_by(InvestigationSession.started_at)
                ).all()
            )
            assert [r.intent for r in rows] == ["first pass", "Resumed: first pass"]
            assert [r.status for r in rows] == ["delivered", "active"]

    @pytest.mark.asyncio
    async def test_resume_then_end_creates_new_archive_entry(
        self, tmp_path: Path, api_key: None, db_url: str
    ) -> None:
        """After resume + end_session, the archive index has the new
        session_id (not the consumed original)."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=tmp_path)
        csv = _make_csv(tmp_path, "data.csv")
        await _call(server, "add_source", {"name": "src1", "path": str(csv)})
        await _call(server, "begin_session", {"source": "src1", "intent": "x"})
        await _call(server, "end_session", {"outcome": "delivered"})

        listing = await _call(server, "resume_session", {})
        first_id = listing["archived_sessions"][0]["session_id"]

        await _call(server, "resume_session", {"session_id": first_id})
        await _call(server, "end_session", {"outcome": "delivered"})

        listing2 = await _call(server, "resume_session", {})
        archives = listing2["archived_sessions"]
        assert len(archives) == 1
        assert archives[0]["session_id"] != first_id
        assert archives[0]["intent"] == "Resumed: x"
