"""Per-workspace role isolation on live Postgres (DAT-816).

The workspace schema is resolved by the DB ROLE, not by client code: the
bootstrap mints ``<ws>_reader`` (``search_path = <ws>_read``, SELECT only
there) and ``<ws>_writer`` (``search_path = <ws>``, exactly the control-table
verbs). These tests stand up TWO full workspace schemas (real ``init_database``
+ ``materialize_read_schema``, no pipeline/LLM) on the session container and
pin the grant surface:

* an unqualified read as a reader resolves that workspace's read schema (the
  search_path IS the schema resolution — no literal anywhere in the client);
* workspace A's reader cannot express a read of workspace B's schemas (no
  USAGE — qualified reads and a re-pointed search_path both fail);
* the reader has NO write path — the legacy control-table carve-out moved to
  the writer role;
* the writer can write exactly the sanctioned control tables in its own raw
  schema (verbs pinned: an ungranted UPDATE fails), nothing in workspace B,
  and nothing in the read schema (reader/writer stay separate clients).
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError

from dataraum.server.workspace import schema_name_for
from dataraum.storage import init_database
from dataraum.storage.read_views import (
    ensure_workspace_roles,
    materialize_read_schema,
    read_schema_name_for,
    reader_role_for,
    writer_role_for,
)

READER_PW = "roles-reader-test-pw"
WRITER_PW = "roles-writer-test-pw"

WS_A = schema_name_for("grant_a")  # ws_grant_a
WS_B = schema_name_for("grant_b")  # ws_grant_b


def _bootstrap_workspace(pg_url: str, schema: str, seed_source: str) -> None:
    """Create one workspace exactly as the engine bootstrap does (sans graph)."""
    engine = create_engine(pg_url, echo=False, future=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_conn, _conn_record):  # noqa: ANN001, ANN202
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute(f'SET search_path TO "{schema}", public')
        finally:
            cursor.close()

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    init_database(engine)
    with engine.begin() as conn:
        materialize_read_schema(conn, schema)
        ensure_workspace_roles(conn, schema, READER_PW, WRITER_PW)
        conn.execute(
            text(
                "INSERT INTO sources (source_id, name, source_type, created_at, updated_at) "
                f"VALUES ('{seed_source}', '{seed_source}', 'csv', now(), now())"
            )
        )
    engine.dispose()


@pytest.fixture(scope="module")
def two_workspaces(pg_url: str) -> Generator[str]:
    """Two bootstrapped workspaces + their minted roles; torn down after."""
    _bootstrap_workspace(pg_url, WS_A, "src-a")
    _bootstrap_workspace(pg_url, WS_B, "src-b")
    yield pg_url
    admin = create_engine(pg_url, echo=False, future=True)
    with admin.begin() as conn:
        for schema in (WS_A, WS_B):
            for role in (reader_role_for(schema), writer_role_for(schema)):
                conn.execute(text(f"DROP OWNED BY {role}"))
                conn.execute(text(f"DROP ROLE {role}"))
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{read_schema_name_for(schema)}" CASCADE'))
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    admin.dispose()


def _connect_as(pg_url: str, role: str, password: str) -> Engine:
    """An engine that LOGS IN as the role — role-level search_path applies."""
    url = make_url(pg_url).set(username=role, password=password)
    return create_engine(url, echo=False, future=True)


def test_reader_search_path_resolves_own_read_schema(two_workspaces: str) -> None:
    """An unqualified read as A's reader hits A's read schema — role IS the resolution."""
    reader = _connect_as(two_workspaces, reader_role_for(WS_A), READER_PW)
    try:
        with reader.connect() as conn:
            assert conn.execute(text("SHOW search_path")).scalar_one() == f"{WS_A}_read"
            names = conn.execute(text("SELECT name FROM sources")).scalars().all()
            assert names == ["src-a"]
    finally:
        reader.dispose()


def test_reader_cannot_read_other_workspace(two_workspaces: str) -> None:
    """A's reader cannot express a read of B — no USAGE on either of B's schemas."""
    reader = _connect_as(two_workspaces, reader_role_for(WS_A), READER_PW)
    try:
        for target in (f'"{WS_B}_read".sources', f'"{WS_B}".sources'):
            with (
                reader.connect() as conn,
                pytest.raises(ProgrammingError, match="permission denied"),
            ):
                conn.execute(text(f"SELECT * FROM {target}"))
        # Re-pointing the session search_path doesn't help: name resolution
        # SKIPS schemas the role lacks USAGE on, so B's relations aren't even
        # visible — the read fails as "does not exist", not merely "denied".
        with reader.connect() as conn, pytest.raises(ProgrammingError, match="does not exist"):
            conn.execute(text(f'SET search_path TO "{WS_B}_read"'))
            conn.execute(text("SELECT * FROM sources"))
    finally:
        reader.dispose()


def test_reader_has_no_write_path(two_workspaces: str) -> None:
    """The reader is SELECT-only: the control-table carve-out lives on the writer now."""
    reader = _connect_as(two_workspaces, reader_role_for(WS_A), READER_PW)
    try:
        # Raw control table: no USAGE on the raw schema at all.
        with reader.connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(
                text(
                    f'INSERT INTO "{WS_A}".config_overlay (overlay_id, type, payload, created_at) '
                    "VALUES ('o1', 'teach', '{}', now())"
                )
            )
        # The pass-through view is auto-updatable, but the reader holds only
        # SELECT on it — the write surface is unreachable from this role.
        with reader.connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(
                text(
                    "INSERT INTO sources (source_id, name, source_type, created_at, updated_at) "
                    "VALUES ('nope', 'nope', 'csv', now(), now())"
                )
            )
    finally:
        reader.dispose()


def test_writer_writes_own_control_tables_only(two_workspaces: str) -> None:
    """The writer holds exactly the control-table verbs, in its own raw schema only."""
    writer = _connect_as(two_workspaces, writer_role_for(WS_A), WRITER_PW)
    try:
        with writer.begin() as conn:
            assert conn.execute(text("SHOW search_path")).scalar_one() == WS_A
            # Unqualified INSERT lands in A's raw sources via the role search_path.
            conn.execute(
                text(
                    "INSERT INTO sources (source_id, name, source_type, created_at, updated_at) "
                    "VALUES ('src-a2', 'src-a2', 'csv', now(), now())"
                )
            )
        # Not a control table → unreachable even in the writer's own schema.
        with writer.connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(text("SELECT * FROM columns"))
        # Granted verbs are exact: sql_snippets carries SELECT+INSERT, no UPDATE.
        with writer.connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(text("UPDATE sql_snippets SET sql = 'x'"))
        # Workspace B stays unreachable.
        with writer.connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(
                text(
                    f'INSERT INTO "{WS_B}".sources (source_id, name, source_type, created_at, updated_at) '
                    "VALUES ('nope', 'nope', 'csv', now(), now())"
                )
            )
        # And so does the READ schema: reader/writer stay separate clients, so
        # the pass-through view names never become ambiguous in one search_path.
        with writer.connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(text(f'SELECT * FROM "{WS_A}_read".sources'))
    finally:
        writer.dispose()

    admin = create_engine(two_workspaces, echo=False, future=True)
    try:
        with admin.connect() as conn:
            names = set(conn.execute(text(f'SELECT name FROM "{WS_A}".sources')).scalars().all())
        assert {"src-a", "src-a2"} <= names
    finally:
        admin.dispose()
