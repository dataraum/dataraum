"""Lane smoke for DAT-325 — Cutover: HTTP MCP as the only entrypoint.

Scope: verify the unified FastAPI control plane wires the contract surface
this lane is supposed to deliver:

* ``/health`` returns the substrate probe (DuckLake + workspace Postgres)
  without auth.
* ``POST /mcp/`` enforces bearer auth — 401 without token, 200 with the
  canonical token, 401 with a wrong token.
* The MCP wire protocol works through the unified app: an ``initialize``
  handshake returns a valid ``InitializeResult`` and the ``Mcp-Session-Id``
  header. This proves the lifespan booted the session manager AND the mount
  forwards ASGI requests correctly.
* ``tools/list`` returns the expected 12-tool surface.
* The lifespan refuses to boot when ``DATARAUM_MCP_TOKEN`` is unset (the
  load-bearing safety invariant of the cutover — no path that serves /mcp/
  without auth exists).

Run:
    uv run pytest tests/platform/smoke_dat325.py -v

This smoke uses Starlette's ``TestClient`` rather than a live uvicorn
process — same ASGI app, same lifespan, same middleware, same mount, but
no socket binding required. The full container path is exercised by the
v0.3 integration smoke on docker-compose (DAT-326 territory).
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from mcp.server import Server
from starlette.testclient import TestClient

TOKEN = "smoke-dat325-correct-horse-battery-staple"
PROTOCOL_VERSION = "2025-03-26"


@pytest.fixture
def stub_create_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Replace ``create_server`` with a tiny no-op MCP server.

    The smoke verifies the transport + auth + mount, not the 12 production
    tool implementations (covered by tests/unit/mcp/*). A stub keeps the
    lifespan boot fast and free of ConnectionManager init.
    """
    from mcp.types import Tool

    stub = Server(name="dat-325-smoke", version="0.0.0")

    @stub.list_tools()
    async def _list_tools() -> list[Tool]:
        return [
            Tool(name=t, description=t, inputSchema={"type": "object"})
            for t in [
                "add_source",
                "list_sources",
                "begin_session",
                "resume_session",
                "look",
                "measure",
                "why",
                "query",
                "run_sql",
                "search_snippets",
                "teach",
                "end_session",
            ]
        ]

    monkeypatch.setattr("dataraum.server.app.create_server", lambda *a, **kw: stub)
    yield


@pytest.fixture
def stub_substrate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Stub the DuckLake + Postgres substrate so the smoke runs hermetically."""
    monkeypatch.setattr("dataraum.server.app.bootstrap_lake", lambda *a, **kw: None)
    monkeypatch.setattr("dataraum.server.app.teardown_lake", lambda: None)
    monkeypatch.setattr(
        "dataraum.server.app.health_probe",
        lambda: {"status": "ok", "schema": "smoke"},
    )
    monkeypatch.setattr(
        "dataraum.server.app._postgres_probe",
        lambda: {"status": "ok"},
    )
    monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://smoke@smoke/smoke")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/smoke-lake")
    yield


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    stub_create_server: None,
    stub_substrate: None,
) -> FastAPI:
    """Construct the control plane app with a valid bearer token."""
    monkeypatch.setenv("DATARAUM_MCP_TOKEN", TOKEN)
    from dataraum.server.app import app as control_plane

    return control_plane


def _initialize_payload() -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "smoke-dat325", "version": "0"},
        },
    }


def _list_tools_payload() -> dict[str, object]:
    return {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}


def _decode_sse_or_json(response: object) -> dict[str, object]:
    """The MCP streamable-HTTP transport may return either JSON or SSE.

    A POST with ``Accept: application/json, text/event-stream`` receives the
    response as a single SSE event containing the JSON-RPC payload. Decode
    either shape so tests don't care about the content negotiation outcome.
    """
    text = response.text  # type: ignore[attr-defined]
    content_type = response.headers.get("content-type", "")  # type: ignore[attr-defined]
    if "text/event-stream" in content_type:
        for line in text.splitlines():
            if line.startswith("data: "):
                return json.loads(line[len("data: ") :])
        pytest.fail(f"no `data:` line in SSE response:\n{text}")
    return json.loads(text)


# ------------------------- health (unauthenticated) -------------------------- #


def test_health_returns_substrate_probe(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["ducklake"]["status"] == "ok"
    assert body["postgres"]["status"] == "ok"


def test_health_does_not_require_auth(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.get("/health")  # no Authorization header
    assert response.status_code == 200


# --------------------------- /mcp/ bearer enforcement ------------------------ #


def test_mcp_without_bearer_returns_401(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.post("/mcp/", json=_initialize_payload())
    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}


def test_mcp_with_wrong_token_returns_401(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/mcp/",
            headers={"Authorization": "Bearer wrong"},
            json=_initialize_payload(),
        )
    assert response.status_code == 401


# ----------------------------- MCP wire protocol ---------------------------- #


def test_initialize_returns_server_info_and_session_id(app: FastAPI) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/mcp/",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/json, text/event-stream",
            },
            json=_initialize_payload(),
        )

    assert response.status_code == 200, response.text
    body = _decode_sse_or_json(response)
    assert body["jsonrpc"] == "2.0"
    assert body["id"] == 1
    assert "result" in body, body
    result = body["result"]
    assert result["protocolVersion"]
    assert result["serverInfo"]["name"] == "dat-325-smoke"
    # The transport assigns a session id on initialize.
    assert "mcp-session-id" in {k.lower() for k in response.headers.keys()}


def test_tools_list_returns_expected_twelve(app: FastAPI) -> None:
    """End-to-end: initialize → notifications/initialized → tools/list returns 12 tools.

    The streamable-HTTP transport requires the client to send
    ``notifications/initialized`` before the server will service further
    requests on the same session. We replay that handshake here.
    """
    with TestClient(app) as client:
        init = client.post(
            "/mcp/",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/json, text/event-stream",
            },
            json=_initialize_payload(),
        )
        assert init.status_code == 200, init.text
        session_id = init.headers.get("mcp-session-id") or init.headers.get("Mcp-Session-Id")
        assert session_id

        # Required handshake step before further requests are honored.
        notified = client.post(
            "/mcp/",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/json, text/event-stream",
                "Mcp-Session-Id": session_id,
            },
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        )
        # Notifications return 202 Accepted with no body.
        assert notified.status_code in (200, 202), notified.text

        tools = client.post(
            "/mcp/",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/json, text/event-stream",
                "Mcp-Session-Id": session_id,
            },
            json=_list_tools_payload(),
        )
        assert tools.status_code == 200, tools.text
        body = _decode_sse_or_json(tools)

    assert body["jsonrpc"] == "2.0"
    assert body["id"] == 2
    tool_names = {t["name"] for t in body["result"]["tools"]}
    assert tool_names == {
        "add_source",
        "list_sources",
        "begin_session",
        "resume_session",
        "look",
        "measure",
        "why",
        "query",
        "run_sql",
        "search_snippets",
        "teach",
        "end_session",
    }


# -------------------- lifespan refuse-to-start (no token) -------------------- #


def test_lifespan_refuses_without_token(
    monkeypatch: pytest.MonkeyPatch,
    stub_create_server: None,
    stub_substrate: None,
) -> None:
    """The load-bearing safety invariant of the cutover.

    There is no code path that serves /mcp/ without bearer auth. The lifespan
    raises before any route is reachable when the token env is missing.
    """
    monkeypatch.delenv("DATARAUM_MCP_TOKEN", raising=False)
    from dataraum.server.app import app as control_plane

    with pytest.raises(RuntimeError, match="DATARAUM_MCP_TOKEN is unset"):
        with TestClient(control_plane):
            pass  # lifespan fires on enter
