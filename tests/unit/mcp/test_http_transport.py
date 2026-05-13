"""DAT-291 Phase 3 — HTTP transport: bearer auth + /health + refuse-to-start.

These tests cover the middleware logic, the `/health` bypass, and the
`main()` refusal to start the HTTP transport when DATARAUM_MCP_TOKEN is unset.
The MCP wire protocol over /mcp is exercised by manual smoke against the
`claude` CLI; here we only assert auth gating, not initialize/call_tool flows.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from mcp.server import Server
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from dataraum.mcp import server as server_mod
from dataraum.mcp.server import BearerAuthMiddleware, _build_http_app, main

TOKEN = "test-token-correct-horse-battery-staple"


@pytest.fixture
def auth_app() -> Starlette:
    """Tiny Starlette app wired with BearerAuthMiddleware.

    Two routes: `/echo` (protected) and `/health` (must bypass auth). Keeps the
    middleware test independent of StreamableHTTPSessionManager.
    """

    async def echo(_request: Any) -> JSONResponse:
        return JSONResponse({"ok": True})

    async def health(_request: Any) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    return Starlette(
        routes=[
            Route("/health", health, methods=["GET"]),
            Route("/echo", echo, methods=["POST", "GET"]),
        ],
        middleware=[Middleware(BearerAuthMiddleware, token=TOKEN)],
    )


class TestBearerAuthMiddleware:
    def test_health_bypasses_auth_no_header(self, auth_app: Starlette) -> None:
        with TestClient(auth_app) as client:
            response = client.get("/health")
        assert response.status_code == 200

    def test_health_bypasses_auth_with_bad_header(self, auth_app: Starlette) -> None:
        with TestClient(auth_app) as client:
            response = client.get("/health", headers={"Authorization": "Bearer wrong"})
        assert response.status_code == 200

    def test_correct_bearer_passes_through(self, auth_app: Starlette) -> None:
        with TestClient(auth_app) as client:
            response = client.post("/echo", headers={"Authorization": f"Bearer {TOKEN}"})
        assert response.status_code == 200
        assert response.json() == {"ok": True}

    def test_missing_auth_header_returns_401(self, auth_app: Starlette) -> None:
        with TestClient(auth_app) as client:
            response = client.post("/echo")
        assert response.status_code == 401
        assert response.json() == {"error": "unauthorized"}

    def test_wrong_scheme_returns_401(self, auth_app: Starlette) -> None:
        with TestClient(auth_app) as client:
            response = client.post("/echo", headers={"Authorization": f"Basic {TOKEN}"})
        assert response.status_code == 401

    def test_wrong_token_returns_401(self, auth_app: Starlette) -> None:
        with TestClient(auth_app) as client:
            response = client.post("/echo", headers={"Authorization": "Bearer wrong-token"})
        assert response.status_code == 401

    def test_empty_bearer_returns_401(self, auth_app: Starlette) -> None:
        with TestClient(auth_app) as client:
            response = client.post("/echo", headers={"Authorization": "Bearer "})
        assert response.status_code == 401

    def test_lowercase_bearer_scheme_accepted(self, auth_app: Starlette) -> None:
        # RFC 7235: auth scheme is case-insensitive.
        with TestClient(auth_app) as client:
            response = client.post("/echo", headers={"Authorization": f"bearer {TOKEN}"})
        assert response.status_code == 200


@pytest.fixture
def stub_create_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Replace create_server() with a tiny no-op MCP server.

    Keeps _build_http_app() fast: the real create_server boots ConnectionManager,
    DuckDB, etc. None of that is needed for testing route + middleware wiring.
    """
    stub = Server(name="dat-291-test", version="0.0.0")
    monkeypatch.setattr(server_mod, "create_server", lambda *args, **kwargs: stub)
    yield


class TestBuildHttpApp:
    def test_health_endpoint_returns_ok(self, stub_create_server: None) -> None:
        app = _build_http_app(token=TOKEN)
        with TestClient(app) as client:
            response = client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ok"
        assert "version" in body

    def test_mcp_endpoint_requires_bearer(self, stub_create_server: None) -> None:
        # No initialize handshake — the middleware should short-circuit at 401
        # long before the streamable-http manager sees the request.
        app = _build_http_app(token=TOKEN)
        with TestClient(app) as client:
            response = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "ping"})
        assert response.status_code == 401


class TestMainRefuseToStart:
    def test_http_transport_without_token_exits_2(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.delenv("DATARAUM_MCP_TOKEN", raising=False)
        monkeypatch.setattr("sys.argv", ["dataraum-mcp", "--transport", "http"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 2
        captured = capsys.readouterr()
        assert "DATARAUM_MCP_TOKEN" in captured.err
        assert "refuses to start" in captured.err

    def test_http_transport_with_empty_token_exits_2(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setenv("DATARAUM_MCP_TOKEN", "")
        monkeypatch.setattr("sys.argv", ["dataraum-mcp", "--transport", "http"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 2
        assert "DATARAUM_MCP_TOKEN" in capsys.readouterr().err
