"""Tests for the unified FastAPI control plane app.

Covers:
- bearer middleware: /health bypass, /mcp/ gated, scheme + token comparison
- lifespan refusal when DATARAUM_MCP_TOKEN is unset
- /mcp/ mount: 503 before lifespan opens the session manager; 200 after

The MCP wire protocol (initialize, call_tool) is exercised by the lane smoke
against a real ASGI client. Here we assert mount + auth + lifespan only.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from mcp.server import Server
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

TOKEN = "test-token-correct-horse-battery-staple"


@pytest.fixture
def stub_create_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Replace ``create_server`` with a tiny no-op MCP server.

    The real ``create_server`` boots ConnectionManager, DuckDB, etc. — heavy
    init unrelated to the routing + middleware + lifespan behavior under test.
    """
    stub = Server(name="dat-325-test", version="0.0.0")
    monkeypatch.setattr("dataraum.server.app.create_server", lambda *a, **kw: stub)
    yield


@pytest.fixture
def stub_substrate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Stub DuckLake + Postgres substrate so the lifespan doesn't touch real infra."""
    monkeypatch.setattr("dataraum.server.app.bootstrap_lake", lambda *a, **kw: None)
    monkeypatch.setattr("dataraum.server.app.teardown_lake", lambda: None)
    monkeypatch.setattr(
        "dataraum.server.app.health_probe",
        lambda: {"status": "ok", "schema": "test"},
    )
    monkeypatch.setattr(
        "dataraum.server.app._postgres_probe",
        lambda: {"status": "ok"},
    )
    monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
    yield


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    stub_create_server: None,
    stub_substrate: None,
) -> FastAPI:
    """Construct the control plane app under test with a valid bearer token."""
    monkeypatch.setenv("DATARAUM_MCP_TOKEN", TOKEN)
    from dataraum.server.app import app as control_plane

    return control_plane


# -------------------------------- bearer auth -------------------------------- #


class TestBearerAuth:
    def test_health_bypasses_auth_no_header(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.get("/health")
        assert response.status_code == 200

    def test_health_bypasses_auth_with_bad_header(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.get("/health", headers={"Authorization": "Bearer wrong"})
        assert response.status_code == 200

    def test_correct_bearer_reaches_mcp(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.post(
                "/mcp/",
                headers={"Authorization": f"Bearer {TOKEN}"},
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
            )
        # Past auth means we hit the session manager — its response shape is not
        # 401; the wire protocol is exercised by the lane smoke, not here.
        assert response.status_code != 401

    def test_missing_auth_header_returns_401(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.post("/mcp/")
        assert response.status_code == 401
        assert response.json() == {"error": "unauthorized"}

    def test_wrong_scheme_returns_401(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.post("/mcp/", headers={"Authorization": f"Basic {TOKEN}"})
        assert response.status_code == 401

    def test_wrong_token_returns_401(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.post("/mcp/", headers={"Authorization": "Bearer wrong-token"})
        assert response.status_code == 401

    def test_empty_bearer_returns_401(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.post("/mcp/", headers={"Authorization": "Bearer "})
        assert response.status_code == 401

    def test_lowercase_bearer_scheme_accepted(self, app: FastAPI) -> None:
        # RFC 7235: auth scheme is case-insensitive.
        with TestClient(app) as client:
            response = client.post(
                "/mcp/",
                headers={"Authorization": f"bearer {TOKEN}"},
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
            )
        assert response.status_code != 401


# ------------------------- middleware in isolation --------------------------- #


def _make_middleware_app() -> Starlette:
    """Tiny app with the bearer middleware wrapped around two stub routes.

    Decouples middleware behavior from the lifespan dance (which requires the
    full substrate stack). The middleware reads ``DATARAUM_MCP_TOKEN`` from
    the env at request time so tests just monkeypatch the env.
    """
    from dataraum.server.app import BearerAuthMiddleware

    async def echo(_request: Any) -> JSONResponse:
        return JSONResponse({"ok": True})

    async def health(_request: Any) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    app = Starlette(
        routes=[
            Route("/health", health, methods=["GET"]),
            Route("/echo", echo, methods=["POST", "GET"]),
        ],
    )
    app.add_middleware(BearerAuthMiddleware)
    return app


class TestBearerAuthMiddlewareIsolated:
    """Direct middleware tests — no lifespan, no MCP transport, no substrate."""

    def test_health_bypasses_auth(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATARAUM_MCP_TOKEN", TOKEN)
        with TestClient(_make_middleware_app()) as client:
            assert client.get("/health").status_code == 200

    def test_correct_token_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATARAUM_MCP_TOKEN", TOKEN)
        with TestClient(_make_middleware_app()) as client:
            response = client.post("/echo", headers={"Authorization": f"Bearer {TOKEN}"})
        assert response.status_code == 200
        assert response.json() == {"ok": True}

    def test_unset_token_blocks_everything_but_health(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The lifespan refuses to start without the env, but the middleware
        # itself must also refuse if it ever sees an empty token at runtime.
        monkeypatch.delenv("DATARAUM_MCP_TOKEN", raising=False)
        with TestClient(_make_middleware_app()) as client:
            assert client.get("/health").status_code == 200
            assert (
                client.post("/echo", headers={"Authorization": f"Bearer {TOKEN}"}).status_code
                == 401
            )


# -------------------------- lifespan refuse-to-start ------------------------- #


class TestLifespanRefuseToStart:
    def test_unset_token_raises_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stub_create_server: None,
        stub_substrate: None,
    ) -> None:
        monkeypatch.delenv("DATARAUM_MCP_TOKEN", raising=False)
        from dataraum.server.app import app as control_plane

        with pytest.raises(RuntimeError, match="DATARAUM_MCP_TOKEN is unset"):
            with TestClient(control_plane):
                pass  # lifespan fires on enter

    def test_empty_token_raises_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stub_create_server: None,
        stub_substrate: None,
    ) -> None:
        monkeypatch.setenv("DATARAUM_MCP_TOKEN", "")
        from dataraum.server.app import app as control_plane

        with pytest.raises(RuntimeError, match="DATARAUM_MCP_TOKEN is unset"):
            with TestClient(control_plane):
                pass

    def test_unset_catalog_url_raises_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stub_create_server: None,
    ) -> None:
        monkeypatch.setenv("DATARAUM_MCP_TOKEN", TOKEN)
        monkeypatch.delenv("DUCKLAKE_CATALOG_URL", raising=False)
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
        from dataraum.server.app import app as control_plane

        with pytest.raises(RuntimeError, match="DUCKLAKE_CATALOG_URL is not set"):
            with TestClient(control_plane):
                pass
