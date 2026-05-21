"""Tests for the FastAPI control plane shell.

Post-PR-3a scope (no MCP mount, no bearer auth):
- /health returns 200 when both substrate components probe ok
- /health returns 503 when DuckLake or Postgres is degraded
- lifespan refuses to start when DUCKLAKE_CATALOG_URL or DUCKLAKE_DATA_PATH
  is unset

Engine REST routes (`/api/*`) land in step 3b and get their own tests.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient


@pytest.fixture
def stub_substrate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Stub DuckLake + Postgres + workspace bootstrap so the lifespan doesn't touch real infra."""
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
    # Workspace bootstrap normally pulls a workspace ConnectionManager
    # (which needs DATABASE_URL) and a config copy. Tests that don't
    # exercise it stub both: the manager getter returns a sentinel
    # with the .session_scope attribute the lifespan reads, and the
    # bootstrap call itself is a no-op.
    class _StubMgr:
        session_scope = staticmethod(lambda: None)

    monkeypatch.setattr(
        "dataraum.server.app._get_workspace_manager",
        lambda: _StubMgr(),
    )
    monkeypatch.setattr(
        "dataraum.server.app.bootstrap_workspace",
        lambda *a, **kw: None,
    )
    monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
    yield


@pytest.fixture
def app(stub_substrate: None) -> FastAPI:
    """Construct the control plane app with the substrate stubbed."""
    from dataraum.server.app import app as control_plane

    return control_plane


# ----------------------------------- /health --------------------------------- #


class TestHealth:
    def test_health_ok_when_both_components_reachable(self, app: FastAPI) -> None:
        with TestClient(app) as client:
            response = client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ok"
        assert body["ducklake"]["status"] == "ok"
        assert body["postgres"]["status"] == "ok"


class TestHealthDegraded:
    """Substrate-down readiness behavior: 503, not 200-with-status-field."""

    def test_health_503_when_ducklake_unreachable(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stub_substrate: None,
    ) -> None:
        monkeypatch.setattr(
            "dataraum.server.app.health_probe",
            lambda: {"status": "unreachable"},
        )
        from dataraum.server.app import app as control_plane

        with TestClient(control_plane) as client:
            response = client.get("/health")
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "degraded"
        assert body["ducklake"]["status"] == "unreachable"

    def test_health_503_when_postgres_unreachable(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stub_substrate: None,
    ) -> None:
        monkeypatch.setattr(
            "dataraum.server.app._postgres_probe",
            lambda: {"status": "unreachable"},
        )
        from dataraum.server.app import app as control_plane

        with TestClient(control_plane) as client:
            response = client.get("/health")
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "degraded"
        assert body["postgres"]["status"] == "unreachable"


# -------------------------- lifespan refuse-to-start ------------------------- #


class TestLifespanRefuseToStart:
    def test_unset_catalog_url_raises_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("DUCKLAKE_CATALOG_URL", raising=False)
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
        from dataraum.server.app import app as control_plane

        with pytest.raises(RuntimeError, match="DUCKLAKE_CATALOG_URL is not set"):
            with TestClient(control_plane):
                pass

    def test_unset_data_path_raises_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
        monkeypatch.delenv("DUCKLAKE_DATA_PATH", raising=False)
        from dataraum.server.app import app as control_plane

        with pytest.raises(RuntimeError, match="DUCKLAKE_DATA_PATH is not set"):
            with TestClient(control_plane):
                pass
