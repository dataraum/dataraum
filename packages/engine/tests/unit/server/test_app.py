"""Tests for the Starlette control plane shell.

Post-DAT-339 0c scope:
- /health returns 200 when both substrate components probe ok
- /health returns 503 when DuckLake or Postgres is degraded
- lifespan refuses to start when DUCKLAKE_CATALOG_URL or DUCKLAKE_DATA_PATH
  is unset
- lifespan calls ConnectionManager.initialize() so the workspace schema +
  tables materialize before any request lands (eager substrate init,
  carried from 0b follow-up)
- /measure, /query, /probe return 501 Not Implemented stubs
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.testclient import TestClient


@pytest.fixture
def init_spy(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Spy on ConnectionManager(...).initialize() calls during lifespan."""
    counter: dict[str, int] = {"init_calls": 0, "close_calls": 0}

    class _StubManager:
        def __init__(self, _config: object) -> None:
            pass

        def initialize(self) -> None:
            counter["init_calls"] += 1

        def close(self) -> None:
            counter["close_calls"] += 1

    monkeypatch.setattr("dataraum.server.app.ConnectionManager", _StubManager)
    monkeypatch.setattr(
        "dataraum.server.app.ConnectionConfig.for_workspace",
        object,
    )
    return counter


@pytest.fixture
def stub_substrate(
    monkeypatch: pytest.MonkeyPatch,
    init_spy: dict[str, int],
) -> Iterator[None]:
    """Stub DuckLake + workspace bootstrap so the lifespan doesn't touch real infra."""
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
    monkeypatch.setattr(
        "dataraum.server.app.bootstrap_workspace",
        lambda *a, **kw: None,
    )
    monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
    yield


@pytest.fixture
def app(stub_substrate: None) -> Starlette:
    """Construct the control plane app with the substrate stubbed."""
    from dataraum.server.app import app as control_plane

    return control_plane


# ----------------------------------- /health --------------------------------- #


class TestHealth:
    def test_health_ok_when_both_components_reachable(self, app: Starlette) -> None:
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


# -------------------------- lifespan eager substrate-init ------------------- #


class TestLifespanEagerInit:
    """Workspace SQLAlchemy substrate must materialize before any request."""

    def test_initialize_runs_during_lifespan_startup(
        self,
        app: Starlette,
        init_spy: dict[str, int],
    ) -> None:
        # No requests; just enter + exit the lifespan via TestClient.
        with TestClient(app):
            pass
        assert init_spy["init_calls"] == 1, (
            "Lifespan must call ConnectionManager.initialize() at startup so "
            "the ws_<id> schema + tables exist before requests land."
        )
        assert init_spy["close_calls"] == 1, "Lifespan teardown must close the workspace manager."

    def test_workspace_manager_exposed_on_app_state(
        self,
        app: Starlette,
        init_spy: dict[str, int],  # noqa: ARG002 — spy fixture wires the stub
    ) -> None:
        with TestClient(app):
            assert hasattr(app.state, "workspace_manager")


# ---------------------------- kernel verb stubs ----------------------------- #


class TestKernelStubs:
    """measure / query / probe are not implemented in 0c — placeholders only."""

    @pytest.mark.parametrize("path", ["/measure", "/query", "/probe"])
    def test_stub_returns_501(self, app: Starlette, path: str) -> None:
        with TestClient(app) as client:
            response = client.post(path)
        assert response.status_code == 501
        body = response.json()
        assert "not implemented" in body["detail"].lower()


# -------------------------- lifespan refuse-to-start ------------------------- #


class TestLifespanRefuseToStart:
    def test_unset_catalog_url_raises_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("DUCKLAKE_CATALOG_URL", raising=False)
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
        from dataraum.server.app import app as control_plane

        with pytest.raises(ValidationError, match="ducklake_catalog_url"):
            with TestClient(control_plane):
                pass

    def test_unset_data_path_raises_at_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
        monkeypatch.delenv("DUCKLAKE_DATA_PATH", raising=False)
        from dataraum.server.app import app as control_plane

        with pytest.raises(ValidationError, match="ducklake_data_path"):
            with TestClient(control_plane):
                pass


# ------------------- lifespan partial-init teardown safety ------------------ #


class TestLifespanPartialInitTeardown:
    """If post-lake init raises, teardown_lake must still run.

    Without nested try/finally a failed `bootstrap_workspace` or
    `ConnectionManager.initialize()` would leak the open DuckLake
    catalog connection. The lifespan wraps both inner steps in an
    outer try/finally that always tears down the lake.
    """

    def test_teardown_lake_runs_when_bootstrap_workspace_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        teardown_calls: dict[str, int] = {"n": 0}

        monkeypatch.setattr("dataraum.server.app.bootstrap_lake", lambda *a, **kw: None)
        monkeypatch.setattr(
            "dataraum.server.app.teardown_lake",
            lambda: teardown_calls.__setitem__("n", teardown_calls["n"] + 1),
        )

        def _boom() -> None:
            raise RuntimeError("workspace boom")

        monkeypatch.setattr("dataraum.server.app.bootstrap_workspace", _boom)
        monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
        from dataraum.server.app import app as control_plane

        with pytest.raises(RuntimeError, match="workspace boom"):
            with TestClient(control_plane):
                pass
        assert teardown_calls["n"] == 1, (
            "teardown_lake must run even when bootstrap_workspace raises; "
            "otherwise the DuckLake catalog connection leaks on partial init."
        )

    def test_teardown_lake_runs_when_substrate_init_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        teardown_calls: dict[str, int] = {"n": 0}
        close_calls: dict[str, int] = {"n": 0}

        monkeypatch.setattr("dataraum.server.app.bootstrap_lake", lambda *a, **kw: None)
        monkeypatch.setattr(
            "dataraum.server.app.teardown_lake",
            lambda: teardown_calls.__setitem__("n", teardown_calls["n"] + 1),
        )
        monkeypatch.setattr("dataraum.server.app.bootstrap_workspace", lambda *a, **kw: None)

        class _FailingManager:
            def __init__(self, _config: object) -> None:
                pass

            def initialize(self) -> None:
                raise RuntimeError("init boom")

            def close(self) -> None:
                close_calls["n"] += 1

        monkeypatch.setattr("dataraum.server.app.ConnectionManager", _FailingManager)
        monkeypatch.setattr(
            "dataraum.server.app.ConnectionConfig.for_workspace",
            object,
        )
        monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
        from dataraum.server.app import app as control_plane

        with pytest.raises(RuntimeError, match="init boom"):
            with TestClient(control_plane):
                pass
        assert teardown_calls["n"] == 1
        assert close_calls["n"] == 1, (
            "ConnectionManager.close() must run even when initialize() raises "
            "(documented as safe on partial init)."
        )
