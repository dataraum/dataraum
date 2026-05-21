"""Tests for GET /api/workspace.

The route delegates to ``get_workspace_service``, which queries the
workspace registry. These tests stub the session dependency with an
in-memory SQLite Workspace row so the Pydantic shaping is exercised
end-to-end with no real Postgres or DuckLake.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.testclient import TestClient

from dataraum.storage import Workspace


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
def sqlite_workspace_session(
    monkeypatch: pytest.MonkeyPatch,
    stub_substrate: None,
) -> Iterator[Callable[[Workspace | None], FastAPI]]:
    """Provide a FastAPI app with the workspace dep wired to in-memory SQLite.

    Returns a factory: the caller passes a ``Workspace`` row to seed (or
    ``None`` for an empty DB), and gets back the configured app.
    """
    from dataraum.documentation import db_models as _fixes  # noqa: F401
    from dataraum.investigation import db_models as _investigation  # noqa: F401
    from dataraum.pipeline import db_models as _pipeline  # noqa: F401
    from dataraum.pipeline.registry import import_all_phase_models
    from dataraum.query import db_models as _query  # noqa: F401
    from dataraum.query import snippet_models as _snippets  # noqa: F401
    from dataraum.storage import models as _storage_models  # noqa: F401

    import_all_phase_models()

    from dataraum.api.deps import get_workspace_session
    from dataraum.server.app import app as control_plane

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Workspace.__table__.create(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def _scope() -> Iterator[Session]:
        sess = SessionLocal()
        try:
            yield sess
            sess.commit()
        except Exception:
            sess.rollback()
            raise
        finally:
            sess.close()

    def _yield_session() -> Iterator[Session]:
        with _scope() as s:
            yield s

    def configure(seed: Workspace | None) -> FastAPI:
        if seed is not None:
            with _scope() as s:
                s.add(seed)
        control_plane.dependency_overrides[get_workspace_session] = _yield_session
        return control_plane

    yield configure

    control_plane.dependency_overrides.pop(get_workspace_session, None)
    engine.dispose()


def test_get_workspace_returns_active_row(
    sqlite_workspace_session: Callable[[Workspace | None], FastAPI],
) -> None:
    ws_id = str(uuid4())
    created = datetime(2026, 5, 21, 9, 0, 0, tzinfo=UTC)
    seed = Workspace(
        workspace_id=ws_id,
        name="default",
        config_dir="/var/lib/dataraum/workspace/workspaces/" + ws_id + "/config",
        created_at=created,
    )
    app = sqlite_workspace_session(seed)

    with TestClient(app) as client:
        response = client.get("/api/workspace")

    assert response.status_code == 200
    body = response.json()
    assert body["workspace_id"] == ws_id
    assert body["name"] == "default"
    assert body["config_dir"].endswith("/" + ws_id + "/config")
    assert body["created_at"].startswith("2026-05-21")


def test_get_workspace_500_when_no_row_exists(
    sqlite_workspace_session: Callable[[Workspace | None], FastAPI],
) -> None:
    """No workspace = bootstrap was skipped — fail loud, not silently empty."""
    app = sqlite_workspace_session(None)

    # ``raise_server_exceptions=False`` makes Starlette return the 500
    # the production server would emit instead of re-raising in-test.
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/workspace")

    assert response.status_code == 500


def test_get_workspace_picks_lowest_created_at(
    sqlite_workspace_session: Callable[[Workspace | None], FastAPI],
) -> None:
    """Multiple rows shouldn't happen in slice 1 but the picker must be deterministic."""
    older = Workspace(
        workspace_id=str(uuid4()),
        name="older",
        config_dir="/tmp/older",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    newer = Workspace(
        workspace_id=str(uuid4()),
        name="newer",
        config_dir="/tmp/newer",
        created_at=datetime(2026, 5, 21, tzinfo=UTC),
    )
    app = sqlite_workspace_session(older)
    # Seed the second row via a direct add through the override session
    from dataraum.api.deps import get_workspace_session

    factory = app.dependency_overrides[get_workspace_session]
    iterator = factory()
    session = next(iterator)
    session.add(newer)
    session.commit()
    try:
        next(iterator)
    except StopIteration:
        pass

    with TestClient(app) as client:
        response = client.get("/api/workspace")

    assert response.status_code == 200
    assert response.json()["name"] == "older"


def test_openapi_includes_workspace_route(stub_substrate: None) -> None:
    """The auto-generated OpenAPI spec exposes the route + the Workspace schema."""
    from dataraum.server.app import app as control_plane

    spec = control_plane.openapi()
    paths = spec.get("paths", {})
    assert "/api/workspace" in paths
    assert "get" in paths["/api/workspace"]
    schemas = spec.get("components", {}).get("schemas", {})
    assert "Workspace" in schemas


# `Path` is imported only to silence the unused-import warning for the
# Path-typed examples in this module; kept to mirror the test_sources.py pattern.
_ = Path
