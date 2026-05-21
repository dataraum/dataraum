"""Tests for GET /api/sources.

The route delegates to ``list_sources_service``, which delegates to
``SourceManager.list_sources``. These tests stub at the SourceManager
boundary so the service's Pydantic shaping is exercised end-to-end and
no real workspace DB is needed.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient


@dataclass
class _SourceInfoStub:
    """Minimal stand-in for ``dataraum.sources.manager.SourceInfo``.

    Only the fields the service reads — no risk of drift from upstream
    schema additions (the service projects through Pydantic, so extra
    fields would be ignored).
    """

    name: str
    source_type: str
    status: str
    path: str | None = None
    backend: str | None = None
    recipe_tables: list[str] | None = None


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
def app_with_sources(
    monkeypatch: pytest.MonkeyPatch,
    stub_substrate: None,
) -> Iterator[tuple[FastAPI, list[_SourceInfoStub]]]:
    """Wire up the control plane app with the SourceManager stubbed.

    Returns the app and a mutable list the caller can populate with
    ``_SourceInfoStub`` instances; the stubbed ``SourceManager.list_sources``
    returns whatever is in the list at request time.
    """
    from dataraum.server.app import app as control_plane

    sources: list[_SourceInfoStub] = []

    class _StubManager:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def list_sources(self, status_filter: str | None = None) -> list[_SourceInfoStub]:
            del status_filter
            return list(sources)

    # Patch where the service imports SourceManager from.
    monkeypatch.setattr("dataraum.sources.manager.SourceManager", _StubManager)

    # Override the FastAPI dependency to skip the real ConnectionManager —
    # the stubbed SourceManager ignores the session anyway.
    from dataraum.api.deps import get_workspace_session

    def _stub_session() -> Iterator[None]:
        yield None  # session never touched by _StubManager

    control_plane.dependency_overrides[get_workspace_session] = _stub_session
    try:
        yield control_plane, sources
    finally:
        control_plane.dependency_overrides.pop(get_workspace_session, None)


def test_list_sources_empty(
    app_with_sources: tuple[FastAPI, list[_SourceInfoStub]],
) -> None:
    app, _sources = app_with_sources
    with TestClient(app) as client:
        response = client.get("/api/sources")
    assert response.status_code == 200
    assert response.json() == []


def test_list_sources_returns_registered_entries(
    app_with_sources: tuple[FastAPI, list[_SourceInfoStub]],
) -> None:
    app, sources = app_with_sources
    sources.append(
        _SourceInfoStub(
            name="orders",
            source_type="csv",
            status="configured",
            path="/var/lib/dataraum/sources/orders.csv",
        )
    )
    sources.append(
        _SourceInfoStub(
            name="erp",
            source_type="db_recipe",
            status="configured",
            path="/var/lib/dataraum/sources/erp.yaml",
            backend="mssql",
            recipe_tables=["customers", "invoices"],
        )
    )

    with TestClient(app) as client:
        response = client.get("/api/sources")

    assert response.status_code == 200
    payload = response.json()
    assert payload == [
        {
            "name": "orders",
            "type": "csv",
            "status": "configured",
            "path": "/var/lib/dataraum/sources/orders.csv",
            "backend": None,
            "recipe_tables": None,
        },
        {
            "name": "erp",
            "type": "db_recipe",
            "status": "configured",
            "path": "/var/lib/dataraum/sources/erp.yaml",
            "backend": "mssql",
            "recipe_tables": ["customers", "invoices"],
        },
    ]


def test_list_sources_omits_recipe_tables_when_empty(
    app_with_sources: tuple[FastAPI, list[_SourceInfoStub]],
) -> None:
    """The service maps an empty ``recipe_tables`` list to ``None`` in the response.

    File sources (CSV/Parquet/etc.) carry ``recipe_tables=[]`` from
    ``SourceManager.list_sources`` since ``_recipe_table_names`` returns an
    empty list for non-recipe sources. The Pydantic-side projection
    surfaces that as null, not an empty list, so the cockpit can
    distinguish "no tables declared" from "no recipe at all".
    """
    app, sources = app_with_sources
    sources.append(
        _SourceInfoStub(
            name="orders",
            source_type="csv",
            status="configured",
            path="/orders.csv",
            recipe_tables=[],
        )
    )

    with TestClient(app) as client:
        response = client.get("/api/sources")

    assert response.status_code == 200
    body = response.json()
    assert body[0]["recipe_tables"] is None


def test_openapi_includes_sources_route(stub_substrate: None) -> None:
    """The auto-generated OpenAPI spec exposes the route + the Source schema.

    Guards the contract surface that ``scripts/export_openapi.py`` (and the
    eventual CI publish step in PR 3c) will dump for ``dataraum-api``.
    """
    from dataraum.server.app import app as control_plane

    spec = control_plane.openapi()
    paths = spec.get("paths", {})
    assert "/api/sources" in paths
    assert "get" in paths["/api/sources"]
    schemas = spec.get("components", {}).get("schemas", {})
    assert "Source" in schemas
