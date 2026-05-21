"""Lane smoke for DAT-358 — Engine workspace foundation.

Scope: verify the contract surface the cockpit relies on after EW:

* FastAPI startup runs ``bootstrap_workspace`` against real Postgres
  and either picks the existing Workspace row or creates ``"default"``.
* The ``${DATARAUM_HOME}/workspaces/<id>/config/`` overlay is populated
  on first boot by copying the read-only baked-in defaults.
* ``GET /api/workspace`` returns the active workspace's metadata.
* Edits in the overlay survive a "restart" (re-running the lifespan
  against the same Postgres + DATARAUM_HOME mount) — proxy for the
  ticket's "edit a config file inside the container, restart, persists"
  acceptance check.
* The ``_adhoc`` vertical scaffold lands under the overlay (cold-start
  induction has its write target).

The integration-on-compose check ("docker compose up; curl
/api/workspace") belongs to ``main``, not this lane — see the three-tier
smoke table in the ``/take`` skill.

Run:
    uv run pytest tests/platform/smoke_dat_358.py -v
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from dataraum.core.config import reset_active_workspace_for_tests, reset_config_root


@pytest.fixture
def baked_in_config(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A minimal baked-in config tree for the bootstrap to copy from.

    Session-scoped via tmp_path_factory so the path stays stable across
    a "restart" inside one test — DATARAUM_CONFIG_PATH points here both
    before and after.
    """
    src = tmp_path_factory.mktemp("baked_in_config")
    (src / "phases").mkdir()
    (src / "phases" / "import.yaml").write_text("junk_columns: []\n")
    (src / "pipeline.yaml").write_text("phases: {}\npipeline: {}\n")
    (src / "verticals" / "finance").mkdir(parents=True)
    (src / "verticals" / "finance" / "ontology.yaml").write_text("concepts: []\n")
    return src


@pytest.fixture
def datadraum_home(tmp_path: Path) -> Path:
    home = tmp_path / "datahome"
    home.mkdir()
    return home


@pytest.fixture(autouse=True)
def _isolate_active_workspace() -> Iterator[None]:
    """Reset the module-level workspace pointer between tests."""
    yield
    reset_active_workspace_for_tests()
    reset_config_root()


@pytest.fixture
def wired_app(
    monkeypatch: pytest.MonkeyPatch,
    pg_url_clean: str,
    lake_anchor,  # type: ignore[no-untyped-def]
    baked_in_config: Path,
    datadraum_home: Path,
) -> Iterator[FastAPI]:
    """FastAPI app wired against the real Postgres testcontainer + tmp DATARAUM_HOME.

    Stubs only the DuckLake bootstrap (the substrate is already open via
    ``lake_anchor``); workspace bootstrap runs for real against the
    workspace Postgres.
    """
    monkeypatch.setenv("DATABASE_URL", pg_url_clean)
    monkeypatch.setenv("DATARAUM_HOME", str(datadraum_home))
    monkeypatch.setenv("DATARAUM_CONFIG_PATH", str(baked_in_config))
    monkeypatch.setenv("DUCKLAKE_CATALOG_URL", "postgresql://stub@stub/stub")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/tmp/stub-lake")
    monkeypatch.setattr("dataraum.server.app.bootstrap_lake", lambda *a, **kw: None)
    monkeypatch.setattr("dataraum.server.app.teardown_lake", lambda: None)
    monkeypatch.setattr(
        "dataraum.server.app.health_probe",
        lambda: {"status": "ok"},
    )
    monkeypatch.setattr(
        "dataraum.server.app._postgres_probe",
        lambda: {"status": "ok"},
    )

    # Fresh workspace manager each test — the deps cache holds onto a
    # stale ConnectionManager across pg_url_clean cycles otherwise.
    from dataraum.api import deps as api_deps

    api_deps.reset_workspace_manager_for_tests()

    from dataraum.server.app import app as control_plane

    yield control_plane

    api_deps.reset_workspace_manager_for_tests()


def test_bootstrap_runs_on_lifespan_and_workspace_endpoint_returns_default(
    wired_app: FastAPI,
    datadraum_home: Path,
) -> None:
    """Cold start: TestClient triggers lifespan → bootstrap → first row created."""
    with TestClient(wired_app) as client:
        response = client.get("/api/workspace")

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "default"
    assert body["workspace_id"]
    expected_config_dir = datadraum_home / "workspaces" / body["workspace_id"] / "config"
    assert Path(body["config_dir"]) == expected_config_dir
    assert expected_config_dir.is_dir()


def test_bootstrap_copies_baked_in_config_on_first_boot(
    wired_app: FastAPI,
    baked_in_config: Path,
    datadraum_home: Path,
) -> None:
    """First boot populates the overlay with everything under baked-in."""
    with TestClient(wired_app) as client:
        body = client.get("/api/workspace").json()

    overlay = Path(body["config_dir"])
    assert (overlay / "pipeline.yaml").read_text() == "phases: {}\npipeline: {}\n"
    assert (overlay / "phases" / "import.yaml").read_text() == "junk_columns: []\n"
    assert (overlay / "verticals" / "finance" / "ontology.yaml").exists()


def test_adhoc_vertical_scaffold_exists_after_bootstrap(
    wired_app: FastAPI,
) -> None:
    """Induction write target lives on the workspace overlay, not per-session."""
    with TestClient(wired_app) as client:
        body = client.get("/api/workspace").json()

    adhoc = Path(body["config_dir"]) / "verticals" / "_adhoc"
    assert adhoc.is_dir()
    assert (adhoc / "ontology.yaml").exists()
    assert (adhoc / "cycles.yaml").exists()
    assert (adhoc / "validations").is_dir()
    assert (adhoc / "metrics").is_dir()


def test_overlay_edits_survive_restart(
    wired_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
    pg_url_clean: str,
    baked_in_config: Path,
    datadraum_home: Path,
) -> None:
    """Proxy for the ticket's container-restart smoke.

    First boot creates the workspace + populates the overlay. We then
    edit a config file inside the overlay (simulating a teach write),
    re-create the FastAPI app instance (simulating restart against the
    same Postgres + mounted DATARAUM_HOME), and confirm the edit
    persists — bootstrap must NOT re-copy on top of existing state.
    """
    # First boot — create workspace, hold onto its overlay path.
    with TestClient(wired_app) as client:
        first = client.get("/api/workspace").json()
    overlay = Path(first["config_dir"])

    teach_file = overlay / "phases" / "import.yaml"
    teach_file.write_text("junk_columns:\n  - id\n# edited by teach\n")

    # Simulate restart: reset module-level singletons + workspace manager
    # cache, reimport the app module fresh. DATARAUM_HOME + DATABASE_URL
    # + DATARAUM_CONFIG_PATH stay set, so the new lifespan finds the
    # same Postgres + same overlay dir on disk.
    reset_active_workspace_for_tests()
    reset_config_root()
    from dataraum.api import deps as api_deps

    api_deps.reset_workspace_manager_for_tests()

    from dataraum.server.app import app as restarted_app

    with TestClient(restarted_app) as client:
        second = client.get("/api/workspace").json()

    assert second["workspace_id"] == first["workspace_id"], (
        "restart picked a different workspace — bootstrap re-created instead of reusing"
    )
    assert teach_file.read_text() == "junk_columns:\n  - id\n# edited by teach\n", (
        "restart re-copied the baked-in defaults over the teach edit"
    )
