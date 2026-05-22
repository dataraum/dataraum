"""Lane smoke for DAT-358 — Engine workspace foundation.

Scope: verify the FastAPI lifespan bootstraps the active workspace from
``DATARAUM_WORKSPACE_ID``, materializes the overlay, and sets the
active-workspace pointer.

After the DAT-339 pivot the workspace is identified by an env var rather
than a Postgres row, and the cockpit no longer reads workspace metadata
through ``/api/workspace`` — the route was deleted in A2 (pulled forward
from Phase 0c). The remaining smoke contract:

* FastAPI startup runs ``bootstrap_workspace`` against the resolved
  ``${DATARAUM_HOME}/workspaces/<id>/config/`` and populates it on first
  boot by copying the read-only baked-in defaults.
* The ``_adhoc`` vertical scaffold lands under the overlay (cold-start
  induction has its write target).
* Overlay edits survive a "restart" (re-running the lifespan against the
  same DATARAUM_HOME mount) — proxy for the ticket's "edit a config
  file inside the container, restart, persists" acceptance check.
* ``get_active_workspace_id()`` returns the env-var value after
  bootstrap, raises before it.

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
from dataraum.server.workspace import (
    get_active_workspace_id,
    reset_active_workspace_id_for_tests,
)

_FIXED_WS_ID = "00000000-0000-0000-0000-0000000000bb"


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
    """Reset the module-level workspace pointers between tests."""
    yield
    reset_active_workspace_for_tests()
    reset_active_workspace_id_for_tests()
    reset_config_root()


@pytest.fixture
def wired_app(
    monkeypatch: pytest.MonkeyPatch,
    lake_anchor,  # type: ignore[no-untyped-def]
    baked_in_config: Path,
    datadraum_home: Path,
) -> Iterator[FastAPI]:
    """FastAPI app wired against tmp DATARAUM_HOME + a fixed workspace id.

    Stubs the DuckLake bootstrap (the substrate is already open via
    ``lake_anchor``) and probes; workspace bootstrap runs for real against
    the env vars.
    """
    monkeypatch.setenv("DATARAUM_HOME", str(datadraum_home))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)
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

    from dataraum.server.app import app as control_plane

    yield control_plane


def _expected_overlay(home: Path, workspace_id: str) -> Path:
    return home / "workspaces" / workspace_id / "config"


def test_bootstrap_runs_on_lifespan_and_activates_env_var_workspace(
    wired_app: FastAPI,
    datadraum_home: Path,
) -> None:
    """Cold start: TestClient triggers lifespan → bootstrap → pointer set."""
    with TestClient(wired_app):
        active = get_active_workspace_id()

    assert active == _FIXED_WS_ID
    overlay = _expected_overlay(datadraum_home, _FIXED_WS_ID)
    assert overlay.is_dir()


def test_bootstrap_copies_baked_in_config_on_first_boot(
    wired_app: FastAPI,
    baked_in_config: Path,
    datadraum_home: Path,
) -> None:
    """First boot populates the overlay with everything under baked-in."""
    overlay = _expected_overlay(datadraum_home, _FIXED_WS_ID)
    with TestClient(wired_app):
        pass

    assert (overlay / "pipeline.yaml").read_text() == "phases: {}\npipeline: {}\n"
    assert (overlay / "phases" / "import.yaml").read_text() == "junk_columns: []\n"
    assert (overlay / "verticals" / "finance" / "ontology.yaml").exists()


def test_adhoc_vertical_scaffold_exists_after_bootstrap(
    wired_app: FastAPI,
    datadraum_home: Path,
) -> None:
    """Induction write target lives on the workspace overlay, not per-session."""
    overlay = _expected_overlay(datadraum_home, _FIXED_WS_ID)
    with TestClient(wired_app):
        pass

    adhoc = overlay / "verticals" / "_adhoc"
    assert adhoc.is_dir()
    assert (adhoc / "ontology.yaml").exists()
    assert (adhoc / "cycles.yaml").exists()
    assert (adhoc / "validations").is_dir()
    assert (adhoc / "metrics").is_dir()


def test_overlay_edits_survive_restart(
    wired_app: FastAPI,
    datadraum_home: Path,
) -> None:
    """Proxy for the ticket's container-restart smoke.

    First boot populates the overlay. We then edit a config file inside
    the overlay (simulating a teach write), re-create the FastAPI app
    instance (simulating restart against the same DATARAUM_HOME mount),
    and confirm the edit persists — bootstrap must NOT re-copy on top
    of existing state.
    """
    overlay = _expected_overlay(datadraum_home, _FIXED_WS_ID)
    with TestClient(wired_app):
        pass

    teach_file = overlay / "phases" / "import.yaml"
    teach_file.write_text("junk_columns:\n  - id\n# edited by teach\n")

    # Simulate restart: reset module-level singletons and re-enter the
    # lifespan. The re-import below returns the cached module object —
    # `restarted_app is wired_app` — but a second `TestClient` context
    # runs the lifespan again, which is what the real restart contract
    # exercises (idempotent overlay populate, no re-copy over teach edits).
    # DATARAUM_HOME + DATARAUM_WORKSPACE_ID + DATARAUM_CONFIG_PATH stay
    # set so the new lifespan finds the same overlay dir on disk.
    reset_active_workspace_for_tests()
    reset_active_workspace_id_for_tests()
    reset_config_root()

    from dataraum.server.app import app as restarted_app

    with TestClient(restarted_app):
        active = get_active_workspace_id()

    assert active == _FIXED_WS_ID
    assert teach_file.read_text() == "junk_columns:\n  - id\n# edited by teach\n", (
        "restart re-copied the baked-in defaults over the teach edit"
    )
