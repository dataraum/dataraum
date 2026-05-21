"""Lane smoke for DAT-324 (L5 of the DAT-294 Minimum-Port Plan).

Contract surface this smoke covers:

1. ``SOURCES_DIR`` / ``CONFIG_DIR`` constants are the container-absolute
   paths the Confluence Rev 3 conventions specify, and they match the
   ``docker-compose.yml`` bind-mount target + ``Dockerfile`` COPY target.
2. ``DATARAUM_CONFIG_PATH`` env var routes ``core.config`` to the
   specified directory end-to-end (the wiring compose relies on).
3. ``_add_source`` picks up a recipe yaml dropped under
   ``SOURCES_DIR`` (monkeypatched to a tmp dir).
4. Grep audit on ``src/`` minus ``mcp/`` returns zero hits for
   ``~/.dataraum``, ``DATARAUM_HOME``, ``credentials.yaml``,
   ``FileProvider``, ``credentials_dir``, ``credentials_file``.

Integration smoke (compose-up + HTTP MCP + add_source round-trip)
belongs to ``main``, not this lane — see AC7 in DAT-324.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

import dataraum.core.paths as paths_mod
from dataraum.core import config as core_config
from dataraum.core.paths import SOURCES_DIR

_REPO_ROOT = Path(__file__).resolve().parents[2]
# Sibling package in the monorepo — docker-compose moved out of the engine
# package in the v1 restructure (PR #123). Engine-internal anchors
# (Dockerfile, src/) still resolve relative to _REPO_ROOT.
_INFRA_DIR = _REPO_ROOT.parent / "infra"


# === (1) Path constants vs infra files ===


def test_sources_dir_matches_docker_compose_bind_mount() -> None:
    compose = (_INFRA_DIR / "docker-compose.yml").read_text()
    # `${HOST_SOURCES_DIR:-./sources}:/var/lib/dataraum/sources:ro`
    assert re.search(
        r"\$\{HOST_SOURCES_DIR.*?\}:" + re.escape(str(SOURCES_DIR)) + r"(?::ro)?\b",
        compose,
    ), f"docker-compose.yml bind-mount target must equal SOURCES_DIR={SOURCES_DIR}"


def test_config_dir_matches_dockerfile_copy_target() -> None:
    dockerfile = (_REPO_ROOT / "docker" / "control-plane.Dockerfile").read_text()
    assert f"COPY config/ {paths_mod.CONFIG_DIR}/" in dockerfile, (
        f"control-plane.Dockerfile must COPY config/ into {paths_mod.CONFIG_DIR}/"
    )
    assert f"ENV DATARAUM_CONFIG_PATH={paths_mod.CONFIG_DIR}" in dockerfile, (
        f"control-plane.Dockerfile must set DATARAUM_CONFIG_PATH={paths_mod.CONFIG_DIR}"
    )


# === (2) DATARAUM_CONFIG_PATH env var end-to-end ===


def test_datadraum_config_path_env_var_routes_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulate the container env: set DATARAUM_CONFIG_PATH, drop a
    known yaml, assert load_yaml_config reads from there."""
    fake_config = tmp_path / "config"
    fake_config.mkdir()
    (fake_config / "marker.yaml").write_text(yaml.dump({"lane": "DAT-324"}))

    monkeypatch.setenv("DATARAUM_CONFIG_PATH", str(fake_config))
    core_config.reset_config_root()
    try:
        data = core_config.load_yaml_config("marker.yaml")
    finally:
        core_config.reset_config_root()

    assert data == {"lane": "DAT-324"}


# === (3) add_source against monkeypatched SOURCES_DIR ===


def test_add_source_picks_up_recipe_from_sources_dir(
    session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end through _add_source: yaml in SOURCES_DIR resolves
    via bare-name lookup, no DATARAUM_HOME needed."""
    from dataraum.mcp.server import _add_source

    recipe = "backend: mssql\ntables:\n  t:\n    sql: SELECT 1\n"
    (tmp_path / "warehouse.yaml").write_text(recipe)
    monkeypatch.setattr(paths_mod, "SOURCES_DIR", tmp_path)

    result = _add_source(session, {"name": "warehouse", "path": "warehouse"})

    assert "error" not in result, result.get("error")
    assert result["source"]["type"] == "db_recipe"
    assert result["source"]["backend"] == "mssql"


# === (4) Grep audit (acceptance criteria AC4 + AC5) ===


def test_no_home_dataraum_references_outside_mcp() -> None:
    """`grep -rn '~/.dataraum\\|DATARAUM_HOME\\|...' src/` returns zero outside the allowlist.

    Allowlist:
        * ``src/dataraum/mcp/`` — legacy session manager, still uses
          DATARAUM_HOME for session dirs.
        * ``src/dataraum/server/workspace.py`` — bootstrap_workspace
          (DAT-358) reads DATARAUM_HOME as the writable overlay root.
        * ``src/dataraum/storage/workspace_models.py`` — model docstring
          mentions DATARAUM_HOME for the same reason.
    """
    cmd = [
        "grep",
        "-rn",
        "-E",
        r"~/\.dataraum|DATARAUM_HOME|credentials\.yaml|FileProvider|credentials_dir|credentials_file",
        str(_REPO_ROOT / "src"),
        "--include=*.py",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    allowed_prefixes = (
        "/src/dataraum/mcp/",
        "/src/dataraum/server/workspace.py",
        "/src/dataraum/storage/workspace_models.py",
        # Docstrings in core/config.py describe the workspace-overlay
        # resolution priority (DAT-358) — documentation only, no FS
        # access to ~/.dataraum.
        "/src/dataraum/core/config.py",
    )
    hits = [
        line
        for line in proc.stdout.splitlines()
        if line and not any(prefix in line for prefix in allowed_prefixes)
    ]
    assert hits == [], "Expected zero hits in src/ outside allowlist, got:\n  " + "\n  ".join(hits)
