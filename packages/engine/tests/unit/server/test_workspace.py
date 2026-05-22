"""Tests for ``dataraum.server.workspace.bootstrap_workspace``.

Env-var-driven bootstrap (post-DAT-339 pivot): no SQLAlchemy session,
no Postgres dependency. The contract is:

* ``DATARAUM_HOME`` + ``DATARAUM_WORKSPACE_ID`` together resolve the
  workspace's overlay path under ``$HOME/workspaces/<id>/config``.
* First boot copies the baked-in config into that path.
* Subsequent boots reuse the directory and don't overwrite teach edits.
* The ``_adhoc`` vertical scaffold lands under the overlay, once.
* The module-level ``_active_workspace_id`` pointer is set so
  ``get_active_workspace_id()`` returns without raising.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from dataraum.core.config import (
    _get_config_root,
    reset_active_workspace_for_tests,
    reset_config_root,
    set_config_root,
)
from dataraum.server.workspace import (
    bootstrap_workspace,
    get_active_workspace_id,
    reset_active_workspace_id_for_tests,
)


@pytest.fixture
def baked_in_config(tmp_path: Path) -> Path:
    """A minimal baked-in config tree the bootstrap copies from."""
    src = tmp_path / "baked_in_config"
    (src / "phases").mkdir(parents=True)
    (src / "phases" / "import.yaml").write_text("junk_columns: []\n")
    (src / "pipeline.yaml").write_text("phases: {}\npipeline: {}\n")
    (src / "verticals" / "finance").mkdir(parents=True)
    (src / "verticals" / "finance" / "ontology.yaml").write_text("concepts: []\n")
    return src


@pytest.fixture
def home_dir(tmp_path: Path) -> Path:
    home = tmp_path / "datahome"
    home.mkdir()
    return home


@pytest.fixture(autouse=True)
def _isolate_active_workspace() -> Iterator[None]:
    """Reset the module-level pointers between tests."""
    yield
    reset_active_workspace_for_tests()
    reset_active_workspace_id_for_tests()
    reset_config_root()


@pytest.fixture
def pointed_at_baked_in(
    baked_in_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """Make ``_get_config_root()`` return the baked-in fixture tree.

    Uses ``set_config_root`` (the top-priority override) rather than
    ``DATARAUM_CONFIG_PATH`` so the test is robust against the env var
    being unset/inherited from the harness.
    """
    set_config_root(baked_in_config)
    yield baked_in_config


_FIXED_WS_ID = "00000000-0000-0000-0000-0000000000aa"


def test_bootstrap_uses_workspace_id_from_env_var(
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    ws = bootstrap_workspace()

    assert ws.workspace_id == _FIXED_WS_ID
    expected_config_dir = home_dir / "workspaces" / _FIXED_WS_ID / "config"
    assert ws.config_dir == expected_config_dir
    assert expected_config_dir.is_dir()


def test_bootstrap_sets_module_pointer_for_get_active_workspace_id(
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    bootstrap_workspace()

    assert get_active_workspace_id() == _FIXED_WS_ID


def test_get_active_workspace_id_raises_before_bootstrap() -> None:
    # autouse fixture has already reset the pointer; calling without a
    # bootstrap is the precondition.
    with pytest.raises(RuntimeError, match="No active workspace"):
        get_active_workspace_id()


def test_bootstrap_copies_baked_in_config_on_first_boot(
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    ws = bootstrap_workspace()

    overlay = ws.config_dir
    assert (overlay / "pipeline.yaml").read_text() == "phases: {}\npipeline: {}\n"
    assert (overlay / "phases" / "import.yaml").read_text() == "junk_columns: []\n"
    assert (overlay / "verticals" / "finance" / "ontology.yaml").exists()


def test_bootstrap_activates_workspace_as_config_root(
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    ws = bootstrap_workspace()

    # The set_config_root() override would still win; drop it so we can
    # observe the active-workspace step.
    reset_config_root()
    assert _get_config_root() == ws.config_dir


def test_bootstrap_creates_adhoc_vertical_scaffold(
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    ws = bootstrap_workspace()

    adhoc = ws.config_dir / "verticals" / "_adhoc"
    assert adhoc.is_dir()
    assert (adhoc / "ontology.yaml").exists()
    assert (adhoc / "cycles.yaml").exists()
    assert (adhoc / "validations").is_dir()
    assert (adhoc / "metrics").is_dir()


def test_bootstrap_reuses_existing_overlay_and_does_not_overwrite(
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Subsequent boots must not stomp teach edits already on disk."""
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    first = bootstrap_workspace()
    teach_edit = first.config_dir / "phases" / "import.yaml"
    teach_edit.write_text("junk_columns:\n  - id\n# edited by teach\n")
    reset_active_workspace_for_tests()
    reset_active_workspace_id_for_tests()

    second = bootstrap_workspace()

    assert second.workspace_id == first.workspace_id
    assert second.config_dir == first.config_dir
    assert teach_edit.read_text() == "junk_columns:\n  - id\n# edited by teach\n", (
        "second boot overwrote teach edits"
    )


def test_bootstrap_raises_when_datatraum_home_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATARAUM_HOME", raising=False)
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    with pytest.raises(RuntimeError, match="DATARAUM_HOME is not set"):
        bootstrap_workspace()


def test_bootstrap_raises_when_workspace_id_unset(
    home_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.delenv("DATARAUM_WORKSPACE_ID", raising=False)

    with pytest.raises(RuntimeError, match="DATARAUM_WORKSPACE_ID is not set"):
        bootstrap_workspace()


def test_bootstrap_adhoc_scaffold_is_idempotent(
    home_dir: Path,
    pointed_at_baked_in: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATARAUM_HOME", str(home_dir))
    monkeypatch.setenv("DATARAUM_WORKSPACE_ID", _FIXED_WS_ID)

    bootstrap_workspace()
    reset_active_workspace_for_tests()
    reset_active_workspace_id_for_tests()

    # second call should not raise even though _adhoc already exists
    bootstrap_workspace()
