"""Workspace bootstrap — runs once at FastAPI startup.

Owns three things, in order:

1. Pull the active workspace_id from ``DATARAUM_WORKSPACE_ID``. Slice 1
   has exactly one workspace per server.
2. Materialize the writable config overlay at
   ``${DATARAUM_HOME}/workspaces/<workspace_id>/config/`` by copying the
   read-only baked-in defaults on first boot. Existing dirs are left
   alone — teach edits already there must survive container restarts.
3. Register the workspace's ``config_dir`` as the active config root via
   ``set_active_workspace_config_dir`` so every subsequent
   ``load_yaml_config`` / ``load_phase_config`` / teach write resolves
   there, and stash the workspace_id on a module pointer so
   ``get_active_workspace_id`` returns it without a DB hit.

The ``_adhoc`` vertical scaffold (cold-start write target for induction
agents) lives under the workspace overlay too — created here, once per
workspace, instead of on every pipeline setup. See
``dataraum.pipeline.setup`` for the pre-DAT-358 per-session home.

Pivot note (DAT-339): bootstrap no longer touches the workspace
Postgres. Schema-per-workspace makes the row-in-DB approach redundant
— the workspace_id from the env var IS the schema selector, applied at
SQLAlchemy connect time. The cockpit_db is the multi-workspace registry;
slice 1 doesn't query it.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import yaml

from dataraum.core.config import _get_config_root, set_active_workspace_config_dir
from dataraum.core.logging import get_logger
from dataraum.core.settings import get_settings

logger = get_logger(__name__)


_active_workspace_id: str | None = None


_SCHEMA_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def schema_name_for(workspace_id: str) -> str:
    """Return the Postgres schema name for a workspace_id.

    Slice 1 format (locked by /refine 2026-05-22): ``ws_<uuid-with-underscores>``.
    Dashes in the workspace_id are translated to underscores so the result
    is a valid unquoted Postgres identifier.

    Args:
        workspace_id: The workspace identifier from ``DATARAUM_WORKSPACE_ID``.

    Returns:
        The schema name (e.g. ``ws_00000000_0000_0000_0000_0000000000aa``).

    Raises:
        ValueError: If the resulting schema name isn't a valid Postgres
            identifier (would be too short, start with a digit, or contain
            characters that aren't ``[A-Za-z0-9_]``). UUIDs and short ids
            like ``test`` pass; arbitrary user input from a misconfigured
            env var fails loudly here rather than via a confusing
            SQLAlchemy error later.
    """
    candidate = "ws_" + workspace_id.replace("-", "_")
    if len(candidate) > 63:  # Postgres identifier length limit.
        raise ValueError(
            f"workspace_id {workspace_id!r} produces schema name {candidate!r} "
            f"({len(candidate)} chars); Postgres identifiers max out at 63."
        )
    if not _SCHEMA_NAME_PATTERN.match(candidate):
        raise ValueError(
            f"workspace_id {workspace_id!r} produces schema name {candidate!r}, "
            "which is not a valid unquoted Postgres identifier. Use a UUID "
            "or an identifier matching [A-Za-z_][A-Za-z0-9_-]*."
        )
    return candidate


@dataclass(frozen=True)
class BootstrappedWorkspace:
    """Return shape of :func:`bootstrap_workspace`.

    Slice-1 minimum: just enough for the lifespan log + tests. No
    SQLAlchemy row, no DB-bound state.
    """

    workspace_id: str
    config_dir: Path


def get_active_workspace_id() -> str:
    """Return the active workspace_id set by :func:`bootstrap_workspace`.

    Reads from a module-level pointer that mirrors
    ``DATARAUM_WORKSPACE_ID`` after bootstrap has run. Production
    construction sites (loaders, phases, fix interpreters) call this
    when they need the workspace_id without a DB hit.

    Returns:
        The active workspace's id.

    Raises:
        RuntimeError: If bootstrap_workspace has not run yet (the
            lifespan never executed, or test code reset the pointer
            without rebootstrapping).
    """
    if _active_workspace_id is None:
        raise RuntimeError(
            "No active workspace. bootstrap_workspace must run at server "
            "startup before pipeline/loader code reads the active workspace_id."
        )
    return _active_workspace_id


def reset_active_workspace_id_for_tests() -> None:
    """Clear the module-level workspace_id pointer. Tests only."""
    global _active_workspace_id  # noqa: PLW0603
    _active_workspace_id = None


def bootstrap_workspace() -> BootstrappedWorkspace:
    """Activate the workspace identified by ``DATARAUM_WORKSPACE_ID``.

    Refuses to start if either ``DATARAUM_HOME`` or
    ``DATARAUM_WORKSPACE_ID`` is unset — both are container substrate
    concerns, and a misconfigured deployment is a footgun (workspace
    state would land in an ephemeral cwd-relative directory, or every
    pod would think it owns a different workspace).

    Returns:
        :class:`BootstrappedWorkspace` with the resolved workspace_id
        and the (now-populated) config_dir.

    Raises:
        pydantic.ValidationError: via ``get_settings()`` if ``DATARAUM_HOME``
            or ``DATARAUM_WORKSPACE_ID`` is unset.
    """
    settings = get_settings()
    home_dir = settings.dataraum_home
    workspace_id = settings.dataraum_workspace_id
    config_dir = home_dir / "workspaces" / workspace_id / "config"

    # The bootstrap copy source MUST be resolved before we set the
    # active-workspace pointer — otherwise ``_get_config_root()`` returns
    # the (empty) workspace overlay and the copy is a no-op against
    # itself. After this line, env var or auto-detect wins.
    baked_in_config = _get_config_root()

    _ensure_config_dir_populated(config_dir, baked_in_config)
    _ensure_adhoc_vertical(config_dir)

    set_active_workspace_config_dir(config_dir)

    global _active_workspace_id  # noqa: PLW0603
    _active_workspace_id = workspace_id

    logger.info(
        "workspace_bootstrapped",
        workspace_id=workspace_id,
        config_dir=str(config_dir),
        baked_in_config=str(baked_in_config),
    )

    return BootstrappedWorkspace(workspace_id=workspace_id, config_dir=config_dir)


def _ensure_config_dir_populated(config_dir: Path, source: Path) -> None:
    """First-boot copy of the baked-in config into the workspace overlay.

    A pre-existing ``config_dir`` is left untouched — teach edits already
    on the mounted volume survive container restarts. Missing parent
    dirs are created on first boot.
    """
    if config_dir.exists() and any(config_dir.iterdir()):
        logger.debug("workspace_config_dir_reused", path=str(config_dir))
        return

    config_dir.parent.mkdir(parents=True, exist_ok=True)
    if config_dir.exists():
        # exists but empty — copytree refuses an existing destination
        config_dir.rmdir()
    shutil.copytree(source, config_dir)
    logger.info("workspace_config_dir_populated", source=str(source), destination=str(config_dir))


def _ensure_adhoc_vertical(config_dir: Path) -> None:
    """Create the ``_adhoc`` vertical scaffold for cold-start sessions.

    The ``_adhoc`` vertical is the write target for induction agents
    (ontology, cycles, validations) on cold start and for teach
    refinements later. Idempotent — exits early if the directory exists.
    Pre-DAT-358 this ran on every pipeline setup against a per-session
    config copy; it now lives on the workspace overlay and runs once
    per workspace.
    """
    adhoc_dir = config_dir / "verticals" / "_adhoc"
    if adhoc_dir.exists():
        return

    adhoc_dir.mkdir(parents=True)
    with open(adhoc_dir / "ontology.yaml", "w") as f:
        yaml.dump(
            {
                "name": "_adhoc",
                "version": "1.0.0",
                "description": "Auto-generated",
                "concepts": [],
            },
            f,
            default_flow_style=False,
            sort_keys=False,
        )
    with open(adhoc_dir / "cycles.yaml", "w") as f:
        yaml.dump({"cycle_types": {}}, f, default_flow_style=False, sort_keys=False)
    (adhoc_dir / "validations").mkdir()
    (adhoc_dir / "metrics").mkdir()
    logger.debug("adhoc_vertical_created", path=str(adhoc_dir))
