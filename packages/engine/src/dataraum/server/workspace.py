"""Workspace bootstrap — runs once at FastAPI startup.

Owns three things, in order:

1. Pick the active ``Workspace`` row (first existing, else create
   ``name="default"``). Slice 1 has exactly one workspace per server.
2. Materialize the writable config overlay at
   ``${DATARAUM_HOME}/workspaces/<workspace_id>/config/`` by copying
   the read-only baked-in defaults on first boot. Existing dirs are left
   alone — teach edits already there must survive container restarts.
3. Register the workspace's ``config_dir`` as the active config root via
   ``set_active_workspace_config_dir`` so every subsequent
   ``load_yaml_config`` / ``load_phase_config`` / teach write resolves
   there.

The ``_adhoc`` vertical scaffold (cold-start write target for induction
agents) lives under the workspace overlay too — created here, once per
workspace, instead of on every pipeline setup. See
``dataraum.pipeline.setup`` for the pre-DAT-358 per-session home.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session as SASession

from dataraum.core.config import _get_config_root, set_active_workspace_config_dir
from dataraum.core.logging import get_logger
from dataraum.storage import Workspace

logger = get_logger(__name__)


SessionFactory = Callable[[], AbstractContextManager[SASession]]


def bootstrap_workspace(session_factory: SessionFactory) -> Workspace:
    """Pick or create the active workspace and activate its config overlay.

    Refuses to start if ``DATARAUM_HOME`` is unset — the container
    image sets it and a misconfigured deployment is a footgun (workspace
    state would land in an ephemeral cwd-relative directory).

    Args:
        session_factory: Zero-arg callable returning a context-managed
            SQLAlchemy session bound to the workspace Postgres engine.
            Production passes ``workspace_manager.session_scope``.

    Returns:
        The active ``Workspace`` row. Detached from any session — the
        function commits and re-reads the row to avoid handing back a
        bound object whose session has closed.

    Raises:
        RuntimeError: If ``DATARAUM_HOME`` is unset.
    """
    home_env = os.environ.get("DATARAUM_HOME")
    if not home_env:
        raise RuntimeError(
            "DATARAUM_HOME is not set. The container image sets it to "
            "/var/lib/dataraum/workspace; for local dev outside the "
            "container, export DATARAUM_HOME=<absolute path> to a "
            "writable directory."
        )
    home_dir = Path(home_env)

    # The bootstrap copy source MUST be resolved before we set the
    # active-workspace pointer — otherwise ``_get_config_root()`` returns
    # the (empty) workspace overlay and the copy is a no-op against
    # itself. After this line, env var or auto-detect wins.
    baked_in_config = _get_config_root()

    workspace_id, config_dir = _pick_or_create_workspace(session_factory, home_dir)

    _ensure_config_dir_populated(config_dir, baked_in_config)
    _ensure_adhoc_vertical(config_dir)

    set_active_workspace_config_dir(config_dir)

    logger.info(
        "workspace_bootstrapped",
        workspace_id=workspace_id,
        config_dir=str(config_dir),
        baked_in_config=str(baked_in_config),
    )

    # Re-fetch a detached snapshot so callers can read fields without a
    # session attached. The lifespan caller doesn't need the row but
    # tests use it to assert shape.
    with session_factory() as session:
        ws = session.get(Workspace, workspace_id)
        if ws is None:  # defensive — bootstrap just wrote this row
            raise RuntimeError(f"Workspace {workspace_id} vanished after bootstrap")
        session.expunge(ws)
        return ws


def _pick_or_create_workspace(
    session_factory: SessionFactory, home_dir: Path
) -> tuple[str, Path]:
    """Return ``(workspace_id, config_dir)`` for the active workspace.

    Picks the lowest-``created_at`` existing row (deterministic) or
    creates ``name="default"`` if none exist. ``config_dir`` is computed
    from ``workspace_id`` — generated upfront so we can persist the path
    on the row in a single INSERT.
    """
    from uuid import uuid4

    with session_factory() as session:
        existing = session.execute(
            select(Workspace).order_by(Workspace.created_at).limit(1)
        ).scalar_one_or_none()
        if existing is not None:
            return existing.workspace_id, Path(existing.config_dir)

        new_id = str(uuid4())
        new_config_dir = home_dir / "workspaces" / new_id / "config"
        ws = Workspace(
            workspace_id=new_id,
            name="default",
            config_dir=str(new_config_dir),
        )
        session.add(ws)
        # session_scope commits on exit
        return new_id, new_config_dir


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
