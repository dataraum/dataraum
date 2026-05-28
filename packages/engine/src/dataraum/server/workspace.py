"""Workspace bootstrap — runs once at worker startup (DAT-343 simplified).

Owns two things, in order:

1. Pull the active workspace_id from ``DATARAUM_WORKSPACE_ID``. Slice 1
   has exactly one workspace per worker.
2. Stash the workspace_id on a module pointer so
   ``get_active_workspace_id`` returns it without a DB hit.

Pre-DAT-343 this also materialized a writable filesystem overlay under
``${DATARAUM_HOME}/workspaces/<id>/config/`` and scaffolded an ``_adhoc``
vertical there. The filesystem overlay is gone — teach edits now live in
the per-workspace ``config_overlay`` Postgres table and are layered onto
the baked-in YAML by :mod:`dataraum.core.overlay`. ``_adhoc``'s old role
(cold-start write target for induction agents) goes away with the
ontology-induction-in-add_source move planned for the frame stage
(``project_frame_stage_ontology`` memory).

Pivot note (DAT-339): bootstrap doesn't touch the workspace Postgres.
Schema-per-workspace makes the row-in-DB approach redundant — the
workspace_id from the env var IS the schema selector, applied at
SQLAlchemy connect time. The cockpit_db is the multi-workspace registry;
slice 1 doesn't query it.
"""

from __future__ import annotations

import re

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


def bootstrap_workspace() -> str:
    """Activate the workspace identified by ``DATARAUM_WORKSPACE_ID``.

    Refuses to start if ``DATARAUM_WORKSPACE_ID`` is unset — a misconfigured
    deployment is a footgun (every pod would think it owns a different
    workspace, or none at all).

    Returns:
        The activated workspace_id (also retrievable via
        :func:`get_active_workspace_id`).

    Raises:
        pydantic.ValidationError: via ``get_settings()`` if
            ``DATARAUM_WORKSPACE_ID`` is unset.
    """
    workspace_id = get_settings().dataraum_workspace_id

    global _active_workspace_id  # noqa: PLW0603
    _active_workspace_id = workspace_id

    logger.info("workspace_bootstrapped", workspace_id=workspace_id)

    return workspace_id
