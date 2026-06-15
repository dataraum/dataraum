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


def task_queue_for(workspace_id: str) -> str:
    """Return the Temporal task queue name for a workspace_id (DAT-505).

    One queue per workspace — ``engine-<workspace_id>`` — so an engine worker
    polls only its own workspace's work and a payload addressed to another
    workspace never reaches it. That per-workspace queue IS the isolation
    boundary that replaced the 8 copy-pasted per-activity mismatch guards: a
    misrouted payload can't land on the wrong worker because the wrong worker
    never polls its queue. The id is kept verbatim (raw UUID with dashes, or a
    sentinel like ``test``): Temporal queue names have no charset restriction,
    matching the workflow-ID convention.

    The cockpit mirrors this derivation (``engineTaskQueueFor`` in
    ``db/cockpit/registry.ts``) so its drivers route ``workflow.start`` to the
    same queue the worker polls.

    Args:
        workspace_id: The workspace identifier from ``DATARAUM_WORKSPACE_ID``.

    Returns:
        The task queue name (e.g. ``engine-00000000-0000-0000-0000-000000000001``).
    """
    return f"engine-{workspace_id}"


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

    The SINGLE workspace-isolation assertion (DAT-505): the worker's
    ``TEMPORAL_TASK_QUEUE`` must be exactly ``engine-<workspace_id>``. This one
    boot-time check replaced the 8 copy-pasted per-activity mismatch guards in
    ``worker/activity.py`` — with one queue per workspace, a payload for another
    workspace simply never reaches this worker, so the per-activity defence is
    redundant. What CAN still go wrong is a misconfigured container (a queue env
    that doesn't match its workspace env), and that is exactly what this fails
    loud on, before the worker advertises itself as polling.

    Returns:
        The activated workspace_id (also retrievable via
        :func:`get_active_workspace_id`).

    Raises:
        pydantic.ValidationError: via ``get_settings()`` if
            ``DATARAUM_WORKSPACE_ID`` is unset.
        RuntimeError: if ``TEMPORAL_TASK_QUEUE`` is not ``engine-<workspace_id>``
            — a container wired to poll a queue that does not belong to its
            workspace.
    """
    settings = get_settings()
    workspace_id = settings.dataraum_workspace_id

    expected_queue = task_queue_for(workspace_id)
    if settings.temporal_task_queue != expected_queue:
        raise RuntimeError(
            f"Workspace/queue mismatch: TEMPORAL_TASK_QUEUE is "
            f"{settings.temporal_task_queue!r} but workspace "
            f"{workspace_id!r} must poll {expected_queue!r}. Each engine "
            "container polls exactly its own workspace's queue (DAT-505) — fix "
            "the container env so the two agree."
        )

    global _active_workspace_id  # noqa: PLW0603
    _active_workspace_id = workspace_id

    logger.info("workspace_bootstrapped", workspace_id=workspace_id, task_queue=expected_queue)

    return workspace_id
