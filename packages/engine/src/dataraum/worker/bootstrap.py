"""Temporal activity-worker substrate bootstrap (DAT-344).

The engine is a pure Python Temporal *activity* worker (no Starlette shell).
This module hoists the substrate bootstrap that used to live in the
``server/app.py`` lifespan (deleted in E4a/P4): open the DuckLake anchor +
the workspace config overlay, then build ONE workspace-level
:class:`ConnectionManager` with DuckDB open.

The worker holds that single manager for its whole life. Each activity leases
a *scoped* SQLAlchemy session + DuckDB cursor from it (see
:mod:`dataraum.worker.activity`) — connection wiring lives in one place, and
the DuckLake ``:memory:`` anchor + manager survive across every activity
invocation because the lake USE scope is workspace-stable (post-DAT-341).
"""

from __future__ import annotations

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.core.logging import get_logger
from dataraum.core.settings import get_settings
from dataraum.server.overlay_resolver import (
    install_overlay_resolver,
    uninstall_overlay_resolver,
)
from dataraum.server.storage import bootstrap_lake, teardown_lake
from dataraum.server.workspace import bootstrap_workspace

logger = get_logger(__name__)


def bootstrap_worker_substrate() -> ConnectionManager:
    """Open the engine substrate and return the worker's ConnectionManager.

    Calls ``get_settings()`` first, which validates the full env contract
    (DB / DuckLake / workspace / LLM) and raises a ``pydantic.ValidationError``
    naming any missing var — the single boot-time gate, same one the deleted
    Starlette lifespan used.

    Order matters: the DuckLake anchor must exist before the manager opens its
    DuckDB connection, and the workspace pointer must be set before the manager
    initializes its ``ws_<id>`` schema.
    """
    settings = get_settings()
    bootstrap_lake(settings.ducklake_catalog_url, settings.ducklake_data_path)

    # bootstrap_lake set the process-wide DuckLake anchor. If anything after it
    # fails, release the anchor (and any partially-opened manager) so a
    # partial-init boot doesn't leak the Postgres-backed catalog connection —
    # the caller's try/finally only runs once a manager is returned.
    manager: ConnectionManager | None = None
    try:
        bootstrap_workspace()
        manager = ConnectionManager(ConnectionConfig.for_workspace())
        manager.initialize()
        # Workspace-level DuckDB: one connection for the worker's whole life. No
        # session binding — activities carry their own run ref as data.
        manager.open_lake()
        # DAT-343: layered config reads via Postgres-backed overlay resolver.
        # Must come after the manager + workspace pointer are up — the resolver
        # needs both. After this, every load_yaml_config call merges the
        # workspace's active config_overlay rows over the baked-in YAML.
        install_overlay_resolver(manager)
    except Exception:
        if manager is not None:
            manager.close()
        teardown_lake()
        raise

    logger.info("worker_substrate_bootstrapped")
    return manager


def shutdown_worker_substrate(manager: ConnectionManager) -> None:
    """Tear down the worker substrate. Safe to call on partial init."""
    uninstall_overlay_resolver()
    try:
        manager.close()
    finally:
        teardown_lake()
    logger.info("worker_substrate_shutdown")
