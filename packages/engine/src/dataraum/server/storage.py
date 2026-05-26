"""DuckLake bootstrap and shared in-memory anchor for the FastAPI process.

DuckLake stores data as parquet files on a DATA_PATH backend (local-FS in the
spine; rustfs/S3 deferred to post-spine) with metadata in a Postgres catalog
database. DuckDB clients access DuckLake by ATTACHing the catalog as an
external database.

Connection model (post-DAT-323):

* One named in-memory DuckDB database, `:memory:dataraum_lake`, lives for the
  lifetime of the FastAPI process. We open one *anchor* connection at startup
  that is never used for queries — its sole purpose is to keep the named
  database alive (DuckDB tears down a named in-memory database once the last
  connection to it closes).
* Per-session ``ConnectionManager`` instances obtain their own fresh DuckDB
  connection to the same named database via :func:`connect_session`. Catalog
  state (the DuckLake ATTACH) is shared across connections to the same named
  in-memory database; connection-state (``USE``/search_path, prepared
  statements, transaction state) is per-connection. This gives per-session
  schema isolation without re-paying ATTACH cost.

Tests bootstrap the anchor against a testcontainer Postgres + a tmp_path
DATA_PATH; the FastAPI app does the same against the compose stack.
"""

from __future__ import annotations

import threading
from urllib.parse import unquote, urlparse

import duckdb

from dataraum.core.logging import get_logger
from dataraum.core.settings import get_settings

logger = get_logger(__name__)


LAKE_DB_NAME = ":memory:dataraum_lake"
"""Named in-memory DuckDB used as the process-wide shared catalog.

All connections opened with ``duckdb.connect(LAKE_DB_NAME)`` share catalog
state (schemas, ATTACHed DBs, tables). The anchor keeps the database alive.
"""

LAKE_CATALOG_ALIAS = "lake"
"""Alias under which the DuckLake catalog is ATTACHed.

Workspace-stable layer schemas (post-DAT-341):

* ``lake.raw`` — VARCHAR-first staging tables (one row per source/table)
* ``lake.typed`` — type-resolved tables (the default ``USE`` target)
* ``lake.quarantine`` — failed type-cast rows

These three schemas survive across all sessions for a given workspace and
are created at :func:`bootstrap_lake` time.

Reserved namespace (do not create dynamically as a workspace schema):

* ``session_*`` — reserved for slice 2 (DAT-356) per-session overlay schemas
* ``archive_*`` — reserved for slice 2 archived-session schemas

Per-workspace catalog migration: ``LAKE_CATALOG_ALIAS`` is the single point
where the alias is encoded. If slice 2+ moves to per-workspace ATTACH
aliases (e.g. ``lake_<workspace_id>``), only the ATTACH and consumers that
build FQNs from this constant need to change — the layer schemas themselves
stay workspace-stable.
"""

LAKE_LAYER_SCHEMAS: tuple[str, ...] = ("raw", "typed", "quarantine")
"""Workspace-stable layer schemas created at bootstrap.

Kept in sync with :mod:`dataraum.core.duckdb_naming`'s ``_LAYER_SCHEMA``
mapping; any new layer that earns its own schema must be added to both.
"""

# NOTE on flushes: DuckLake buffers writes in memory until ``CHECKPOINT``.
# ``INSERT`` against a ``lake.*`` table does not appear under ``DATA_PATH``
# on disk until a checkpoint runs (manual ``CHECKPOINT``, connection close
# in some configurations, or the periodic checkpointer DuckDB may run).
# Callers that need files-on-disk semantics (export, hand-off, snapshot)
# must issue ``CHECKPOINT`` explicitly.


_anchor: duckdb.DuckDBPyConnection | None = None
_bootstrap_lock = threading.Lock()


def _pg_url_to_libpq(url: str) -> str:
    """Convert a ``postgresql://user:pass@host:port/db`` URL to libpq KV form.

    DuckLake's ATTACH string expects libpq keyword-value syntax
    (``dbname=... host=... user=... password=... port=...``), not the
    URL form.
    """
    p = urlparse(url)
    parts: list[str] = []
    if p.path and p.path != "/":
        parts.append(f"dbname={unquote(p.path.lstrip('/'))}")
    if p.hostname:
        parts.append(f"host={p.hostname}")
    if p.port:
        parts.append(f"port={p.port}")
    if p.username:
        parts.append(f"user={unquote(p.username)}")
    if p.password:
        # urlparse leaves percent-encoding in place; libpq wants the decoded
        # value. Then single-quote+escape if the decoded value contains
        # whitespace or quote characters; alphanumeric passes through bare.
        decoded = unquote(p.password)
        if any(c.isspace() or c in ("'", "\\") for c in decoded):
            escaped = decoded.replace("\\", "\\\\").replace("'", "\\'")
            parts.append(f"password='{escaped}'")
        else:
            parts.append(f"password={decoded}")
    return " ".join(parts)


def _escape_sql_literal(value: str) -> str:
    r"""Backslash-escape ``\`` and ``'`` for safe single-quoted SQL interpolation."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def bootstrap_lake(catalog_url: str, data_path: str) -> None:
    """Open the process-wide DuckLake anchor.

    Idempotent: subsequent calls with the anchor already open are a no-op.

    Fails loud if the Postgres catalog is unreachable, ``data_path`` is not
    writable, or the ATTACH fails.

    Args:
        catalog_url: Postgres connection URL for the DuckLake catalog
            (``postgresql://...``).
        data_path: Filesystem path where DuckLake writes parquet files. The
            directory must exist and be writable.

    Raises:
        RuntimeError: If bootstrap fails. The original DuckDB exception is
            chained for inspection.

    Environment overrides:
        DUCKDB_EXTENSION_DIRECTORY: Path where DuckDB looks up cached
            extensions. The container image pre-installs extensions at
            ``/opt/dataraum/duckdb-extensions`` (Dockerfile); this env var
            keeps the runtime ``LOAD`` aligned with that path so it does
            not fall back to ``$HOME/.duckdb/`` (which the system user has
            no access to).
        DUCKLAKE_PG_POOL_MAX: Postgres extension pool ceiling (default 64).
            DuckDB's default is 8, which exhausts under multi-session churn.
        DUCKLAKE_SKIP_INSTALL: Set to ``1`` to skip the network ``INSTALL
            ducklake`` step. Container builds should pre-install the extension
            at image build time and set this to avoid the cold-start round
            trip (and to allow air-gapped deployments).
    """
    global _anchor

    with _bootstrap_lock:
        if _anchor is not None:
            return

        libpq = _pg_url_to_libpq(catalog_url)
        safe_data_path = _escape_sql_literal(data_path)
        attach_sql = (
            f"ATTACH 'ducklake:postgres:{libpq}' AS {LAKE_CATALOG_ALIAS} "
            f"(DATA_PATH '{safe_data_path}')"
        )
        settings = get_settings()
        pool_max = settings.ducklake_pg_pool_max
        ext_dir = settings.duckdb_extension_directory

        try:
            conn = duckdb.connect(LAKE_DB_NAME)
            if ext_dir:
                # Must precede INSTALL/LOAD so DuckDB looks up the extension
                # at the image-baked path rather than ``$HOME/.duckdb/``.
                conn.execute(f"SET extension_directory = '{_escape_sql_literal(str(ext_dir))}'")
            if not settings.ducklake_skip_install:
                conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
            # Raise the Postgres pool ceiling and unpin connections from
            # threads — DuckLake routes every catalog op through the postgres
            # extension's pool, and the defaults (max=8, thread-local pinning
            # ON) exhaust under multi-session churn. Must be ``SET GLOBAL`` and
            # ``BEFORE`` the ATTACH for the lake's pool to inherit the values.
            conn.execute(f"SET GLOBAL pg_pool_max_connections = {pool_max}")
            conn.execute("SET GLOBAL pg_pool_enable_thread_local_cache = false")
            conn.execute(attach_sql)
            # Smoke probe: ATTACH took effect (catalog reachable). DuckLake does
            # not expose ``information_schema``; ``duckdb_schemas()`` filtered by
            # database name exercises the catalog driver against Postgres.
            conn.execute(
                "SELECT 1 FROM duckdb_schemas() "
                f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' LIMIT 1"
            )
            # Materialize the workspace-stable layer schemas. Idempotent — every
            # bootstrap re-asserts the same set, which lets the schemas survive
            # process restarts and lets new layers added in future versions
            # appear on next boot without a separate migration step.
            for layer_schema in LAKE_LAYER_SCHEMAS:
                conn.execute(f'CREATE SCHEMA IF NOT EXISTS {LAKE_CATALOG_ALIAS}."{layer_schema}"')
        except Exception as e:
            raise RuntimeError(
                f"DuckLake bootstrap failed (catalog_url={catalog_url}, data_path={data_path}): {e}"
            ) from e

        _anchor = conn
        logger.info(
            "ducklake_bootstrapped",
            catalog_url=catalog_url,
            data_path=data_path,
            pg_pool_max=pool_max,
        )


def get_anchor() -> duckdb.DuckDBPyConnection:
    """Return the process-wide DuckLake anchor connection.

    Raises:
        RuntimeError: If :func:`bootstrap_lake` has not been called.
    """
    # Capture into a local — without it, a teardown_lake() racing this call
    # could null out the global between the None-check and the return.
    anchor = _anchor
    if anchor is None:
        raise RuntimeError(
            "DuckLake not bootstrapped. Call bootstrap_lake(...) at server "
            "startup (or via the test fixture) before opening per-session "
            "connections."
        )
    return anchor


def connect_session() -> duckdb.DuckDBPyConnection:
    """Open a fresh DuckDB connection to the named lake database.

    The returned connection shares catalog state with the anchor (so the
    DuckLake ATTACH is already visible) but has its own connection-state
    (``USE``, search_path, transaction). Callers own the connection's
    lifecycle and must ``.close()`` it.

    Raises:
        RuntimeError: If :func:`bootstrap_lake` has not been called.
    """
    if _anchor is None:
        raise RuntimeError(
            "DuckLake not bootstrapped. Call bootstrap_lake(...) at server "
            "startup before opening per-session connections."
        )
    return duckdb.connect(LAKE_DB_NAME)


def teardown_lake() -> None:
    """Close the anchor connection. Safe to call when not bootstrapped.

    The named in-memory database persists until the last connection to it
    closes; tests should ensure per-session managers are closed before this.
    """
    global _anchor
    with _bootstrap_lock:
        if _anchor is None:
            return
        try:
            _anchor.close()
        except Exception as e:
            logger.warning("ducklake_anchor_close_failed", error=str(e))
        _anchor = None


def health_probe() -> dict[str, str]:
    """Return a ``/health``-shaped dict for the DuckLake catalog.

    Status is ``ok`` when the anchor exists and the catalog is queryable,
    ``not_bootstrapped`` when the bootstrap hook hasn't run, or
    ``unreachable`` when the catalog query fails.
    """
    # Local capture: a teardown_lake() racing this can null the global
    # between the None-check and ``anchor.execute(...)``.
    anchor = _anchor
    if anchor is None:
        return {"status": "not_bootstrapped"}
    try:
        anchor.execute(
            f"SELECT 1 FROM duckdb_schemas() WHERE database_name = '{LAKE_CATALOG_ALIAS}' LIMIT 1"
        )
    except Exception as e:
        logger.warning("ducklake_health_probe_failed", error=str(e))
        return {"status": "unreachable"}
    return {"status": "ok"}


__all__ = [
    "LAKE_DB_NAME",
    "LAKE_CATALOG_ALIAS",
    "LAKE_LAYER_SCHEMAS",
    "bootstrap_lake",
    "get_anchor",
    "connect_session",
    "teardown_lake",
    "health_probe",
]
