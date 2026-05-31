"""DuckLake bootstrap and shared in-memory anchor for the FastAPI process.

DuckLake stores data as parquet files on an object store (DATA_PATH is an
``s3://`` URI — DAT-388) with metadata in a Postgres catalog database. DuckDB
clients access DuckLake by ATTACHing the catalog as an external database; the
connection must first register the S3 secret + ``httpfs`` (see
:func:`apply_s3_secret`) so DuckLake can resolve the ``s3://`` DATA_PATH.

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
            # libpq connection-string grammar (NOT a DuckDB SQL literal): inside
            # a single-quoted libpq value, backslash IS the escape character, so
            # both ``\`` and ``'`` are backslash-escaped here. Do NOT
            # "consistency-fix" this to ``''`` doubling — that is correct for the
            # DuckDB literals in :func:`_escape_sql_literal`, a different layer.
            escaped = decoded.replace("\\", "\\\\").replace("'", "\\'")
            parts.append(f"password='{escaped}'")
        else:
            parts.append(f"password={decoded}")
    return " ".join(parts)


def _escape_sql_literal(value: str) -> str:
    r"""Escape a value for safe interpolation into a DuckDB single-quoted literal.

    DuckDB single-quoted string literals do NOT honor backslash escapes — a
    backslash is just a literal character inside the literal. The only escape
    that matters is doubling an embedded single quote (``'`` → ``''``). This
    matches the engine's own source loaders (``sources/*/loader.py``) and the
    cockpit's ``applyS3Secret``. (Distinct from :func:`_pg_url_to_libpq`, which
    escapes for libpq connection-string grammar where backslash IS the escape.)
    """
    return value.replace("'", "''")


S3_SECRET_NAME = "dataraum_s3"
"""Name of the DuckDB S3 secret registered on lake connections (DAT-388).

Mirrors the cockpit's ``applyS3Secret`` so both sides present the same
credentials to the same object-store endpoint. ``CREATE OR REPLACE`` makes
registration idempotent across the anchor + every per-session connection.
"""


def _build_s3_secret_sql(
    *,
    access_key_id: str,
    secret_access_key: str,
    endpoint: str,
    region: str,
    use_ssl: bool,
    bucket: str,
) -> str:
    """Build the idempotent ``CREATE OR REPLACE SECRET`` for the object store.

    All interpolated values are single-quoted SQL literals, so each is escaped.
    ``URL_STYLE 'path'`` is required for non-AWS S3 (SeaweedFS/MinIO) — DuckDB
    defaults to virtual-host style and does not auto-flip on a custom endpoint;
    path-style is also accepted by AWS, so it is safe as a constant here.

    ``SCOPE 's3://<bucket>'`` confines the credentials to the lake bucket
    (DAT-389 hardening): DuckDB only attaches this secret to ``s3://<bucket>/*``
    paths, so even a request for another bucket finds no matching secret.
    """
    return (
        f"CREATE OR REPLACE SECRET {S3_SECRET_NAME} ("
        "TYPE s3, "
        f"KEY_ID '{_escape_sql_literal(access_key_id)}', "
        f"SECRET '{_escape_sql_literal(secret_access_key)}', "
        f"ENDPOINT '{_escape_sql_literal(endpoint)}', "
        f"REGION '{_escape_sql_literal(region)}', "
        "URL_STYLE 'path', "
        f"USE_SSL {'true' if use_ssl else 'false'}, "
        f"SCOPE 's3://{_escape_sql_literal(bucket)}'"
        ")"
    )


def apply_s3_secret(conn: duckdb.DuckDBPyConnection, *, disable_local_fs: bool = False) -> None:
    """Register the object-store S3 secret + load ``httpfs`` on ``conn``.

    Must run before ATTACHing a DuckLake catalog whose ``DATA_PATH`` is an
    ``s3://`` URI (DuckLake resolves the path eagerly), on every connection that
    reads or writes lake parquet — and on the throwaway connections that sniff an
    ``s3://`` source schema (DAT-389). Idempotent (``CREATE OR REPLACE SECRET``).

    Honors ``DUCKLAKE_SKIP_INSTALL`` for the ``httpfs`` install (the worker image
    pre-bakes it), same as the ducklake extension. When the install is skipped,
    ``LOAD httpfs`` must find the pre-baked extension, so point
    ``extension_directory`` at the image-baked path first — a fresh in-memory
    connection (e.g. a schema-sniff throwaway) otherwise defaults to
    ``$HOME/.duckdb/`` and the ``LOAD`` fails. Mirrors :func:`bootstrap_lake`.

    Args:
        conn: the DuckDB connection to register the secret on.
        disable_local_fs: defense in depth for the schema-sniff / preview
            throwaway connections (DAT-389 hardening). When ``True``, disables
            DuckDB's local filesystem AFTER ``httpfs`` is loaded (extensions
            load from the local FS, so the order matters) — so a source URI that
            somehow slipped past ``validate_source_uri`` still cannot read a
            local file on the worker. NOT set on the anchor / session lake
            connections, which only ever touch the ``s3://`` lake but must keep
            local access for the extension/ATTACH machinery.
    """
    settings = get_settings()
    ext_dir = settings.duckdb_extension_directory
    if ext_dir:
        conn.execute(f"SET extension_directory = '{_escape_sql_literal(str(ext_dir))}'")
    if not settings.ducklake_skip_install:
        conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(
        _build_s3_secret_sql(
            access_key_id=settings.s3_access_key_id,
            secret_access_key=settings.s3_secret_access_key.get_secret_value(),
            endpoint=settings.s3_endpoint,
            region=settings.s3_region,
            use_ssl=settings.s3_use_ssl,
            bucket=settings.s3_bucket,
        )
    )
    if disable_local_fs:
        # AFTER LOAD httpfs (extensions load from the local FS). The lake/sniff
        # reads are all s3:// over httpfs, so refusing the local filesystem here
        # blocks an arbitrary-file read without touching legitimate reads.
        conn.execute("SET disabled_filesystems='LocalFileSystem'")


def bootstrap_lake(catalog_url: str, data_path: str) -> None:
    """Open the process-wide DuckLake anchor.

    Idempotent: subsequent calls with the anchor already open are a no-op.

    Fails loud if the Postgres catalog is unreachable, the S3 bucket is
    inaccessible, or the ATTACH fails.

    Args:
        catalog_url: Postgres connection URL for the DuckLake catalog
            (``postgresql://...``).
        data_path: ``s3://`` URI where DuckLake writes parquet files. The bucket
            must exist; the object-store secret is registered via
            :func:`apply_s3_secret` here, before the ATTACH.

    Raises:
        RuntimeError: If bootstrap fails. The original DuckDB exception is
            chained for inspection.

    Configuration (resolved via typed settings — ``core/settings.py`` — from
    these env vars, no longer read here directly; DAT-363):
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

        conn = duckdb.connect(LAKE_DB_NAME)
        try:
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
            # The lake DATA_PATH is an ``s3://`` URI: register the object-store
            # secret + httpfs before the ATTACH (DuckLake resolves DATA_PATH
            # eagerly).
            apply_s3_secret(conn)
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
            # Don't leak the connection on a failed bootstrap: an open connection
            # keeps the named in-memory DB (and any partial ATTACH state) alive,
            # so a later re-bootstrap fails with "database 'lake' already exists".
            # Relying on GC to close it is fragile (a retained reference — e.g. a
            # test double recording call args — defers the close indefinitely).
            conn.close()
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
    "S3_SECRET_NAME",
    "apply_s3_secret",
    "bootstrap_lake",
    "get_anchor",
    "connect_session",
    "teardown_lake",
    "health_probe",
]
