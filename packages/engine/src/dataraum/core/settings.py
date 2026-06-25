"""Typed application settings.

Single source of truth for environment-provided configuration. Replaces
scattered ``os.environ.get(...)`` reads with one validated, typed object so a
missing or malformed var fails loud at boot rather than silently at first use.
Source-swappable: today values come from the process env (the shared ``.env``
feeding both the engine and cockpit containers); a future secrets backend swaps
in here without touching call sites.

Two related concerns are deliberately NOT modeled here:

* **Config-file location** is owned by :mod:`dataraum.core.config` (the
  ``DATARAUM_CONFIG_PATH`` resolver with its own priority chain). Folding it in
  would couple the file resolver to the full env contract.
* **Per-source DB credentials** use dynamic names (``DATARAUM_<NAME>_URL``)
  unknown at boot; they are resolved at runtime by
  :mod:`dataraum.core.credentials` and consumed by DB-source extraction.

Usage:
    from dataraum.core.settings import get_settings

    settings = get_settings()        # validates once; raises on missing vars
    engine = create_engine(settings.database_url)
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Validated environment configuration for the engine process.

    Field names map to upper-cased env vars (``database_url`` -> ``DATABASE_URL``).
    Required fields have no default and raise a ``pydantic.ValidationError``
    naming the field when unset — that is the boot-time fail-loud contract.
    """

    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")

    # --- Substrate (required; the container provides all of these) ---
    # DB URLs stay ``str``: the exact schemes are load-bearing (SQLAlchemy needs
    # ``postgresql+psycopg://``; DuckDB's postgres extension wants the bare
    # ``postgresql://`` libpq form) and a parsed DSN type would re-serialize them.
    database_url: str
    ducklake_catalog_url: str
    # ``str``, NOT ``Path``: this is a DuckLake DATA_PATH, an ``s3://bucket/prefix``
    # URI in production (DAT-388). ``Path`` collapses ``s3://`` to ``s3:/`` and
    # drops the trailing slash, corrupting the URI; ``str`` carries it verbatim.
    ducklake_data_path: str
    # Stays ``str`` (not ``UUID``): the engine resolves the schema name from it
    # and dev/test use stable non-UUID ids (e.g. "test").
    dataraum_workspace_id: str

    # --- LLM (required) ---
    anthropic_api_key: SecretStr

    # --- Object store (DAT-388; required, like the DB + LLM creds). The engine
    # lake lives on an S3-compatible object store — DATA_PATH is an ``s3://`` URI;
    # there is no local-filesystem lake in production. These are plain env-var
    # fields validated through this same seam (the DB password already lives in
    # ``database_url``; the S3 key is no more sensitive). The test harness
    # bootstraps DuckLake against a local tmp DATA_PATH and stubs
    # ``apply_s3_secret`` (``_stub_s3_secret`` in conftest) rather than standing
    # up an object store, so conftest supplies placeholders for these. ---
    s3_endpoint: str  # host:port, no scheme (DuckDB ENDPOINT form)
    s3_access_key_id: str
    s3_secret_access_key: SecretStr
    s3_region: str = "us-east-1"
    s3_use_ssl: bool = True
    # The single lake bucket. Source URIs are gated against it
    # (``dataraum.core.uri.validate_source_uri``) so the worker can only read
    # ``s3://<s3_bucket>/...`` — never a foreign bucket or a local path (DAT-389
    # hardening). Discrete (not parsed from ``ducklake_data_path``) so the
    # allow-list stays a plain string the validator reads directly; compose sets
    # both from the same ``S3_BUCKET`` (``DUCKLAKE_DATA_PATH=s3://${S3_BUCKET}/lake``).
    s3_bucket: str

    # --- DuckLake tuning (defaulted; see server/storage.py) ---
    # Postgres connection-pool ceiling for the (singleton, process-wide) DuckLake
    # catalog ATTACH. Small + static on purpose: with thread-local caching
    # DISABLED (storage.py) connections recycle to the pool promptly, so the pool
    # only needs to cover peak concurrent catalog ops (~_MAX_CONCURRENT_METRICS),
    # NOT total churn. 16 covers the operating_model concurrency with headroom and
    # stays well under Postgres max_connections — which the engine ORM pool,
    # temporal, and cockpit share. (Was 64: an over-correction stacked on top of
    # the thread-local-cache disable that actually fixed DuckLake's exhaustion,
    # and 64 alone tipped a 100-connection Postgres over once metrics ran.)
    ducklake_pg_pool_max: int = 16
    ducklake_skip_install: bool = False
    duckdb_extension_directory: Path | None = None
    # Max retry attempts for a DuckLake transaction commit (DAT-641 groundwork).
    # DuckLake serializes snapshots via a PK on snapshot_id, so concurrent writers
    # (the per-table typing fan-out) race for the next id and retry. The default
    # is 10 (ducklake docs) — too low for a wide replay fan-out, where 7–21 tables
    # commit at once. Non-logical conflicts (distinct tables) auto-resolve on
    # retry, so a higher budget makes the fan-out reliable. Set per lake connection
    # in core/connections. (The Temporal-retry fallback stays in DAT-641 proper.)
    ducklake_max_retry_count: int = 100

    # --- Promoted-read surface (ADR-0008 / DAT-453; defaulted for dev) ---
    # Password for the cluster-global ``cockpit_reader`` role the bootstrap
    # provisions with SELECT on ``ws_<id>_read`` ONLY — the cockpit's metadata
    # connection uses it, so raw run-stamped tables are unreachable from there.
    # Compose overrides this; managed-Postgres deployments pre-provision the
    # role and the bootstrap's CREATE ROLE branch is skipped (role exists).
    metadata_reader_password: SecretStr = SecretStr("cockpit-reader-dev")

    # --- Temporal (required: the engine process IS the Temporal activity
    # worker; it cannot start without a broker to poll. DAT-369 flipped these
    # from slice-1 optional now that the worker is the only Settings consumer
    # and compose always provides them.) ---
    temporal_host: str
    temporal_namespace: str
    temporal_task_queue: str


@cache
def get_settings() -> Settings:
    """Return the process-wide settings singleton, validating env on first call.

    Call this once at boot (the Temporal worker entrypoint) so a
    misconfigured deployment fails loud before serving.
    """
    return Settings()  # type: ignore[call-arg]


def reset_settings() -> None:
    """Clear the cached settings so the next ``get_settings()`` re-reads env.

    Tests only — production constructs settings once at boot.
    """
    get_settings.cache_clear()
