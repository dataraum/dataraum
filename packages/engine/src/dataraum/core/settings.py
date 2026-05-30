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
    # bootstraps DuckLake against a local tmp DATA_PATH instead of standing up an
    # object store (the ``s3://`` guard in ``server/storage.bootstrap_lake``), so
    # conftest supplies placeholder values to satisfy this contract. ---
    s3_endpoint: str  # host:port, no scheme (DuckDB ENDPOINT form)
    s3_access_key_id: str
    s3_secret_access_key: SecretStr
    s3_region: str = "us-east-1"
    s3_use_ssl: bool = True

    # --- DuckLake tuning (defaulted; see server/storage.py) ---
    ducklake_pg_pool_max: int = 64
    ducklake_skip_install: bool = False
    duckdb_extension_directory: Path | None = None

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

    Call this once at boot (the Starlette lifespan / future Temporal worker
    entrypoint) so a misconfigured deployment fails loud before serving.
    """
    return Settings()  # type: ignore[call-arg]


def reset_settings() -> None:
    """Clear the cached settings so the next ``get_settings()`` re-reads env.

    Tests only — production constructs settings once at boot.
    """
    get_settings.cache_clear()
