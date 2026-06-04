FROM python:3.14-slim

LABEL org.opencontainers.image.source="https://github.com/dataraum/dataraum"
LABEL org.opencontainers.image.description="DataRaum engine — Temporal activity worker"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# System deps: gcc/g++ for any source builds. No curl — the engine is a Temporal
# worker now (DAT-344), not an HTTP service; its health is the worker heartbeat
# the Temporal server records, checked externally via `temporal worker list`.
# libgssapi-krb5-2: runtime dependency of the community mssql DuckDB extension
# (db-recipe backend) — without it LOAD mssql fails with a missing
# libgssapi_krb5.so.2, both at the pre-bake below and at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libgssapi-krb5-2 && \
    rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Resolve deps first so the slow install layer caches across source-only changes.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --no-dev --frozen --no-install-project

# Now copy the project itself and re-sync to install the package
COPY src/ src/
RUN uv sync --no-dev --frozen

# Engine config root. Config is the standalone `dataraum-config` package, NOT
# baked into this image — it is bind-mounted at /opt/dataraum/config by compose
# (and would be a volume / object-store mount in other deploys). This env var
# tells dataraum.core.config where to find it; the directory is provided at
# runtime, so a bare `docker run` without the mount has no config by design.
ENV DATARAUM_CONFIG_PATH=/opt/dataraum/config

# Non-root runtime user. DuckDB extensions live at a known image path
# (``/opt/dataraum/duckdb-extensions``) rather than ``$HOME/.duckdb/`` so we
# don't need to provision a writable home — the path is immutable, predictable,
# and pre-populated at build time. Pattern adapted from web-app/roboduck.
RUN groupadd -r dataraum && useradd -r -g dataraum -u 1001 dataraum && \
    mkdir -p /var/lib/dataraum/sources /opt/dataraum/duckdb-extensions && \
    chown -R dataraum:dataraum /app /opt/dataraum /var/lib/dataraum

USER dataraum

# Pre-install the DuckDB extensions at image build time. Runtime sets
# DUCKLAKE_SKIP_INSTALL=1 (read by server/storage.bootstrap_lake,
# apply_s3_secret AND sources/backends.extract_backend) so neither the cold
# start nor a db-recipe extraction hits the network — also makes air-gapped
# deploys work. httpfs is required to read/write the lake's parquet over s3://
# (DAT-388); postgres/mysql/sqlite + community mssql are the db-recipe backends
# (`postgres` is the extension DuckLake's ``ATTACH 'ducklake:postgres:...'``
# auto-loads to reach the Postgres catalog). ``SET extension_directory`` makes
# the cache land at the known image path rather than the (missing) home
# directory of the system user. Behind a corporate proxy DuckDB ignores
# HTTP_PROXY, so a runtime INSTALL would stall — ``SET http_proxy`` from the
# build-arg/env proxy lets the pre-bake reach extensions.duckdb.org (no-op when
# unset / building with direct egress).
RUN /app/.venv/bin/python -c "import os, duckdb; \
    c = duckdb.connect(); \
    c.execute(\"SET extension_directory = '/opt/dataraum/duckdb-extensions'\"); \
    _p = (os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy') or '').replace('http://', '').replace('https://', '').rstrip('/'); \
    c.execute(\"SET http_proxy = '\" + _p + \"'\") if _p else None; \
    [ (c.execute('INSTALL ' + e), c.execute('LOAD ' + e)) \
      for e in ['ducklake', 'httpfs', 'postgres', 'mysql', 'sqlite'] ]; \
    c.execute('INSTALL mssql FROM community'); \
    c.execute('LOAD mssql'); \
    c.close()"

ENV DUCKDB_EXTENSION_DIRECTORY=/opt/dataraum/duckdb-extensions
ENV DUCKLAKE_SKIP_INSTALL=1

# The engine runs as a Temporal activity worker (DAT-344): it polls the task
# queue, it does not listen on a port. Bootstraps the substrate, then runs the
# bundled workflow + phase activities until SIGTERM.
ENTRYPOINT ["/app/.venv/bin/python", "-m", "dataraum.worker.main"]
