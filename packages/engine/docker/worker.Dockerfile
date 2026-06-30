# DataRaum engine — Temporal activity worker (DAT-344). Multi-stage:
#   builder — resolve + install the venv; carries the build toolchain (gcc/g++)
#             and uv, both discarded with this stage.
#   runtime — slim image with only the venv + the mssql runtime lib. No gcc/g++,
#             no uv. The venv stays root-owned/world-readable (NO recursive chown
#             of /app — that would copy-up the whole ~700MB venv into a duplicate
#             layer; only the writable runtime dirs are chowned to dataraum).

# ---------- builder ----------
FROM python:3.14-slim AS builder

# gcc/g++ for any source-built wheels during `uv sync` (most deps ship
# arm64/x86_64 manylinux wheels, but keep the toolchain so a source build never
# fails the image — it is discarded with this stage, costing the final image 0).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution (build-only — not in the runtime).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Pin uv to the system CPython 3.14 (matches the runtime base below) so the venv
# references /usr/local/bin/python3.14 — present identically in the runtime
# stage — rather than a managed interpreter that the COPY below wouldn't carry.
ENV UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Resolve deps first so the slow install layer caches across source-only changes.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --no-dev --frozen --no-install-project

# Now copy the project itself and re-sync to install the package (editable — the
# venv references /app/src, so the runtime stage keeps /app at this same path).
COPY src/ src/
RUN uv sync --no-dev --frozen

# ---------- runtime ----------
FROM python:3.14-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/dataraum/dataraum"
LABEL org.opencontainers.image.description="DataRaum engine — Temporal activity worker"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# libgssapi-krb5-2: runtime dependency of the community mssql DuckDB extension
# (db-recipe backend) — without it LOAD mssql fails with a missing
# libgssapi_krb5.so.2, at the pre-bake below and at runtime. No curl: the engine
# is a Temporal worker (DAT-344), not an HTTP service; its health is the worker
# heartbeat, checked externally via `temporal worker list`. gcc/g++ and uv are
# build-only and deliberately absent here (they live in the builder stage).
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgssapi-krb5-2 && \
    rm -rf /var/lib/apt/lists/*

# Engine config root. Config is the standalone `dataraum-config` package, NOT
# baked into this image — it is bind-mounted at /opt/dataraum/config by compose
# (and would be a volume / object-store mount in other deploys). This env var
# tells dataraum.core.config where to find it; the directory is provided at
# runtime, so a bare `docker run` without the mount has no config by design.
ENV DATARAUM_CONFIG_PATH=/opt/dataraum/config

# Non-root runtime user. Only the WRITABLE dirs are chowned to dataraum; the venv
# + source under /app stay root-owned and world-readable (a non-root user
# reads/executes them fine). Chowning /app would copy-up the entire ~700MB venv
# into a duplicate layer. DuckDB extensions live at a known image path
# (/opt/dataraum/duckdb-extensions) — immutable, predictable, pre-populated below
# — so we don't need a writable home for the system user.
RUN groupadd -r dataraum && useradd -r -g dataraum -u 1001 dataraum && \
    mkdir -p /var/lib/dataraum/sources /opt/dataraum/duckdb-extensions && \
    chown -R dataraum:dataraum /opt/dataraum /var/lib/dataraum

# The installed venv + project from the builder. Root-owned, world-readable — no
# recursive chown, no duplicate layer. The editable install references /app/src,
# preserved at this same path.
COPY --from=builder /app /app

WORKDIR /app
USER dataraum

# Pre-install the DuckDB extensions at image build time. Runtime sets
# DUCKLAKE_SKIP_INSTALL=1 (read by server/storage.bootstrap_lake,
# apply_s3_secret AND sources/backends.extract_backend) so neither the cold
# start nor a db-recipe extraction hits the network — also makes air-gapped
# deploys work. httpfs is required to read/write the lake's parquet over s3://
# (DAT-388); postgres/mysql/sqlite + community mssql are the db-recipe
# backends. ``SET extension_directory`` makes the cache land at the known
# image path rather than the (missing) home directory of the system user.
RUN /app/.venv/bin/python -c "import duckdb; \
    c = duckdb.connect(); \
    c.execute(\"SET extension_directory = '/opt/dataraum/duckdb-extensions'\"); \
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
