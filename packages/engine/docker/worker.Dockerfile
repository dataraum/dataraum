FROM python:3.14-slim

LABEL org.opencontainers.image.source="https://github.com/dataraum/dataraum"
LABEL org.opencontainers.image.description="DataRaum engine — Temporal activity worker"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# System deps: gcc/g++ for any source builds. No curl — the engine is a Temporal
# worker now (DAT-344), not an HTTP service; its health is the worker heartbeat
# the Temporal server records, checked externally via `temporal worker list`.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
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
    mkdir -p /var/lib/dataraum/lake /var/lib/dataraum/sources /var/lib/dataraum/workspace /opt/dataraum/duckdb-extensions && \
    chown -R dataraum:dataraum /app /opt/dataraum /var/lib/dataraum

USER dataraum

# Workspace state (sessions, archives, logs) lives here — outside $HOME so
# the non-root user doesn't need a writable home directory. Consumers of the
# image (compose / k8s / `docker run`) inherit this default; mount a volume
# at /var/lib/dataraum/workspace to persist across container recreations.
ENV DATARAUM_HOME=/var/lib/dataraum/workspace

# Pre-install the DuckLake extension at image build time. Runtime sets
# DUCKLAKE_SKIP_INSTALL=1 (read by server/storage.bootstrap_lake) so the cold
# start does not hit the network — also makes air-gapped deploys work.
# ``SET extension_directory`` makes the cache land at the known image path
# rather than the (missing) home directory of the system user.
RUN /app/.venv/bin/python -c "import duckdb; \
    c = duckdb.connect(); \
    c.execute(\"SET extension_directory = '/opt/dataraum/duckdb-extensions'\"); \
    c.execute('INSTALL ducklake'); \
    c.execute('LOAD ducklake'); \
    c.close()"

ENV DUCKDB_EXTENSION_DIRECTORY=/opt/dataraum/duckdb-extensions
ENV DUCKLAKE_SKIP_INSTALL=1

# The engine runs as a Temporal activity worker (DAT-344): it polls the task
# queue, it does not listen on a port. Bootstraps the substrate, then runs the
# bundled workflow + phase activities until SIGTERM.
ENTRYPOINT ["/app/.venv/bin/python", "-m", "dataraum.worker.main"]
