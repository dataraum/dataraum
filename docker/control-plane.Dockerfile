FROM python:3.14-slim

LABEL org.opencontainers.image.source="https://github.com/dataraum/dataraum"
LABEL org.opencontainers.image.description="DataRaum control plane (platform substrate)"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# System deps: gcc/g++ for any source builds; curl for the container healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl && \
    rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Resolve deps first so the slow install layer caches across source-only changes.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --no-dev --frozen --no-install-project

# Now copy the project itself and re-sync to install the package
COPY src/ src/
COPY config/ /opt/dataraum/config/
RUN uv sync --no-dev --frozen

# Engine config root — load verticals/ontologies/prompts/llm configs from the
# baked-in /opt/dataraum/config/ rather than auto-detecting via package layout.
ENV DATARAUM_CONFIG_PATH=/opt/dataraum/config

# Non-root runtime user. DuckDB extensions live at a known image path
# (``/opt/dataraum/duckdb-extensions``) rather than ``$HOME/.duckdb/`` so we
# don't need to provision a writable home — the path is immutable, predictable,
# and pre-populated at build time. Pattern adapted from web-app/roboduck.
RUN groupadd -r dataraum && useradd -r -g dataraum -u 1001 dataraum && \
    mkdir -p /var/lib/dataraum/lake /var/lib/dataraum/sources /opt/dataraum/duckdb-extensions && \
    chown -R dataraum:dataraum /app /opt/dataraum /var/lib/dataraum

USER dataraum

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

EXPOSE 8000

ENTRYPOINT ["/app/.venv/bin/uvicorn", "dataraum.server.app:app", "--host", "0.0.0.0", "--port", "8000"]
