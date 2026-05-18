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
COPY config/ config/
RUN uv sync --no-dev --frozen

# Non-root runtime user; own the data dirs so named volumes initialize correctly.
RUN groupadd -r dataraum && useradd -r -g dataraum -u 1001 dataraum && \
    mkdir -p /var/lib/dataraum/lake /var/lib/dataraum/sources && \
    chown -R dataraum:dataraum /app /var/lib/dataraum

USER dataraum

EXPOSE 8000

ENTRYPOINT ["/app/.venv/bin/uvicorn", "dataraum.server.app:app", "--host", "0.0.0.0", "--port", "8000"]
